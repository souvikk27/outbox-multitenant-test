# Integrations — Plugging the Outbox into Enterprise Infrastructure

A guide to placing this outbox library inside a real enterprise architecture — alongside Kafka,
RabbitMQ, Azure Service Bus, Hangfire, and Quartz.

The library is intentionally narrow: it transactionally durabilizes the *promise* to do work and
runs that work later. It does not deliver to brokers, schedule cron, or do retries with
exponential-billed dashboards. Those are jobs for the tools below — and this doc is about how to
make them work together without duplicating durability or hiding failure modes.

If you have not read `PROJECT_BIBLE.md`, read it first. The terminology (`IOutboxHandler<TPayload>`,
`IOutboxPublisher`, `OutboxMessage`, `OutboxMutation`) is from there.

---

## 1. Where the outbox fits

```
   Producer service                                            Far-side consumer
   ─────────────────                                           ─────────────────
                                                                       ▲
   ┌────────────────────────┐                                          │ at-least-once
   │ Business code          │                                          │
   │  AppDbContext          │  business tx       ┌──────────────────┐  │
   │   Orders ◄─── commit ──┤                    │ Kafka / RabbitMQ │──┘
   │   OutboxMessages ◄─────┤                    │ ASB / Hangfire   │
   └────────┬───────────────┘                    │ Quartz / SMTP    │
            │ enqueue (no extra tx)              └────────┬─────────┘
            ▼                                             ▲
   ┌────────────────────────┐                             │
   │ IOutboxPublisher       │                             │
   │  Enqueue<TPayload>     │                             │
   └────────────────────────┘                             │
                                                          │ publish / enqueue / send
                                                          │
   ┌────────────────────────┐    SKIP LOCKED   ┌──────────┴─────────┐
   │ outbox_messages (PG)   │ ─── claim ─────► │ Outbox worker      │
   │                        │ ◄── complete ─── │  Dispatcher        │
   └────────────────────────┘                  │   IOutboxHandler   │ ◄── you write this
                                               └────────────────────┘
```

The dotted line between `IOutboxHandler` and the broker/scheduler on the right is the integration
boundary this document is about. Everything to the left of it is the library; everything to the
right is some piece of enterprise infra you already operate.

### 1.1 The at-least-once chain

Every link in the chain offers **at-least-once delivery**. End-to-end exactly-once is impossible
in distributed systems; the only honest answer is at-least-once + idempotent consumers.

```
producer commit  →  outbox row durable  →  worker dispatch  →  bus publish  →  consumer
   atomic              atomic                ≥1×                ≥1×             ≥1× (must dedup)
```

The outbox guarantees the first arrow is atomic with business state. Every later arrow is "at
least once". Plan dedup at the far end (consumer-side), keyed off the `MessageId` Guid, which
flows unchanged through every link.

### 1.2 What the outbox is NOT

- **Not a job scheduler** — no cron, no recurring jobs, no calendar logic. Use Quartz/Hangfire.
- **Not a message bus** — no fan-out subscriptions, no consumer groups, no partitioning. Use Kafka/RabbitMQ/ASB.
- **Not a workflow engine** — no orchestration of multi-step sagas with compensations. Use a saga library.
- **Not a CDC tool** — it watches no log; the producer code calls `Enqueue` explicitly. If you want
  database-level change capture, use Debezium.

The outbox is for one specific problem: making "we committed business state AND we will do a
side-effect" atomic, without sacrificing the side-effect's durability.

---

## 2. Decision matrix

| You need... | Use this | Why |
|---|---|---|
| Atomic "save row + emit event" with at-least-once delivery | This library + bus integration | The whole point. |
| Eventual cross-service consistency | This library + Kafka/ASB/RabbitMQ handler | Producer side of choreography. |
| Long-running compute jobs triggered by events | This library + Hangfire/Quartz delegating handler | Outbox makes the trigger durable; Hangfire owns execution. |
| Cron-style recurring jobs ("every Monday 9am") | Hangfire/Quartz directly | Not a transactional concern. |
| Delayed message ("send reminder in 3 days") | This library — set `OutboxPublishOptions.AvailableAt` | Native delayed dispatch. |
| Workflow with branching/compensation | A saga framework (MassTransit, NServiceBus) | Outside the outbox's scope. |
| Pure async API offload ("return 202 to client, do work later") | Hangfire/Quartz | No transactional coupling needed; outbox is overkill. |
| Database change replication to a data warehouse | Debezium / CDC tooling | Application-level outbox can't capture changes from non-app writers. |

The outbox earns its complexity when the producer's *business write* and the *side-effect* must
either both happen or neither. If they don't, simpler tools win.

---

## 3. Message bus integrations

The dominant enterprise pattern is **outbox-as-bus-bridge**: handlers don't do work themselves,
they publish to a real bus, and downstream services consume from the bus. This isolates
"transactional emission" (this library's job) from "fan-out, partitioning, retention" (the bus's
job).

### 3.1 The pattern: a generic publishing handler

One handler type per bus, generic over the payload. The library's `AddHandler<TPayload, THandler>`
registration handles closed generics naturally.

```csharp
public sealed class BusPublishingHandler<TPayload> : IOutboxHandler<TPayload>
{
    private readonly IBusPublisher _bus;
    private readonly ILogger<BusPublishingHandler<TPayload>> _logger;

    public BusPublishingHandler(IBusPublisher bus, ILogger<BusPublishingHandler<TPayload>> logger)
    {
        _bus = bus;
        _logger = logger;
    }

    public async Task HandleAsync(OutboxHandlerContext<TPayload> ctx, CancellationToken ct)
    {
        var envelope = new BusEnvelope<TPayload>(
            MessageId: ctx.MessageId,           // stable across outbox retries → consumer dedup key
            EventType: ctx.EventType,           // routing
            TenantId:  ctx.TenantId,            // partitioning / sessions
            Payload:   ctx.Payload,
            Headers:   BuildHeaders(ctx)
        );
        await _bus.PublishAsync(envelope, ct);
    }

    private static IReadOnlyDictionary<string, string> BuildHeaders(OutboxHandlerContext<TPayload> ctx)
    {
        var headers = new Dictionary<string, string>(ctx.Metadata, StringComparer.OrdinalIgnoreCase)
        {
            ["x-outbox-message-id"]  = ctx.MessageId.ToString(),
            ["x-outbox-tenant-id"]   = ctx.TenantId,
            ["x-outbox-event-type"]  = ctx.EventType,
            ["x-outbox-retry-count"] = ctx.RetryCount.ToString(),
            ["x-outbox-created-at"]  = ctx.CreatedAt.ToString("O"),
        };
        return headers;
    }
}
```

Registration per event type:

```csharp
services.AddSingleton<IBusPublisher, KafkaBusPublisher>();   // or RabbitMQ, ASB
services
    .AddOutbox<AppDbContext>(o => config.GetSection("Outbox").Bind(o))
    .AddProcessing()
    .AddHandler<OrderPlacedPayload,    BusPublishingHandler<OrderPlacedPayload>>   ("order.placed")
    .AddHandler<PaymentCapturedPayload, BusPublishingHandler<PaymentCapturedPayload>>("payment.captured")
    .AddHandler<EmailPayload,           BusPublishingHandler<EmailPayload>>         ("email");
```

### 3.2 The `IBusPublisher` abstraction

Apps define their own — the bus-agnostic shape below is a starting point, but you'll evolve it
based on bus quirks (Kafka keys, ASB sessions, RabbitMQ routing keys).

```csharp
public interface IBusPublisher
{
    Task PublishAsync<TPayload>(BusEnvelope<TPayload> envelope, CancellationToken ct);
}

public sealed record BusEnvelope<TPayload>(
    Guid MessageId,
    string EventType,
    string TenantId,
    TPayload Payload,
    IReadOnlyDictionary<string, string> Headers
);
```

This single shape covers ~80% of enterprise needs. Don't try to define one universal abstraction
that captures every bus feature — bus-specific affordances (Kafka headers vs ASB application
properties, RabbitMQ delivery modes) belong in the implementation.

### 3.3 Trace context propagation

The outbox stores message metadata as JSON in the `metadata` column. Producers should put W3C
trace context into it; the publishing handler should propagate it across the bus boundary.

**Producer side**:
```csharp
public async Task PlaceOrder(Order order, CancellationToken ct)
{
    _db.Orders.Add(order);

    var headers = new Dictionary<string, string>();
    if (Activity.Current is { } current)
    {
        headers["traceparent"] = current.Id ?? "";
        if (!string.IsNullOrEmpty(current.TraceStateString))
            headers["tracestate"] = current.TraceStateString;
    }

    _publisher.Enqueue(_db,
        new OrderPlacedPayload(order.Id),
        new OutboxPublishOptions { TenantId = order.TenantId, Metadata = headers });

    await _db.SaveChangesAsync(ct);
}
```

**Handler side** — restore the producer's context as a parent so the dispatcher's `Outbox.Dispatch`
span is a child of the original request span:

```csharp
public async Task HandleAsync(OutboxHandlerContext<TPayload> ctx, CancellationToken ct)
{
    if (ctx.Metadata.TryGetValue("traceparent", out var tp))
    {
        ActivityContext.TryParse(tp, ctx.Metadata.GetValueOrDefault("tracestate"), out var parent);
        using var activity = MyActivitySource.StartActivity(
            "BusPublish", ActivityKind.Producer, parent);
        await _bus.PublishAsync(...);
    }
    else
    {
        await _bus.PublishAsync(...);
    }
}
```

This stitches: `producer HTTP request → outbox enqueue → outbox dispatch → bus publish → far-side
consumer → ...` into one trace tree. Without it, the outbox is a black hole in your traces.

### 3.4 Kafka (Confluent.Kafka)

#### Architecture

```
Producer ──► outbox_messages ──► Outbox worker ──► Kafka ──► Consumer group(s)
                                          │            │
                                          │            ├─ topic per event_type, OR
                                          │            └─ one topic, event_type as header
                                          │
                                          └─ key = TenantId or aggregate id (partition affinity)
```

**Topic strategy**:
- *Topic per event type* — easier ACLs, clearer schema-per-topic. Use when event types have
  different retention, throughput, or auth needs.
- *One topic with type header* — fewer admin objects, fan-out via filtering on consumer side. Use
  for high-cardinality event-type catalogs.

#### Configuration

```csharp
// appsettings.json
"Kafka": {
    "BootstrapServers": "broker-1:9092,broker-2:9092",
    "Acks": "all",
    "EnableIdempotence": true,
    "MaxInFlight": 5,
    "CompressionType": "lz4",
    "TopicPrefix": "ordering."
}
```

`Acks=all` + `EnableIdempotence=true` is the only safe combo when the outbox is the source of truth.
Without idempotence, a producer retry inside the Kafka client can publish the same message twice
*on top of* the at-least-once the outbox already gives you, and your consumer dedup window has to
be twice as wide.

#### Implementation sketch

```csharp
public sealed class KafkaBusPublisher : IBusPublisher, IDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly IOutboxSerializer _serializer;
    private readonly KafkaOptions _options;

    public KafkaBusPublisher(IOptions<KafkaOptions> options, IOutboxSerializer serializer)
    {
        _options = options.Value;
        _serializer = serializer;
        _producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MaxInFlight = 5,
            CompressionType = CompressionType.Lz4,
        }).Build();
    }

    public async Task PublishAsync<TPayload>(BusEnvelope<TPayload> envelope, CancellationToken ct)
    {
        var topic = _options.TopicPrefix + envelope.EventType;       // "ordering.order.placed"
        var key   = envelope.TenantId;                               // partition key

        var headers = new Headers();
        foreach (var (k, v) in envelope.Headers)
            headers.Add(k, System.Text.Encoding.UTF8.GetBytes(v));

        var message = new Message<string, byte[]>
        {
            Key = key,
            Value = System.Text.Encoding.UTF8.GetBytes(_serializer.Serialize(envelope.Payload!, typeof(TPayload))),
            Headers = headers,
        };

        var result = await _producer.ProduceAsync(topic, message, ct);

        if (result.Status == PersistenceStatus.NotPersisted)
            throw new TimeoutException($"Kafka produce not persisted: {result.TopicPartitionOffset}");
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
```

Register `KafkaBusPublisher` as a **singleton** — `IProducer` is thread-safe and expensive to
construct. The outbox workers will call it concurrently.

#### Failure semantics

`ProduceAsync` throws on broker unavailability or timeout → outbox classifies it as transient
(default classifier handles `TimeoutException` and `KafkaException`'s base `Exception`) →
retried with backoff. The default classifier handles common cases; for fine control:

```csharp
public class KafkaErrorClassifier : IOutboxErrorClassifier
{
    public OutboxErrorClassification Classify(Exception ex, OutboxMessage msg, int attempt) => ex switch
    {
        ProduceException<string, byte[]> pe when pe.Error.Code == ErrorCode.MsgSizeTooLarge
            => OutboxErrorClassification.Permanent,
        ProduceException<string, byte[]> pe when pe.Error.IsFatal
            => OutboxErrorClassification.Permanent,
        ProduceException<string, byte[]>
            => OutboxErrorClassification.Transient,
        _ => OutboxErrorClassification.Transient,
    };
}
```

#### Consumer-side idempotency

```csharp
// In your consumer service:
public async Task ConsumeAsync(ConsumeResult<string, byte[]> result, CancellationToken ct)
{
    var messageId = Guid.Parse(GetHeader(result, "x-outbox-message-id"));

    // Postgres unique constraint on (message_id) is the simplest dedup table.
    var alreadyProcessed = await _db.ProcessedMessages
        .AnyAsync(p => p.MessageId == messageId, ct);
    if (alreadyProcessed) return;

    // ... process the message ...

    _db.ProcessedMessages.Add(new ProcessedMessage(messageId, DateTime.UtcNow));
    await _db.SaveChangesAsync(ct);
}
```

Index on `processed_at` for periodic cleanup; keep the dedup table at least 2× your outbox's
maximum end-to-end retry window.

#### Schema registry

If you use Confluent Schema Registry, replace the JSON `IOutboxSerializer` with one that produces
Avro/Protobuf bytes registered against the schema registry. This affects the outbox's `payload`
column too — it'll hold base64'd registry-compatible bytes instead of JSON. Worth it for strong
contracts; not worth it for low-volume internal events.

### 3.5 RabbitMQ

#### Architecture

```
Producer ──► outbox_messages ──► Outbox worker ──► Exchange ──► Queue(s) ──► Consumer
                                          │
                                          └─ exchange: topic per bounded-context
                                             routing key: event_type (e.g. "order.placed")
```

Use **topic exchanges** as the default; the routing key is `event_type`, queues bind with
patterns (`order.*`, `payment.captured`).

#### Implementation sketch

Using `RabbitMQ.Client` 7.x (async API):

```csharp
public sealed class RabbitMqBusPublisher : IBusPublisher, IAsyncDisposable
{
    private readonly IConnection _connection;
    private readonly IChannel _channel;
    private readonly RabbitMqOptions _options;
    private readonly IOutboxSerializer _serializer;
    private readonly SemaphoreSlim _channelLock = new(1, 1);

    public RabbitMqBusPublisher(IOptions<RabbitMqOptions> options, IOutboxSerializer serializer)
    {
        _options = options.Value;
        _serializer = serializer;
        var factory = new ConnectionFactory { Uri = new Uri(_options.ConnectionString) };
        _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
        _channel = _connection.CreateChannelAsync(new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true)).GetAwaiter().GetResult();
        _channel.ExchangeDeclareAsync(_options.Exchange, ExchangeType.Topic, durable: true)
            .GetAwaiter().GetResult();
    }

    public async Task PublishAsync<TPayload>(BusEnvelope<TPayload> envelope, CancellationToken ct)
    {
        var body = System.Text.Encoding.UTF8.GetBytes(
            _serializer.Serialize(envelope.Payload!, typeof(TPayload)));

        var props = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            MessageId = envelope.MessageId.ToString(),
            Type = envelope.EventType,
            Headers = envelope.Headers.ToDictionary(
                h => h.Key,
                h => (object?)System.Text.Encoding.UTF8.GetBytes(h.Value)),
        };

        // RabbitMQ.Client 7 channels are not thread-safe for publish; serialize.
        await _channelLock.WaitAsync(ct);
        try
        {
            await _channel.BasicPublishAsync(
                exchange: _options.Exchange,
                routingKey: envelope.EventType,
                mandatory: true,
                basicProperties: props,
                body: body,
                cancellationToken: ct);
        }
        finally { _channelLock.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await _channel.CloseAsync();
        await _connection.CloseAsync();
    }
}
```

#### Publisher confirms vs basic publish

Always enable **publisher confirms** for outbox-driven publishing. Without confirms,
`BasicPublishAsync` returns as soon as the message is in the local TCP buffer — a broker crash at
that moment loses the message. The outbox would mark it Processed; consumers never see it.

With `publisherConfirmationsEnabled: true`, `BasicPublishAsync` awaits the broker's ack. If the
broker rejects (queue full, mandatory routing failure), it throws — outbox classifies and retries.

#### Mandatory + alternate exchange

Set `mandatory: true` so unroutable messages fail loudly instead of silently dropping. Configure
an **alternate exchange** on the topic exchange to capture unroutable messages for inspection
rather than letting them disappear.

#### Idempotency

RabbitMQ has no broker-side dedup. The `BasicProperties.MessageId` (set to the outbox `MessageId`)
flows through; consumer-side dedup as in §3.4.

### 3.6 Azure Service Bus

ASB is the friendliest for outbox patterns because it has **native duplicate detection** — set a
detection window (10 min – 7 days) and ASB drops re-sends of the same `MessageId` server-side. The
outbox's at-least-once retries become genuinely "exactly once at the bus boundary" within the
window.

#### Architecture

```
Producer ──► outbox_messages ──► Outbox worker ──► Topic ──► Subscription(s) ──► Consumer
                                          │           │
                                          │           ├─ requires-duplicate-detection: true
                                          │           ├─ duplicate-detection-window: 10m
                                          │           └─ session-enabled: true (for FIFO)
                                          │
                                          └─ MessageId = outbox MessageId Guid
                                             SessionId  = TenantId (for per-tenant FIFO)
```

#### Implementation sketch

Using `Azure.Messaging.ServiceBus`:

```csharp
public sealed class ServiceBusPublisher : IBusPublisher, IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new();
    private readonly ServiceBusOptions _options;
    private readonly IOutboxSerializer _serializer;

    public ServiceBusPublisher(IOptions<ServiceBusOptions> options, IOutboxSerializer serializer)
    {
        _options = options.Value;
        _serializer = serializer;
        _client = new ServiceBusClient(_options.ConnectionString);
    }

    public async Task PublishAsync<TPayload>(BusEnvelope<TPayload> envelope, CancellationToken ct)
    {
        var topic = _options.TopicPrefix + envelope.EventType;
        var sender = _senders.GetOrAdd(topic, t => _client.CreateSender(t));

        var message = new ServiceBusMessage(_serializer.Serialize(envelope.Payload!, typeof(TPayload)))
        {
            MessageId = envelope.MessageId.ToString(),       // dedup key
            SessionId = envelope.TenantId,                   // FIFO per tenant (session-enabled topics)
            Subject = envelope.EventType,
            ContentType = "application/json",
        };

        foreach (var (k, v) in envelope.Headers)
            message.ApplicationProperties[k] = v;

        await sender.SendMessageAsync(message, ct);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var sender in _senders.Values)
            await sender.DisposeAsync();
        await _client.DisposeAsync();
    }
}
```

#### Session-based FIFO

If you set `SessionId = TenantId` AND configure the topic with `RequiresSession = true`, ASB
guarantees per-tenant FIFO across the bus boundary. Combined with the outbox's per-tenant FIFO
inside `OutboxProcessor.ProcessBatchAsync`, you get end-to-end ordered delivery per tenant.

The cost: session-enabled subscriptions can only be consumed via `ServiceBusSessionReceiver`,
which adds complexity on the consumer side. Use sessions only when ordering is a real requirement.

#### Duplicate detection window

```bicep
resource topic 'Microsoft.ServiceBus/namespaces/topics@2022-10-01-preview' = {
  // ...
  properties: {
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M'  // 10 minutes
  }
}
```

The window must be ≥ your outbox's worst-case retry duration. With default outbox settings
(`MaxRetries=5`, `MaxBackoffSeconds=300`), the worst-case end-to-end is ~25 minutes — set the ASB
window to 30 min minimum, or shrink `MaxRetries` accordingly.

If a retry happens *outside* the window, ASB will accept the duplicate. Plan consumer dedup as a
backstop even with ASB native dedup enabled.

---

## 4. Job scheduler integrations

Job schedulers solve a different problem: "run this work later, retry it, show me a dashboard".
They can complement the outbox, but they don't replace it.

### 4.1 Hangfire

#### Don't use Hangfire to replace the outbox

The temptation: skip the outbox entirely and call `BackgroundJob.Enqueue(...)` from your producer.
This works ONLY if `Hangfire.SqlServer` (or `Hangfire.PostgreSql`) and your business DbContext
share the same database AND you wrap both in the same `TransactionScope`. The reliability is
brittle — Hangfire's storage drivers don't all support transactional enqueue, version upgrades
sometimes break the assumption, and you're now coupled to one vendor's storage.

The outbox solves this generically: any DbContext, any provider, any version. Use Hangfire for
what it's good at — execution and scheduling — not for what the outbox already gives you.

#### Pattern: outbox handler enqueues a Hangfire job

The outbox makes the *trigger* durable; Hangfire owns *execution*. The handler is a one-liner:

```csharp
public sealed class HangfireDelegatingHandler<TPayload, TJob> : IOutboxHandler<TPayload>
    where TJob : IPayloadJob<TPayload>
{
    private readonly IBackgroundJobClient _jobs;

    public HangfireDelegatingHandler(IBackgroundJobClient jobs) => _jobs = jobs;

    public Task HandleAsync(OutboxHandlerContext<TPayload> ctx, CancellationToken ct)
    {
        // Use the outbox MessageId as Hangfire's idempotency key so duplicate dispatch
        // (from outbox retries) doesn't enqueue twice.
        _jobs.Create(
            Job.FromExpression<TJob>(j => j.ExecuteAsync(ctx.Payload, ctx.MessageId, default)),
            new EnqueuedState());
        return Task.CompletedTask;
    }
}

public interface IPayloadJob<TPayload>
{
    Task ExecuteAsync(TPayload payload, Guid outboxMessageId, CancellationToken ct);
}
```

Registration:

```csharp
services.AddHangfire(c => c.UsePostgreSqlStorage(connectionString));
services.AddHangfireServer();
services.AddTransient<SendNewsletterJob>();   // implements IPayloadJob<NewsletterPayload>

services
    .AddOutbox<AppDbContext>(o => config.GetSection("Outbox").Bind(o))
    .AddProcessing()
    .AddHandler<NewsletterPayload, HangfireDelegatingHandler<NewsletterPayload, SendNewsletterJob>>(
        "newsletter.send");
```

#### When to use it

- **Long-running work** (minutes to hours). The outbox handler timeout (default 5 s) is too small.
  Hand off to Hangfire which has its own timeout / monitoring.
- **You already operate Hangfire** and want one place to see job execution.
- **Per-job retry policies** that differ from the outbox's defaults — Hangfire has rich retry
  attributes.

#### When NOT to use it

- **Quick, in-process work** (< 5 s). Adding Hangfire is operational overhead for nothing.
- **You don't already operate Hangfire**. Don't take on a dashboard service for a single use case.

#### Recurring/cron jobs

Use Hangfire's `RecurringJob.AddOrUpdate(...)` directly. The outbox has no role here — there's
nothing transactional about "run this every Monday".

### 4.2 Quartz.NET

#### Pattern

Quartz is more programmatic than Hangfire — no built-in dashboard, but stronger scheduling
semantics (calendar exclusions, misfire policies, clustered triggers).

```csharp
public sealed class QuartzDelegatingHandler<TPayload> : IOutboxHandler<TPayload>
{
    private readonly ISchedulerFactory _factory;
    private readonly QuartzDispatchOptions _options;

    public QuartzDelegatingHandler(ISchedulerFactory factory, IOptions<QuartzDispatchOptions> options)
    {
        _factory = factory;
        _options = options.Value;
    }

    public async Task HandleAsync(OutboxHandlerContext<TPayload> ctx, CancellationToken ct)
    {
        var scheduler = await _factory.GetScheduler(ct);

        var jobData = new JobDataMap
        {
            ["payload"] = JsonSerializer.Serialize(ctx.Payload),
            ["outbox_message_id"] = ctx.MessageId.ToString(),
            ["tenant_id"] = ctx.TenantId,
        };

        var jobKey = new JobKey(ctx.MessageId.ToString(), _options.JobGroup);
        var job = JobBuilder.Create<TPayloadJob>()
            .WithIdentity(jobKey)
            .UsingJobData(jobData)
            .RequestRecovery()       // re-run on scheduler crash
            .StoreDurably(false)
            .Build();

        var trigger = TriggerBuilder.Create()
            .ForJob(jobKey)
            .StartNow()
            .Build();

        await scheduler.ScheduleJob(job, trigger, ct);
    }
}
```

The handler uses the outbox `MessageId` as the Quartz job identity, so duplicate dispatch from
outbox retries collides at the scheduler ("job already exists") rather than running twice. Catch
`ObjectAlreadyExistsException` and treat it as success.

#### When to use Quartz over Hangfire

- **Complex schedules** — calendar-aware (exclude holidays), misfire instructions, multi-trigger jobs.
- **Clustered scheduler** — Quartz's clustered mode is more mature for "exactly one instance runs
  this trigger at this time" semantics.
- **No need for a dashboard** — fewer moving parts.

#### Recurring jobs

Same as Hangfire: configure them in Quartz directly, the outbox has no role.

---

## 5. End-to-end idempotency strategy

Plan idempotency at every link or it WILL bite you in production.

| Link | Risk | Mitigation |
|---|---|---|
| Producer enqueues twice (e.g. retry of an idempotent API) | Two outbox rows, two side-effects | Producer-side request deduplication; OR add a unique index on `(metadata->>'idempotency_key')` if your producers can supply stable keys. |
| Outbox dispatches twice (worker crash mid-dispatch) | Bus receives same payload twice, with same `MessageId` | At-least-once is by design; consumer-side dedup keyed on `MessageId`. |
| Bus delivers twice (consumer redelivery on ack failure) | Consumer processes twice | Consumer-side dedup table or upsert semantics in the consumer's writes. |
| Consumer crashes after side-effect, before ack | Bus redelivers, side-effect runs again | Side-effect must be idempotent OR record completion in the same transaction as the consumer's business write. |

The cleanest design: every consumer's business mutation is a `INSERT ... ON CONFLICT DO NOTHING`
(Postgres) or `MERGE` (SQL Server) keyed on `(message_id, target_id)`. Then re-delivery is
naturally absorbed.

### Idempotency key column (optional library extension)

If many of your producers can supply business-meaningful idempotency keys (request id, order id),
add a unique index:

```csharp
public class OutboxMessage {
    // ...existing fields...
    public string? IdempotencyKey { get; init; }
}

// In OutboxMessageConfiguration:
builder.HasIndex(m => m.IdempotencyKey)
    .IsUnique()
    .HasFilter("idempotency_key IS NOT NULL")
    .HasDatabaseName("ux_outbox_messages_idempotency_key");
```

The publisher catches the unique-violation and treats it as a no-op:

```csharp
try
{
    publisher.Enqueue(_db, payload, new OutboxPublishOptions { IdempotencyKey = order.RequestId });
    await _db.SaveChangesAsync(ct);
}
catch (DbUpdateException ex) when (IsUniqueViolation(ex, "ux_outbox_messages_idempotency_key"))
{
    _logger.LogInformation("Outbox enqueue ignored — duplicate idempotency key {Key}", order.RequestId);
}
```

This gives you producer-side dedup for free.

---

## 6. Distributed tracing across the boundary

The library emits one `Outbox.Dispatch` span per message, on `OutboxDiagnostics.ActivitySourceName`.
For a complete trace from "HTTP request created the order" to "downstream service consumed the
event", three context propagation steps are needed:

1. **HTTP → outbox row**: producer copies `Activity.Current?.Id` into `OutboxPublishOptions.Metadata["traceparent"]` (§3.3).
2. **Outbox row → bus message**: handler reads `ctx.Metadata["traceparent"]`, passes it as a bus
   header (Kafka header / RabbitMQ property / ASB application property).
3. **Bus message → consumer span**: consumer extracts `traceparent` from the bus header, sets it
   as parent of its own span before processing.

OpenTelemetry has built-in propagation helpers (`Propagators.DefaultTextMapPropagator.Inject` /
`Extract`); the manual approach above is what you use when the bus client doesn't have a
first-party OTel instrumentation. Confluent.Kafka has one; RabbitMQ.Client has one in 7.x; ASB
has one. Prefer the official instrumentation when available.

---

## 7. Migration playbook — adding the outbox to an existing app

You have a service that today does:

```csharp
public async Task PlaceOrder(Order order, CancellationToken ct)
{
    _db.Orders.Add(order);
    await _db.SaveChangesAsync(ct);                   // commits the order
    await _bus.PublishAsync(new OrderPlacedEvent(...)); // publishes — may fail, may double on retry
}
```

This is the classic dual-write bug. The migration is in three phases.

### Phase 1: Introduce the outbox alongside

```csharp
public async Task PlaceOrder(Order order, CancellationToken ct)
{
    _db.Orders.Add(order);

    // NEW: enqueue inside the same tx
    _publisher.Enqueue(_db, new OrderPlacedPayload(order.Id),
        new OutboxPublishOptions { TenantId = order.TenantId });

    await _db.SaveChangesAsync(ct);

    // OLD: keep this for now — we're going to switch over
    await _bus.PublishAsync(new OrderPlacedEvent(order.Id));
}
```

At this point you're publishing twice (outbox handler + direct), so consumer dedup must already
work. If it doesn't, fix that first — see §5.

### Phase 2: Switch to outbox-only

Register the bus-publishing handler:

```csharp
.AddHandler<OrderPlacedPayload, BusPublishingHandler<OrderPlacedPayload>>("order.placed");
```

Remove the direct `_bus.PublishAsync` call from the producer. The outbox handler is now the sole
publisher. Verify in production that the message-id stream into the bus is unbroken (no gaps)
during the switchover.

### Phase 3: Remove the legacy bus publisher

Once you've confirmed Phase 2 has run cleanly through one outage / restart cycle, remove the
direct `IBusPublisher` from the producer service entirely. Producers depend only on the outbox.

### Why this order

Going straight from direct-publish to outbox-only has a window where messages can be lost (during
deployment) or behave inconsistently (mixed-version pods). The dual-publish phase is uncomfortable
but safe — every message is published at least once whether through the old or new path; consumer
dedup absorbs the rest.

---

## 8. Anti-patterns

These come up repeatedly. They look fine at first; they hurt later.

### 8.1 Calling SaveChanges from the publisher

```csharp
// DON'T:
public async Task PublishAsync<T>(T payload) {
    using var db = _factory.CreateDbContext();
    db.OutboxMessages.Add(new OutboxMessage(...));
    await db.SaveChangesAsync();   // <-- not in business tx anymore
}
```

This defeats the entire point of the outbox. It guarantees the message is persisted, but not
*atomically with the business state*. If the business transaction rolls back after this call, you
have a phantom event for an order that doesn't exist.

The library's `IOutboxPublisher.Enqueue` deliberately doesn't call SaveChanges; if you find
yourself wanting to, you've decoupled it from the business tx.

### 8.2 Doing real work inside the producer

```csharp
// DON'T:
_publisher.Enqueue(_db, payload);
await _bus.PublishAsync(payload);    // <-- side-effect before commit
await _db.SaveChangesAsync();
```

This is the pre-outbox bug, ported. The bus publish happens before the business commit; if the
commit fails, you've already told downstream services about an event that didn't happen.

The producer NEVER does the side-effect. The outbox handler does. Producer code only ever stages
the intent.

### 8.3 Long-running handlers

```csharp
// DON'T:
public async Task HandleAsync(OutboxHandlerContext<MyPayload> ctx, CancellationToken ct)
{
    await _api.LongRunningExportAsync(ctx.Payload, ct);  // 30 minutes
}
```

The default `HandlerTimeoutSeconds` is 5. Raising it means a single stuck handler holds a worker
for 30 minutes, blocking everything behind it on that worker. And the lease will expire,
triggering a duplicate dispatch on another worker.

For long-running work, use the Hangfire/Quartz delegating pattern (§4) — the outbox handler is a
quick "trigger Hangfire job" call; Hangfire owns the long execution.

### 8.4 Replacing the outbox with the bus's transactional outbox feature

Both Kafka (transactional producer) and ASB (transactions) have "transactional" features. They
are NOT a substitute for the application outbox. They guarantee atomicity *within the bus*, not
between the bus and your application database. There is no two-phase commit between Postgres and
Kafka in production-grade form.

If you want the business write and the publish to be atomic with each other, the outbox table is
the only sound design. The bus's transactional features are useful for inter-topic atomicity once
you're already on the bus.

### 8.5 Storing large payloads in the outbox

The `payload` column is `text`, but it's not free. Multi-MB payloads inflate the table, slow the
backlog query, and bloat replication. Cap payloads at a few KB; for larger data, store the payload
in object storage (S3, Azure Blob) and put the URL in the outbox payload.

### 8.6 Sharing an outbox table between unrelated services

Tempting for ops simplicity. Disastrous for everything else: one service's permanent failure
backlog drags down another's claim performance, schema changes require coordinated deploys,
tenant_id meanings collide. Each service gets its own `outbox_messages` table in its own database.

---

## 9. Reference matrix

| Bus / Scheduler | Topic/Exchange/Queue | Routing | Idempotency | FIFO option | This library's role |
|---|---|---|---|---|---|
| **Kafka** | Topic per event_type | Partition by TenantId | Consumer-side (no native dedup) | Per-partition (= per TenantId) | Transactional emission of producer records |
| **RabbitMQ** | Topic exchange | Routing key = event_type | Consumer-side via MessageId | Per-queue (single consumer) | Transactional emission of basic publishes |
| **Azure Service Bus** | Topic per bounded context | Subject = event_type | Native, MessageId-based, windowed | Per-session (SessionId = TenantId) | Transactional emission of ServiceBusMessages |
| **Hangfire** | n/a | Job class | MessageId as job key | n/a | Trigger durability for delegated jobs |
| **Quartz** | n/a | Job key = MessageId | Job-already-exists collision | n/a | Trigger durability for scheduled jobs |

---

## 10. TL;DR

- **Bus integrations**: implement one `IBusPublisher`, register a generic `BusPublishingHandler<TPayload>`
  per event type. The outbox guarantees the message lands; the bus fans it out.
- **Job schedulers**: use them for execution and cron, not for transactional durability. The outbox
  trigger + delegating handler pattern gives you both.
- **Idempotency**: at-least-once at every link. Plan dedup by `MessageId` at the consumer.
- **Tracing**: propagate `traceparent` through `OutboxPublishOptions.Metadata` and into the bus
  message headers.
- **Don't replace the outbox** with a bus's "transactional" feature — they solve different problems.
- **Don't share** an outbox table across services. Each service owns its own.
