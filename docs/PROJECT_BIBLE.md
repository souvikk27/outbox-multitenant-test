# Project Bible — OutboxTestInmemory

A production-grade transactional outbox library for .NET 10 + PostgreSQL, plus a sample consumer
that demonstrates idiomatic usage. The library half (`Outbox/`) is intended to be lifted directly
into other services; the sample half (`Sample/` + `Program.cs`) shows how a consuming application
wires it up.

---

## 1. What problem this solves

When a service mutates business state and *also* needs to emit a side-effect (send email, publish
to a bus, call a webhook), the two operations must be atomic. If you write the row and crash before
publishing, the event is lost. If you publish and crash before committing, the event is a lie.

The transactional outbox pattern solves this by:

1. **Producer** writes the business row AND a row in `outbox_messages` inside the same database
   transaction. The DB guarantees both commit or neither does.
2. **Worker** reads pending outbox rows, runs the side-effect, and marks them processed. Crashes,
   retries, and concurrency are handled by the worker — the producer is unaffected.

This library is the worker and the publisher API for that pattern, packaged so the producer side
stays a one-liner (`publisher.Enqueue(db, payload)`) and the worker side scales horizontally.

---

## 2. Architecture at a glance

```
                           Producer service                          Consumer service(s)
                           ─────────────────                          ───────────────────
   Order placed ──┐        ┌──────────────────────┐                  ┌──────────────────────┐
                  │        │ AppDbContext         │                  │ OutboxHostedService  │
                  ├──┐     │  Orders              │  business tx     │  └─ N workers        │
                  │  │     │  OutboxMessages ◄────┼──── commit ───┐  │                      │
                  │  │     └──────────────────────┘               │  │ LeaseRecoveryService │
                  │  ▼                                            │  └──────────┬───────────┘
                  │ IOutboxPublisher.Enqueue                      │             │
                  │                                               │             │
                  └─── orders + outbox_messages atomic ──────────►│             │
                                                                  ▼             ▼
                                            ┌────────────────────────────────────────┐
                                            │  Postgres                              │
                                            │  outbox_messages (status,available_at) │
                                            │  partial idx on Pending, Processing    │
                                            └────────────────────────────────────────┘
                                                                  ▲             ▲
                                                       FOR UPDATE │             │
                                                       SKIP LOCKED│             │
                                                                  │             │ recover
                                            ┌─────────────────────┴───┐         │ expired
                                            │ PostgresOutboxStore     │         │ leases
                                            │  ClaimBatchAsync        │         │
                                            │  BulkCompleteAsync      │         │
                                            │  RecoverExpiredLeases   │─────────┘
                                            └─────────────────────────┘
                                                       │
                                                       ▼
                                  ┌───────────────────────────────────────┐
                                  │ OutboxProcessor (per worker loop)     │
                                  │   claim → fan out per tenant →        │
                                  │   dispatch each → bulk-complete       │
                                  └──────────────────┬────────────────────┘
                                                     ▼
                                  ┌───────────────────────────────────────┐
                                  │ OutboxDispatcher (per message)        │
                                  │   resolve handler from Registry       │
                                  │   create scope, apply timeout         │
                                  │   invoke typed handler                │
                                  │   classify exception                  │
                                  │   produce OutboxMutation              │
                                  └──────────────────┬────────────────────┘
                                                     ▼
                                  ┌───────────────────────────────────────┐
                                  │ IOutboxHandler<TPayload>              │
                                  │   (consumer-supplied)                 │
                                  └───────────────────────────────────────┘
```

The split between **Processor** (batch lifecycle) and **Dispatcher** (per-message lifecycle) is
deliberate: it keeps the worker loop oblivious to handler shapes, and the dispatcher oblivious to
batching, scheduling, and lease semantics. Each can be reasoned about and tested in isolation.

---

## 3. Directory map

```
OutboxTestInmemory/
├── OutboxTestInmemory.slnx                 Solution file
├── OutboxTestInmemory.sln.DotSettings.user Rider/ReSharper user settings
└── OutboxTestInmemory/                     The single project
    ├── OutboxTestInmemory.csproj           Project file (net10.0, packages)
    ├── OutboxTestInmemory.http             Sample HTTP requests
    ├── Program.cs                          Composition root — DI, OTel, health, endpoints
    ├── appsettings.json                    Bound config (Outbox section)
    ├── appsettings.Development.json        Dev overrides incl. connection string
    ├── Properties/launchSettings.json      Launch profiles
    │
    ├── Outbox/                             ── REUSABLE LIBRARY ──
    │   ├── Abstractions/                   Public contracts apps depend on
    │   ├── Configuration/                  Options + fluent builder
    │   ├── DependencyInjection/            ServiceCollection extensions
    │   ├── Persistence/                    EF config, Postgres store, publisher
    │   ├── Processing/                     Worker, dispatcher, registry, defaults
    │   ├── Telemetry/                      Meter + ActivitySource
    │   └── Health/                         Backlog health check
    │
    ├── Sample/                             ── CONSUMER OF THE LIBRARY ──
    │   ├── Persistence/AppDbContext.cs     The app's DbContext (hosts outbox)
    │   ├── Email/EmailPayload.cs           Example typed payload
    │   ├── Email/EmailHandler.cs           Example IOutboxHandler<EmailPayload>
    │   └── Endpoints/OutboxAdminEndpoints  Admin HTTP API (seed/status/requeue)
    │
    └── Migrations/                         EF Core migrations for AppDbContext
        ├── 20260429055828_InitialCreate.cs
        └── AppDbContextModelSnapshot.cs
```

---

## 4. The library — file-by-file

This section is the canonical reference for the contents of `Outbox/`. Each entry says **what the
file is**, **why it exists**, and **when (if ever) you touch it**.

### 4.1 Abstractions — `Outbox/Abstractions/`

These are the only types apps need to reference by default. They form the public contract.

#### `OutboxMessage.cs`
The durable record of one event awaiting dispatch. Sealed; apps do not subclass.
- `Id` — primary key, generated client-side.
- `TenantId` — multi-tenant routing key. Required.
- `EventType` — string discriminator, used to resolve a handler.
- `Payload` — serialized form of the typed payload (JSON by default).
- `Metadata` — optional JSON header bag (trace context, idempotency keys).
- `Status` — `Pending | Processing | Processed | Failed`.
- `RetryCount` — incremented on each transient failure.
- `AvailableAt` — earliest UTC time a worker may claim. Drives both initial delay and retry backoff.
- `CreatedAt`, `ProcessedAt`, `ClaimedAt` — lifecycle timestamps.
- `WorkerId` — set on claim, cleared on completion. Used for lease attribution.
- `LastError` — truncated message of the most recent failure.

You touch this only if you query the table directly (admin tooling).

#### `OutboxStatus` (enum, same file)
Stored as `int` in the DB. The numeric values are load-bearing — `0=Pending`, `1=Processing` —
because the partial indexes have `WHERE status = 0` and `WHERE status = 1` baked in. **Do not
renumber.**

#### `IOutboxDbContext.cs`
Marker interface: `DbSet<OutboxMessage> OutboxMessages { get; }`. Apps' DbContexts implement this
so the library can:
- Have the publisher add to the same context the app uses for business state (transactional enqueue).
- Have the store query the table via a strongly-typed `DbSet`.

#### `IOutboxPublisher.cs`
The write-side entrypoint. One method:
```csharp
OutboxMessage Enqueue<TPayload>(IOutboxDbContext context, TPayload payload, OutboxPublishOptions? options = null);
```
Critically: **does not call SaveChanges**. The caller commits the message as part of their existing
unit of work, which is the entire point of the outbox pattern.

`OutboxPublishOptions` exposes `TenantId`, optional `EventType` override, optional `AvailableAt`
for delayed dispatch, and a metadata header dictionary.

#### `IOutboxHandler.cs`
Consumer-implemented contract:
```csharp
public interface IOutboxHandler<TPayload> {
    Task HandleAsync(OutboxHandlerContext<TPayload> context, CancellationToken ct);
}
```
The handler receives `OutboxHandlerContext<TPayload>` with:
- `MessageId`, `TenantId`, `EventType`, `CreatedAt` — provenance.
- `Payload` — already deserialized to `TPayload`.
- `RetryCount` — for handlers that adapt behavior on retry.
- `Metadata` — header bag the publisher set.

**Handlers must be idempotent.** The outbox guarantees at-least-once delivery, not exactly-once.

#### `IOutboxSerializer.cs`
`Serialize(object, Type)` / `Deserialize(string, Type)`. Default impl is JSON (`Web` defaults).
Swap via `OutboxBuilder.UseSerializer<T>()` if you need MessagePack, protobuf, etc.

#### `IOutboxErrorClassifier.cs`
Decides what an exception means:
- `Transient` — retry with backoff, counts toward `MaxRetries`.
- `Permanent` — skip retries, mark `Failed`.
- `Cancelled` — return to `Pending` without consuming an attempt (shutdown case).

The default classifier (in `Processing/`) is conservative. Apps with domain knowledge — e.g.
"4xx HTTP is permanent, 5xx is transient" — should provide their own.

#### `IOutboxStore.cs`
Persistence abstraction:
- `ClaimBatchAsync(workerId, batchSize, ct)` — atomic claim, returns claimed messages.
- `BulkCompleteAsync(mutations, ct)` — apply outcomes in one transaction.
- `RecoverExpiredLeasesAsync(timeout, ct)` — release messages stuck in `Processing`.
- `GetBacklogAsync(ct)` — counts by status + oldest-pending age. Drives the health check.
- `RequeueAsync(id, ct)` — move a `Failed` row back to `Pending`. Drives the DLQ requeue endpoint.

Same file defines `OutboxMutation` (per-message outcome record) and `OutboxBacklog` (status counts).

### 4.2 Configuration — `Outbox/Configuration/`

#### `OutboxOptions.cs`
All knobs in one place, bound from the `Outbox` section of `appsettings.json` and validated at
startup via `ValidateDataAnnotations` + `ValidateOnStart`. Each property has a `[Range]` attribute
to fail fast on misconfiguration. See §6 for the full reference.

#### `OutboxBuilder.cs`
The fluent builder returned by `AddOutbox<TContext>`. Methods:
- `AddHandler<TPayload, THandler>(eventType)` — registers handler in DI and creates an
  `OutboxHandlerRegistration` (event type → payload type → handler type → invoker factory).
- `UseSerializer<T>()` / `UseErrorClassifier<T>()` — replace defaults.
- `AddProcessing()` — adds the two `BackgroundService`s. Omit on producer-only services.

Same file declares two internal records: `OutboxHandlerRegistration` (the handler-side registry
entry) and `OutboxPayloadBinding` (publisher-side `payload type → event type` map, so producers
don't have to specify `EventType` when there's only one binding for a payload).

### 4.3 DI — `Outbox/DependencyInjection/`

#### `OutboxServiceCollectionExtensions.cs`
The single entrypoint:
```csharp
services.AddOutbox<AppDbContext>(opts => config.GetSection("Outbox").Bind(opts))
        .AddProcessing()
        .AddHandler<EmailPayload, EmailHandler>("email");
```
Internals: registers options + validation, `TimeProvider.System`, default serializer/classifier
(via `TryAdd` so apps can pre-register their own), publisher, `PostgresOutboxStore<TContext>`,
`OutboxRegistry`, `OutboxDispatcher`, `OutboxProcessor`, `OutboxMetrics`.

Also exposes `AddOutboxBacklogHealthCheck()` as an extension on `IHealthChecksBuilder`.

### 4.4 Persistence — `Outbox/Persistence/`

#### `OutboxMessageConfiguration.cs`
EF `IEntityTypeConfiguration<OutboxMessage>`. Owns the column names, lengths, conversions, and
indexes:
- `ix_outbox_messages_pending_due` — `(status, available_at)` filtered on `status=0`. The hot path.
- `ix_outbox_messages_processing_claimed` — `(status, claimed_at)` filtered on `status=1`. Used by
  lease recovery.
- `ix_outbox_messages_tenant_id` — for tenant-scoped queries (admin tooling).

Constructor takes table name + optional schema, so the `OutboxOptions.TableName/.Schema` flow
through.

Same file: `OutboxModelBuilderExtensions.ApplyOutboxConfiguration(...)`. Apps call this in
`OnModelCreating` to wire the entity into their own DbContext.

#### `PostgresOutboxStore.cs`
`PostgresOutboxStore<TContext>` where `TContext : DbContext, IOutboxDbContext`. Generic over the
app's DbContext type so the same store can serve any consumer. Implementation details:
- `ClaimBatchAsync` issues one `WITH cte AS (SELECT ... FOR UPDATE SKIP LOCKED) UPDATE ... RETURNING`
  statement. This is the load-bearing primitive: any number of workers (in-process or across pods)
  can claim concurrently without blocking and without ever returning the same row twice.
- `BulkCompleteAsync` uses `ExecuteUpdateAsync` per mutation inside one `BeginTransactionAsync`.
  Per-row UPDATEs are needed because each row's `RetryCount`, `AvailableAt`, `LastError` are different,
  but they amortize one network round-trip into one transaction.
- `RecoverExpiredLeasesAsync` is a single `ExecuteUpdateAsync` over the partial index — fast.
- `GetBacklogAsync` — two cheap queries (status histogram + oldest pending), used for health and
  the `/outbox/status` endpoint.
- `RequeueAsync` — a single `ExecuteUpdateAsync` filtered on `Status == Failed`.

Table identifiers are quoted; schema is opt-in via `OutboxOptions.Schema`.

#### `OutboxPublisher.cs` (internal)
Implementation of `IOutboxPublisher`. Builds an `OutboxMessage`, serializes the payload via
`IOutboxSerializer`, and calls `context.OutboxMessages.Add(message)`. Resolves the event type from
the `OutboxPayloadBinding` map unless the caller overrode it via `OutboxPublishOptions.EventType`.

### 4.5 Processing — `Outbox/Processing/`

#### `IOutboxHandlerInvoker.cs` + `OutboxHandlerInvoker.cs` (internal)
Bridges the runtime `string EventType` to a strongly-typed `IOutboxHandler<TPayload>` call. One
concrete `OutboxHandlerInvoker<TPayload, THandler>` is built per registration at startup, which
means dispatch does no per-call reflection. The invoker:
1. Deserializes the payload via the registered serializer.
2. Parses `Metadata` JSON into a dictionary.
3. Builds the typed `OutboxHandlerContext<TPayload>`.
4. Resolves the handler from the supplied scoped `IServiceProvider`.
5. Awaits `handler.HandleAsync(...)`.

#### `OutboxRegistry.cs` (internal)
Built once at startup from all `OutboxHandlerRegistration` entries; produces an immutable
`Dictionary<string, IOutboxHandlerInvoker>`. Throws on duplicate registration to fail fast on
config errors.

#### `JsonOutboxSerializer.cs` (internal)
The default `IOutboxSerializer`. Wraps `System.Text.Json.JsonSerializer` with web defaults
(camelCase property names, case-insensitive deserialization). Throws on null deserialization.

#### `DefaultOutboxErrorClassifier.cs` (internal)
The default `IOutboxErrorClassifier`. Maps:
- `OperationCanceledException` → `Cancelled` (shutdown — return to Pending without consuming attempt).
- `ArgumentException`, `FormatException`, `InvalidOperationException`, `NotSupportedException`,
  `JsonException` → `Permanent` (data/programming errors won't fix themselves).
- `TimeoutException`, `HttpRequestException`, `IOException` → `Transient`.
- Everything else → `Transient` (fail-safe; assume retryable).

Replace this in any service where you can do better.

#### `OutboxDispatcher.cs` (internal)
Per-message orchestration:
1. Open an `Activity` from `OutboxDiagnostics.ActivitySource` for tracing.
2. Look up the invoker by event type. Unknown event type → `Permanent` failure (no handler is
   going to appear later in the same deployment).
3. Create a per-message DI scope so handlers can resolve scoped services (e.g. their own DbContext).
4. Apply `HandlerTimeoutSeconds` via a linked `CancellationTokenSource`.
5. Invoke; on success build a `Processed` mutation, emit metrics + activity status.
6. On exception: classify, build `Pending`/`Failed`/cancellation mutation. Compute next
   `AvailableAt` as exponential backoff with **full jitter** (random in `[0, min(2^attempt, MaxBackoffSeconds)]`),
   capped to avoid pathological waits.

The dispatcher *never* mutates the message in place. Every state transition is an
`OutboxMutation` value applied later by the store.

#### `OutboxProcessor.cs` (internal)
The worker loop. One method: `RunWorkerAsync(workerId, ct)`. Per iteration:
1. `ClaimBatchAsync(workerId, BatchSize)`.
2. If empty: `Task.Delay(IdleBackoffMs)` then continue.
3. Group claimed messages by `TenantId`.
4. `Parallel.ForEachAsync` over partitions with `MaxDegreeOfParallelism = MaxTenantConcurrency`
   — gives bounded cross-tenant parallelism while preserving per-tenant FIFO.
5. Inside a partition: dispatch each message in order, collect mutations.
6. `BulkCompleteAsync(mutations)` — using `CancellationToken.None` so shutdown can't leave rows
   stuck in `Processing`.
7. `Task.Delay(PollIntervalMs)` before next claim.

Worker errors (DB blip, serialization bug) increment `outbox.worker.errors`, log, and back off —
never crash the loop.

#### `OutboxHostedService.cs` (internal)
`BackgroundService` that spawns `WorkerCount` parallel `RunWorkerAsync` calls. Each worker gets a
stable `workerId` of the form `{machine}:{pid}/wN` so lease attribution survives restarts (good
for log correlation).

#### `OutboxLeaseRecoveryService.cs` (internal)
Independent `BackgroundService` that runs `RecoverExpiredLeasesAsync` every
`LeaseRecoveryIntervalSeconds`. The price of a worker crashing mid-batch is one extra dispatch
attempt, not a stuck message.

### 4.6 Telemetry — `Outbox/Telemetry/`

#### `OutboxDiagnostics.cs`
Public constants `ActivitySourceName` and `MeterName`, plus the `ActivitySource` instance. These
are the contract apps subscribe to in their OpenTelemetry config.

#### `OutboxMetrics.cs` (internal)
Owns the `Meter` and exposes typed `Record*` methods. Counters:
- `outbox.messages.processed` — success.
- `outbox.messages.failed` — failure (tagged `kind=transient|permanent`).
- `outbox.messages.retried` — scheduled for retry.
- `outbox.messages.dead_lettered` — gave up (Failed).
- `outbox.messages.claimed` — claim batch sizes.
- `outbox.lease.recovered` — lease recovery sweep results.
- `outbox.worker.errors` — worker loop errors.

Histogram:
- `outbox.message.duration` (ms) — handler invocation time.

All counters/histograms are tagged with `event_type` and `tenant_id` (where applicable) so dashboards
can slice by either.

### 4.7 Health — `Outbox/Health/`

#### `OutboxBacklogHealthCheck.cs` (internal)
Calls `IOutboxStore.GetBacklogAsync` and maps the pending count to:
- `>= BacklogUnhealthyThreshold` → `Unhealthy`.
- `>= BacklogWarningThreshold` → `Degraded`.
- otherwise → `Healthy`.

Returns the full backlog snapshot in `data` so the response body is useful.

---

## 5. The sample app — file-by-file

The sample exists to show idiomatic library usage. None of these files are needed to consume the
library in your own service.

#### `Sample/Persistence/AppDbContext.cs`
A minimal `DbContext` that implements `IOutboxDbContext` and calls `ApplyOutboxConfiguration()`.
In a real service this also holds business entities (`DbSet<Order>`, etc.).

#### `Sample/Email/EmailPayload.cs`
A typed payload record. Records work great for outbox payloads — immutable, `init`-only, JSON
round-trips for free.

#### `Sample/Email/EmailHandler.cs`
Example `IOutboxHandler<EmailPayload>`. Throws `TimeoutException` ~25% of the time (transient) and
`InvalidOperationException` ~5% of the time (permanent), so you can watch the retry/DLQ paths
work. The default classifier maps both correctly.

#### `Sample/Endpoints/OutboxAdminEndpoints.cs`
HTTP endpoints under `/outbox`:
- `POST /outbox/seed?count=N` — generates N email events using `IOutboxPublisher.Enqueue` then
  `SaveChangesAsync`. This is the canonical demonstration of the producer flow.
- `GET /outbox/status` — returns `OutboxBacklog`.
- `POST /outbox/dlq/requeue/{id}` — moves a `Failed` message back to `Pending`.

#### `Program.cs`
The composition root. Wires up:
- `AppDbContext` factory with `EnableRetryOnFailure(3)` and `NoTracking` query default.
- `services.AddOutbox<AppDbContext>(...).AddProcessing().AddHandler<EmailPayload, EmailHandler>("email")`.
- Health checks: NpgSql + outbox backlog, both tagged `ready`.
- OpenTelemetry: meter and activity source registered, OTLP exporter for both metrics and traces.
- `/health/live` (always 200) and `/health/ready` (gated on the `ready`-tagged checks).

---

## 6. Configuration reference

All settings live under `"Outbox"` in `appsettings.json`. Bound to `OutboxOptions`, validated at
startup.

| Setting | Default | Range | Purpose |
|---|---|---|---|
| `TableName` | `outbox_messages` | — | Physical table name. Override for naming conventions. |
| `Schema` | `null` | — | Optional Postgres schema. |
| `WorkerCount` | `4` | 1–64 | Concurrent workers per process. Scale per CPU and DB capacity. |
| `BatchSize` | `100` | 1–1000 | Messages claimed per tick. Bigger = better throughput, longer lease. |
| `MaxTenantConcurrency` | `8` | 1–64 | Parallel tenants processed inside one batch. |
| `MaxRetries` | `5` | 1–100 | Transient retries before moving to `Failed`. |
| `PollIntervalMs` | `500` | 100–60000 | Sleep between successful batches. |
| `IdleBackoffMs` | `2000` | 100–60000 | Sleep when no work was claimed. |
| `HandlerTimeoutSeconds` | `5` | 1–600 | Per-message handler timeout. Cancels via linked CTS. |
| `LeaseTimeoutSeconds` | `120` | 10–3600 | After this, a `Processing` row is presumed orphaned. |
| `LeaseRecoveryIntervalSeconds` | `30` | 10–3600 | How often the lease sweep runs. |
| `MaxBackoffSeconds` | `300` | 1–600 | Cap on retry-backoff jitter window. |
| `BacklogWarningThreshold` | `10000` | 0+ | Pending count → health `Degraded`. |
| `BacklogUnhealthyThreshold` | `100000` | 0+ | Pending count → health `Unhealthy`. |

### Connection string
**Not** in `appsettings.json` for production. Set:
```
ConnectionStrings__DefaultConnection=Host=...;Database=...;Username=...;Password=...
```
The dev connection string lives in `appsettings.Development.json` for convenience and is not used
outside Development environment.

---

## 7. Database schema

Generated by EF migration `20260429055828_InitialCreate`.

```sql
CREATE TABLE outbox_messages (
    id            uuid                     PRIMARY KEY,
    tenant_id     varchar(128)             NOT NULL,
    event_type    varchar(128)             NOT NULL,
    payload       text                     NOT NULL,
    metadata      text                     NULL,
    status        integer                  NOT NULL,    -- 0=Pending 1=Processing 2=Processed 3=Failed
    retry_count   integer                  NOT NULL,
    available_at  timestamptz              NOT NULL,
    created_at    timestamptz              NOT NULL,
    processed_at  timestamptz              NULL,
    claimed_at    timestamptz              NULL,
    worker_id     varchar(64)              NULL,
    last_error    text                     NULL
);

CREATE INDEX ix_outbox_messages_pending_due
    ON outbox_messages (status, available_at) WHERE status = 0;

CREATE INDEX ix_outbox_messages_processing_claimed
    ON outbox_messages (status, claimed_at)   WHERE status = 1;

CREATE INDEX ix_outbox_messages_tenant_id
    ON outbox_messages (tenant_id);
```

The partial indexes are critical: the hot claim query touches only `status = 0` rows, the lease
sweep touches only `status = 1` rows. As `Processed` rows accumulate they don't bloat either index.

---

## 8. How to use the library

### 8.1 Producer-side: enqueue inside a business transaction

```csharp
public class OrderService
{
    private readonly AppDbContext _db;
    private readonly IOutboxPublisher _publisher;

    public OrderService(AppDbContext db, IOutboxPublisher publisher)
    {
        _db = db;
        _publisher = publisher;
    }

    public async Task PlaceOrderAsync(Order order, CancellationToken ct)
    {
        _db.Orders.Add(order);
        _publisher.Enqueue(_db, new OrderPlacedPayload(order.Id, order.Total),
            new OutboxPublishOptions { TenantId = order.TenantId });

        await _db.SaveChangesAsync(ct);   // ← orders + outbox row commit atomically
    }
}
```

If `SaveChangesAsync` throws (FK violation, unique constraint), neither the order nor the message
is written. There is no window in which a side-effect could fire for an order that doesn't exist.

### 8.2 Consumer-side: implement a handler

```csharp
public sealed class OrderPlacedHandler : IOutboxHandler<OrderPlacedPayload>
{
    private readonly IEmailService _email;

    public OrderPlacedHandler(IEmailService email) => _email = email;

    public async Task HandleAsync(OutboxHandlerContext<OrderPlacedPayload> ctx, CancellationToken ct)
    {
        // Idempotency: ctx.MessageId is stable across retries.
        await _email.SendOrderConfirmationAsync(ctx.Payload.OrderId, ct);
    }
}
```

Register both:
```csharp
services
    .AddOutbox<AppDbContext>(o => config.GetSection("Outbox").Bind(o))
    .AddProcessing()
    .AddHandler<OrderPlacedPayload, OrderPlacedHandler>("order.placed");
```

### 8.3 Mixed (most common): one service that publishes and processes

The default registration enables both. Producers and the worker run in the same process.

### 8.4 Split topology: producer-only and consumer-only services

**Producer service** — only the publisher, no workers:
```csharp
services
    .AddOutbox<AppDbContext>(o => config.GetSection("Outbox").Bind(o));
    // no .AddProcessing()
```

**Consumer service** — workers, no producer code:
```csharp
services
    .AddOutbox<AppDbContext>(o => config.GetSection("Outbox").Bind(o))
    .AddProcessing()
    .AddHandler<OrderPlacedPayload, OrderPlacedHandler>("order.placed");
```

Both services point at the same Postgres database. The `FOR UPDATE SKIP LOCKED` guarantee means
running multiple consumer service instances scales linearly without coordination.

### 8.5 Custom error classification

```csharp
public class HttpAwareClassifier : IOutboxErrorClassifier
{
    public OutboxErrorClassification Classify(Exception ex, OutboxMessage msg, int attempt) => ex switch
    {
        HttpRequestException { StatusCode: HttpStatusCode.NotFound } => OutboxErrorClassification.Permanent,
        HttpRequestException { StatusCode: HttpStatusCode.BadRequest } => OutboxErrorClassification.Permanent,
        HttpRequestException { StatusCode: >= HttpStatusCode.InternalServerError } => OutboxErrorClassification.Transient,
        _ => OutboxErrorClassification.Transient,
    };
}

services.AddOutbox<AppDbContext>(...)
        .AddProcessing()
        .UseErrorClassifier<HttpAwareClassifier>();
```

### 8.6 Custom serializer

Implement `IOutboxSerializer`, register via `.UseSerializer<MyMessagePackSerializer>()`. Make sure
both producer-side and consumer-side services use the same serializer.

---

## 9. Operating it

### 9.1 Local dev

```
ConnectionStrings__DefaultConnection=Host=localhost;Database=outbox_db;Username=postgres;Password=postgres
dotnet ef database update --context AppDbContext
dotnet run
```

Workers start immediately. Drive load via:
```
POST http://localhost:5203/outbox/seed?count=5000
GET  http://localhost:5203/outbox/status
```

### 9.2 Scaling

- **Vertical**: bump `WorkerCount` + `MaxTenantConcurrency` until DB CPU or downstream service
  saturates. `BatchSize` × `WorkerCount` is the in-flight message ceiling per pod.
- **Horizontal**: add pods. `FOR UPDATE SKIP LOCKED` makes them coordination-free. Each pod's
  workers see their own slice of pending rows.
- **DB capacity**: the hot query is an indexed point read (`status=0 AND available_at <= now`).
  pgbench numbers around 5–10k claims/sec are achievable on commodity Postgres before lock
  contention starts mattering — far past most apps' actual event volume.

### 9.3 Observability

- **Metrics**: subscribed via `meter.AddMeter(OutboxDiagnostics.MeterName)`. Watch
  `outbox.messages.processed` rate and `outbox.message.duration` p95. Alert on
  `outbox.messages.dead_lettered` and `outbox.lease.recovered` (a handful is fine; sustained
  recovery means workers are crashing).
- **Tracing**: subscribed via `tracer.AddSource(OutboxDiagnostics.ActivitySourceName)`. Each dispatch
  produces one `Outbox.Dispatch` span tagged with message id, event type, tenant, retry count.
- **Health**: `/health/ready` flips to `Degraded` at `BacklogWarningThreshold` and `Unhealthy` at
  `BacklogUnhealthyThreshold`. Wire to Kubernetes readiness so saturated pods stop receiving HTTP
  traffic but keep draining the outbox.
- **Logs**: structured via `ILogger<T>`. Worker, dispatcher, store, recovery service all log with
  consistent fields (`MessageId`, `WorkerId`, `EventType`, `TenantId`, `Count`).

### 9.4 Failure modes & guarantees

| Failure | Result |
|---|---|
| Worker crashes mid-batch | Lease recovery resets stuck rows to `Pending` after `LeaseTimeoutSeconds`. At-least-once guarantee preserved. |
| Handler throws transient | Mutation: `Pending`, `RetryCount++`, `AvailableAt = now + jitter(2^attempt)`. |
| Handler throws permanent | Mutation: `Failed`, `LastError` set. Visible via `/outbox/status` and requeueable. |
| Handler exceeds timeout | Linked `CancellationTokenSource` fires. The `OperationCanceledException` is classified by your `IOutboxErrorClassifier` (`Cancelled` by default → returns to Pending without consuming attempt). |
| DB unreachable | EF retry-on-failure absorbs transient blips (3 attempts). Beyond that, the worker loop catches the exception, logs, and backs off — never crashes. |
| Process SIGTERM | `BackgroundService.StopAsync` cancels stoppingToken. In-flight handlers receive cancellation; their mutations are still persisted via `BulkCompleteAsync(CT.None)`. Clean stop. |
| Two pods running simultaneously | `FOR UPDATE SKIP LOCKED` partitions claims naturally. No work duplicated. |
| Same pod, multiple workers | Same — claims are serialized at the row-lock level inside one transaction. |

### 9.5 Migrations

```
dotnet ef migrations add <Name> --context AppDbContext
dotnet ef database update --context AppDbContext
```

The library's EF config is applied via `ApplyOutboxConfiguration()` in your context's
`OnModelCreating`. If you change `TableName`/`Schema`, generate a new migration — EF will produce
a `RenameTable` operation.

---

## 10. What's deliberately not in scope

These are real concerns; they're omitted because YAGNI for most consumers. Don't add them
speculatively.

- **Idempotency dedup at the storage layer** — handlers must be idempotent themselves. Adding a
  unique `IdempotencyKey` index is one column away if your producers can supply stable keys.
- **Multiple outbox streams per service** — would require named options + named DbContexts. Doable
  if a real use case appears; today every service has one outbox.
- **Distributed tracing context propagation through `Metadata`** — the column exists and the
  publisher accepts a metadata dict, but the library doesn't auto-inject `traceparent`. If you
  need cross-service trace continuity, populate `OutboxPublishOptions.Metadata` in your producer
  and read it in your handler.
- **Polly / in-process retry around the handler call** — intentionally removed. The outbox itself
  is the durable retry mechanism; layering Polly on top would mean retries that don't survive a
  worker crash. If your handler does its own HTTP calls, put Polly *inside* the handler around
  those calls.
- **Sharding / partitioning the outbox table** — not needed at expected scale. If pending grows
  past ~10M rows it's worth revisiting; until then, the partial index keeps the hot path fast.
- **A separate dead-letter table** — `Status = Failed` with the partial-index design is sufficient.
  An admin tool can list and requeue from there.
- **Built-in unit/integration tests** — the abstractions are designed to be testable (every
  external dependency is an interface, `TimeProvider` is injected, mutations are pure records),
  but no test project ships with the library. Add one in your service.

---

## 11. Quick reference

### File you change to add a new event type
1. Define a payload record in your app's domain folder.
2. Implement `IOutboxHandler<TPayload>`.
3. Register: `.AddHandler<TPayload, THandler>("event-type-string")`.

That's it. No library code to touch.

### File you change to tune throughput
`appsettings.json` — `WorkerCount`, `BatchSize`, `MaxTenantConcurrency`, `PollIntervalMs`.

### File you change to deploy to a new database
`appsettings.json` connection string + run `dotnet ef database update --context AppDbContext`.

### File you change to add domain-specific error handling
Implement `IOutboxErrorClassifier`, register via `.UseErrorClassifier<T>()`.

### Files you should not need to change
Anything in `Outbox/Processing/` (workers, dispatcher, registry, defaults), `Outbox/Persistence/`
(store, EF config), `Outbox/Telemetry/`, `Outbox/Health/`. These are library internals — submit a
PR upstream rather than forking.

---

## 12. Glossary

- **Claim** — atomic transition `Pending → Processing` with `claimed_at` and `worker_id` set.
- **Mutation** — an `OutboxMutation` record returned by the dispatcher; the store applies it as
  one or more column updates inside a transaction.
- **Lease** — the implicit ownership a worker has over a `Processing` row from claim until
  completion. Bounded by `LeaseTimeoutSeconds`.
- **Lease recovery** — the periodic sweep that releases rows whose lease expired (presumed worker
  crash). Resets them to `Pending` so another worker can claim.
- **Dead-letter** — a row in `Status = Failed`. Surfaced via `/outbox/status` and `/outbox/dlq/requeue/{id}`.
- **At-least-once** — the delivery guarantee. Handlers may run more than once for the same
  `MessageId`; design them to be idempotent.
- **Tenant fairness** — the `Parallel.ForEachAsync` over tenant partitions in
  `OutboxProcessor.ProcessBatchAsync` ensures one slow tenant can't monopolize a worker for the
  full batch duration.
