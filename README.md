# OutboxTestInmemory

A production-grade transactional outbox library for .NET 10 + PostgreSQL, with a sample consumer demonstrating idiomatic usage.

## Overview

The transactional outbox pattern solves the dual-write problem: when a service mutates business state and needs to emit a side-effect (send email, publish to a bus, call a webhook), both operations must be atomic. This library provides:

- **Atomic writes**: Business state and outbox messages commit together in the same database transaction
- **Reliable delivery**: Workers claim pending messages, execute handlers, and handle retries
- **Horizontal scaling**: `FOR UPDATE SKIP LOCKED` enables coordination-free multi-pod deployment
- **Observability**: Built-in OpenTelemetry metrics, tracing, and health checks
- **Tenant fairness**: Per-tenant FIFO with bounded cross-tenant parallelism

## Features

- 🔄 **Transactional enqueue** - Messages are written atomically with business state
- 🚀 **High throughput** - Configurable workers, batching, and tenant concurrency
- 🔄 **Automatic retries** - Exponential backoff with full jitter for transient failures
- 📊 **Built-in telemetry** - OpenTelemetry metrics and tracing out of the box
- 🏥 **Health checks** - Backlog monitoring for Kubernetes readiness
- 🔧 **Flexible handlers** - Implement `IOutboxHandler<TPayload>` for your side-effects
- 🌐 **Enterprise integrations** - Ready for Kafka, RabbitMQ, Azure Service Bus, Hangfire, Quartz

## Quick Start

### Prerequisites

- .NET 10.0 SDK
- PostgreSQL 12+
- Connection string to your database

### Installation

```bash
dotnet add package OutboxTestInmemory
```

### Basic Usage

**1. Configure your DbContext**

```csharp
public class AppDbContext : DbContext, IOutboxDbContext
{
    public DbSet<OutboxMessage> OutboxMessages { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyOutboxConfiguration();
    }
}
```

**2. Register the outbox**

```csharp
services.AddOutbox<AppDbContext>(o => config.GetSection("Outbox").Bind(o))
        .AddProcessing()
        .AddHandler<EmailPayload, EmailHandler>("email");
```

**3. Enqueue messages in your business transaction**

```csharp
public class OrderService
{
    private readonly AppDbContext _db;
    private readonly IOutboxPublisher _publisher;

    public async Task PlaceOrderAsync(Order order, CancellationToken ct)
    {
        _db.Orders.Add(order);
        _publisher.Enqueue(_db, new OrderPlacedPayload(order.Id),
            new OutboxPublishOptions { TenantId = order.TenantId });

        await _db.SaveChangesAsync(ct); // Atomic commit
    }
}
```

**4. Implement your handler**

```csharp
public sealed class OrderPlacedHandler : IOutboxHandler<OrderPlacedPayload>
{
    public async Task HandleAsync(OutboxHandlerContext<OrderPlacedPayload> ctx, CancellationToken ct)
    {
        // Idempotent handler implementation
        await _emailService.SendOrderConfirmationAsync(ctx.Payload.OrderId, ct);
    }
}
```

## Configuration

All settings live under the `Outbox` section in `appsettings.json`:

```json
{
  "Outbox": {
    "TableName": "outbox_messages",
    "WorkerCount": 4,
    "BatchSize": 100,
    "MaxTenantConcurrency": 8,
    "MaxRetries": 5,
    "PollIntervalMs": 500,
    "IdleBackoffMs": 2000,
    "HandlerTimeoutSeconds": 5,
    "LeaseTimeoutSeconds": 120,
    "LeaseRecoveryIntervalSeconds": 30,
    "MaxBackoffSeconds": 300,
    "BacklogWarningThreshold": 10000,
    "BacklogUnhealthyThreshold": 100000
  }
}
```

See [PROJECT_BIBLE.md](docs/PROJECT_BIBLE.md#6-configuration-reference) for detailed configuration options.

## Running the Sample

```bash
# Set connection string
export ConnectionStrings__DefaultConnection="Host=localhost;Database=outbox_db;Username=postgres;Password=postgres"

# Apply migrations
dotnet ef database update --context AppDbContext

# Run the service
dotnet run

# Seed test data
curl -X POST "http://localhost:5203/outbox/seed?count=1000"

# Check status
curl http://localhost:5203/outbox/status
```

## Architecture

```
Producer Service                          Consumer Service(s)
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
                                               └────────────────────────────────────────┘
```

## Documentation

- **[PROJECT_BIBLE.md](docs/PROJECT_BIBLE.md)** - Complete architecture, file-by-file reference, and usage guide
- **[INTEGRATIONS.md](docs/INTEGRATIONS.md)** - Enterprise integrations with Kafka, RabbitMQ, Azure Service Bus, Hangfire, Quartz
- **[METRIC_COLLECTOR.md](docs/METRIC_COLLECTOR.md)** - Performance testing, observability, and tuning guide

## Key Concepts

- **At-least-once delivery**: Handlers may run more than once; design them to be idempotent
- **Lease recovery**: Workers that crash mid-batch have their messages reclaimed after `LeaseTimeoutSeconds`
- **Tenant fairness**: Per-tenant FIFO with bounded cross-tenant parallelism prevents noisy neighbors
- **Error classification**: Transient errors retry with backoff; permanent errors move to dead-letter queue

## Health Checks

The library provides a backlog health check:

```csharp
services.AddHealthChecks()
    .AddNpgSql(connectionString)
    .AddOutboxBacklogHealthCheck();
```

Endpoints:
- `/health/live` - Always healthy
- `/health/ready` - Degraded at `BacklogWarningThreshold`, Unhealthy at `BacklogUnhealthyThreshold`

## Observability

### Metrics

All metrics are emitted on the `OutboxTestInmemory.Outbox` meter:

- `outbox.messages.processed` - Successful dispatches
- `outbox.messages.failed` - Failures by kind (transient/permanent)
- `outbox.messages.retried` - Transient failures rescheduled
- `outbox.messages.dead_lettered` - Moved to Failed
- `outbox.message.duration` - Handler invocation time
- `outbox.message.queue_delay` - Time waiting for a worker
- `outbox.lease.recovered` - Stuck rows reset by recovery sweep

### Tracing

Each dispatch produces an `Outbox.Dispatch` span with tags for message id, event type, tenant, and retry count.

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please read the documentation thoroughly before submitting changes. The `Outbox/` directory is intended as a reusable library - changes should be backward compatible and well-tested.
