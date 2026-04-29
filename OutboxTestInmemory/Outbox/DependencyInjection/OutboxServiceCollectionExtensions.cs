using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Outbox.Configuration;
using OutboxTestInmemory.Outbox.Health;
using OutboxTestInmemory.Outbox.Persistence;
using OutboxTestInmemory.Outbox.Processing;
using OutboxTestInmemory.Outbox.Telemetry;

namespace OutboxTestInmemory.Outbox.DependencyInjection;

public static class OutboxServiceCollectionExtensions
{
    /// <summary>
    /// Register the outbox library against an application DbContext. Adds the publisher,
    /// store, dispatcher, default serializer/classifier, telemetry, and bound options.
    /// Call <c>.AddProcessing()</c> on the returned builder to enable workers,
    /// and <c>.AddHandler&lt;TPayload, THandler&gt;</c> for each event type.
    /// </summary>
    public static OutboxBuilder AddOutbox<TContext>(
        this IServiceCollection services,
        Action<OutboxOptions>? configure = null
    )
        where TContext : DbContext, IOutboxDbContext
    {
        services
            .AddOptions<OutboxOptions>()
            .Configure(o => configure?.Invoke(o))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.TryAddSingleton<TimeProvider>(TimeProvider.System);

        // Pluggable strategies — TryAdd so apps can override before AddOutbox is called.
        services.TryAddSingleton<IOutboxSerializer, JsonOutboxSerializer>();
        services.TryAddSingleton<IOutboxErrorClassifier, DefaultOutboxErrorClassifier>();

        // Core library plumbing.
        services.AddSingleton<IOutboxPublisher, OutboxPublisher>();
        services.AddSingleton<IOutboxStore, PostgresOutboxStore<TContext>>();
        services.AddSingleton<OutboxRegistry>();
        services.AddSingleton<OutboxDispatcher>();
        services.AddSingleton<OutboxProcessor>();
        services.AddSingleton<OutboxMetrics>();

        return new OutboxBuilder(services);
    }

    /// <summary>
    /// Register the backlog health check. Tag with "ready" by default to gate readiness.
    /// </summary>
    public static IHealthChecksBuilder AddOutboxBacklogHealthCheck(
        this IHealthChecksBuilder builder,
        string name = "outbox_backlog",
        params string[] tags
    )
    {
        return builder.AddCheck<OutboxBacklogHealthCheck>(name, tags: tags);
    }
}
