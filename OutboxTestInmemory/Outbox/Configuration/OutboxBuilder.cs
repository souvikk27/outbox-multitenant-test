using Microsoft.Extensions.DependencyInjection;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Outbox.Processing;

namespace OutboxTestInmemory.Outbox.Configuration;

/// <summary>
/// Fluent builder returned by <c>AddOutbox&lt;TContext&gt;</c>. Composes handlers,
/// processing, and pluggable strategies (serializer, error classifier).
/// </summary>
public sealed class OutboxBuilder
{
    public IServiceCollection Services { get; }

    internal OutboxBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Register a handler for a specific event type. The same payload type can be
    /// reused across multiple event types if you point them at different handlers.
    /// </summary>
    public OutboxBuilder AddHandler<TPayload, THandler>(string eventType)
        where THandler : class, IOutboxHandler<TPayload>
    {
        if (string.IsNullOrWhiteSpace(eventType))
            throw new ArgumentException("Event type is required.", nameof(eventType));

        Services.AddTransient<THandler>();
        Services.AddSingleton(
            new OutboxHandlerRegistration(
                EventType: eventType,
                PayloadType: typeof(TPayload),
                HandlerType: typeof(THandler),
                InvokerFactory: sp => new OutboxHandlerInvoker<TPayload, THandler>(
                    sp.GetRequiredService<IOutboxSerializer>()
                )
            )
        );

        // Also register a typed-payload publisher binding so producers can omit the event type.
        Services.AddSingleton(new OutboxPayloadBinding(typeof(TPayload), eventType));
        return this;
    }

    /// <summary>Replace the JSON serializer with a custom one.</summary>
    public OutboxBuilder UseSerializer<TSerializer>()
        where TSerializer : class, IOutboxSerializer
    {
        Services.AddSingleton<IOutboxSerializer, TSerializer>();
        return this;
    }

    /// <summary>Replace the default error classifier with domain-aware logic.</summary>
    public OutboxBuilder UseErrorClassifier<TClassifier>()
        where TClassifier : class, IOutboxErrorClassifier
    {
        Services.AddSingleton<IOutboxErrorClassifier, TClassifier>();
        return this;
    }

    /// <summary>
    /// Register the worker hosted services. Call this in services that consume the
    /// outbox; producer-only services can omit it.
    /// </summary>
    public OutboxBuilder AddProcessing()
    {
        Services.AddHostedService<OutboxHostedService>();
        Services.AddHostedService<OutboxLeaseRecoveryService>();
        return this;
    }
}

/// <summary>Maps an event type to its payload type, handler type, and invoker factory.</summary>
internal sealed record OutboxHandlerRegistration(
    string EventType,
    Type PayloadType,
    Type HandlerType,
    Func<IServiceProvider, IOutboxHandlerInvoker> InvokerFactory
);

/// <summary>Maps a payload CLR type to its registered event type for publisher convenience.</summary>
internal sealed record OutboxPayloadBinding(Type PayloadType, string EventType);
