using OutboxTestInmemory.Outbox.Configuration;

namespace OutboxTestInmemory.Outbox.Processing;

/// <summary>
/// Resolved view of all registered handlers, keyed by event type.
/// Built once at startup from the <see cref="OutboxHandlerRegistration"/> entries
/// the builder added to DI.
/// </summary>
internal sealed class OutboxRegistry
{
    private readonly IReadOnlyDictionary<string, IOutboxHandlerInvoker> _invokers;

    public OutboxRegistry(
        IEnumerable<OutboxHandlerRegistration> registrations,
        IServiceProvider serviceProvider
    )
    {
        var byEventType = new Dictionary<string, IOutboxHandlerInvoker>(StringComparer.Ordinal);
        foreach (var reg in registrations)
        {
            if (byEventType.ContainsKey(reg.EventType))
            {
                throw new InvalidOperationException(
                    $"Duplicate handler registration for event type '{reg.EventType}'."
                );
            }
            byEventType[reg.EventType] = reg.InvokerFactory(serviceProvider);
        }
        _invokers = byEventType;
    }

    public bool TryGet(string eventType, out IOutboxHandlerInvoker invoker) =>
        _invokers.TryGetValue(eventType, out invoker!);
}
