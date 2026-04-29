using System.Text.Json;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Outbox.Configuration;

namespace OutboxTestInmemory.Outbox.Persistence;

internal sealed class OutboxPublisher : IOutboxPublisher
{
    private readonly IOutboxSerializer _serializer;
    private readonly TimeProvider _time;
    private readonly IReadOnlyDictionary<Type, string> _typeToEvent;

    public OutboxPublisher(
        IOutboxSerializer serializer,
        TimeProvider time,
        IEnumerable<OutboxPayloadBinding> bindings
    )
    {
        _serializer = serializer;
        _time = time;
        // Last registration wins per payload type; same payload reused across event types
        // forces the caller to specify EventType explicitly via OutboxPublishOptions.
        _typeToEvent = bindings
            .GroupBy(b => b.PayloadType)
            .ToDictionary(g => g.Key, g => g.Last().EventType);
    }

    public OutboxMessage Enqueue<TPayload>(
        IOutboxDbContext context,
        TPayload payload,
        OutboxPublishOptions? options = null
    )
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(payload);

        options ??= new OutboxPublishOptions();
        var eventType = options.EventType ?? ResolveEventType(typeof(TPayload));

        var availableAt = options.AvailableAt?.UtcDateTime ?? _time.GetUtcNow().UtcDateTime;

        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            TenantId = options.TenantId,
            EventType = eventType,
            Payload = _serializer.Serialize(payload, typeof(TPayload)),
            Metadata = options.Metadata is { Count: > 0 }
                ? JsonSerializer.Serialize(options.Metadata)
                : null,
            Status = OutboxStatus.Pending,
            AvailableAt = availableAt,
            CreatedAt = _time.GetUtcNow().UtcDateTime,
        };

        context.OutboxMessages.Add(message);
        return message;
    }

    private string ResolveEventType(Type payloadType)
    {
        if (_typeToEvent.TryGetValue(payloadType, out var evt))
            return evt;

        throw new InvalidOperationException(
            $"No event type registered for payload '{payloadType.FullName}'. "
                + $"Either register a handler with AddHandler<{payloadType.Name}, ...>(\"event-type\") "
                + $"or set OutboxPublishOptions.EventType explicitly."
        );
    }
}
