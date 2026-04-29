namespace OutboxTestInmemory.Outbox.Abstractions;

/// <summary>
/// Implement one per event type. The library deserializes the payload and resolves
/// your handler from DI on each invocation. Handlers SHOULD be idempotent —
/// the outbox guarantees at-least-once delivery, not exactly-once.
/// </summary>
public interface IOutboxHandler<TPayload>
{
    Task HandleAsync(OutboxHandlerContext<TPayload> context, CancellationToken ct);
}

/// <summary>
/// What a handler receives at dispatch time. Carries the deserialized payload
/// plus message metadata that handlers commonly need (retry count for adaptive
/// behavior, headers for trace context, etc.).
/// </summary>
public sealed record OutboxHandlerContext<TPayload>(
    Guid MessageId,
    string TenantId,
    string EventType,
    TPayload Payload,
    int RetryCount,
    DateTime CreatedAt,
    IReadOnlyDictionary<string, string> Metadata
);
