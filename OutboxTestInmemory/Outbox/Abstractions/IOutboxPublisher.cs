namespace OutboxTestInmemory.Outbox.Abstractions;

/// <summary>
/// Write-side entrypoint. Producers call <see cref="Enqueue"/> inside their existing
/// business transaction; the message becomes visible to workers only after the caller
/// commits via <c>SaveChangesAsync</c>.
/// </summary>
public interface IOutboxPublisher
{
    /// <summary>
    /// Stages a typed payload as an outbox message inside the supplied DbContext.
    /// Does NOT call SaveChanges — the caller commits as part of their own unit of work.
    /// </summary>
    /// <returns>The staged message, useful for correlation/logging.</returns>
    OutboxMessage Enqueue<TPayload>(
        IOutboxDbContext context,
        TPayload payload,
        OutboxPublishOptions? options = null
    );
}

public sealed record OutboxPublishOptions
{
    /// <summary>Multi-tenant routing key. Required.</summary>
    public string TenantId { get; init; } = "default";

    /// <summary>Override the event type registered for <c>TPayload</c>.</summary>
    public string? EventType { get; init; }

    /// <summary>Schedule the message for future dispatch; defaults to immediate.</summary>
    public DateTimeOffset? AvailableAt { get; init; }

    /// <summary>Optional metadata header bag (trace context, idempotency keys, etc.).</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }
}
