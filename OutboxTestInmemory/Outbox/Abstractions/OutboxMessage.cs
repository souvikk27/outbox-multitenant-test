namespace OutboxTestInmemory.Outbox.Abstractions;

/// <summary>
/// The durable record of a domain event awaiting dispatch.
/// Owned by the library; apps don't subclass it.
/// </summary>
public sealed class OutboxMessage
{
    public Guid Id { get; init; } = Guid.NewGuid();

    public string TenantId { get; init; } = null!;

    public string EventType { get; init; } = null!;

    public string Payload { get; init; } = null!;

    public string? Metadata { get; init; }

    public OutboxStatus Status { get; set; } = OutboxStatus.Pending;

    public int RetryCount { get; set; }

    /// <summary>Earliest UTC time at which a worker may claim this message.</summary>
    public DateTime AvailableAt { get; set; } = DateTime.UtcNow;

    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;

    public DateTime? ProcessedAt { get; set; }

    public DateTime? ClaimedAt { get; set; }

    public string? WorkerId { get; set; }

    public string? LastError { get; set; }
}

public enum OutboxStatus
{
    Pending = 0,
    Processing = 1,
    Processed = 2,
    Failed = 3,
}
