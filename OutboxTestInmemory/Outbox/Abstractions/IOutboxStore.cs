namespace OutboxTestInmemory.Outbox.Abstractions;

/// <summary>
/// Persistence abstraction for the outbox. The library ships a Postgres implementation
/// (<c>FOR UPDATE SKIP LOCKED</c>); other stores can be added by implementing this contract.
/// </summary>
public interface IOutboxStore
{
    /// <summary>
    /// Atomically claim up to <paramref name="batchSize"/> messages whose
    /// <c>AvailableAt</c> &lt;= now and whose status is Pending. Implementations
    /// MUST guarantee no two callers see the same message — Postgres uses
    /// <c>FOR UPDATE SKIP LOCKED</c>; SQL Server uses readpast/updlock; etc.
    /// </summary>
    Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(
        string workerId,
        int batchSize,
        CancellationToken ct = default
    );

    /// <summary>Apply per-message terminal/retry decisions in a single transaction.</summary>
    Task BulkCompleteAsync(
        IReadOnlyCollection<OutboxMutation> mutations,
        CancellationToken ct = default
    );

    /// <summary>
    /// Reset rows stuck in <c>Processing</c> with a <c>ClaimedAt</c> older than
    /// <paramref name="leaseTimeout"/>. Returns the count recovered.
    /// </summary>
    Task<int> RecoverExpiredLeasesAsync(TimeSpan leaseTimeout, CancellationToken ct = default);

    /// <summary>Counts by status, plus the age of the oldest pending message.</summary>
    Task<OutboxBacklog> GetBacklogAsync(CancellationToken ct = default);

    /// <summary>Move a Failed message back to Pending so it gets re-attempted.</summary>
    Task<int> RequeueAsync(Guid messageId, CancellationToken ct = default);
}

/// <summary>
/// The decision a worker reached for one message in a claimed batch.
/// All state transitions flow through this record; the store turns them into UPDATEs.
/// </summary>
public sealed record OutboxMutation(
    Guid Id,
    OutboxStatus Status,
    int RetryCount,
    DateTime AvailableAt,
    DateTime? ProcessedAt,
    string? LastError
);

public sealed record OutboxBacklog(
    int Pending,
    int Processing,
    int Failed,
    int OldestPendingAgeSeconds
);
