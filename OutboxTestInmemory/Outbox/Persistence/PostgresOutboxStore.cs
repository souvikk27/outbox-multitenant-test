using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Outbox.Configuration;
using OutboxTestInmemory.Outbox.Telemetry;

namespace OutboxTestInmemory.Outbox.Persistence;

/// <summary>
/// Postgres implementation of <see cref="IOutboxStore"/>. Uses
/// <c>FOR UPDATE SKIP LOCKED</c> so any number of workers — in-process or
/// across pods — can claim concurrently without blocking or double-dispatch.
/// </summary>
internal sealed class PostgresOutboxStore<TContext> : IOutboxStore
    where TContext : DbContext, IOutboxDbContext
{
    private readonly IDbContextFactory<TContext> _contextFactory;
    private readonly TimeProvider _time;
    private readonly OutboxOptions _options;
    private readonly OutboxMetrics _metrics;
    private readonly ILogger<PostgresOutboxStore<TContext>> _logger;
    private readonly string _qualifiedTable;

    public PostgresOutboxStore(
        IDbContextFactory<TContext> contextFactory,
        TimeProvider time,
        IOptions<OutboxOptions> options,
        OutboxMetrics metrics,
        ILogger<PostgresOutboxStore<TContext>> logger
    )
    {
        _contextFactory = contextFactory;
        _time = time;
        _options = options.Value;
        _metrics = metrics;
        _logger = logger;
        _qualifiedTable = string.IsNullOrEmpty(_options.Schema)
            ? Quote(_options.TableName)
            : $"{Quote(_options.Schema!)}.{Quote(_options.TableName)}";
    }

    public async Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(
        string workerId,
        int batchSize,
        CancellationToken ct = default
    )
    {
        await using var context = await _contextFactory.CreateDbContextAsync(ct);
        var now = _time.GetUtcNow().UtcDateTime;

        // CTE selects eligible rows with row-level locks; the UPDATE...FROM
        // flips them to Processing and stamps the worker — all atomic.
        var sql = $$"""
            WITH cte AS (
                SELECT id
                FROM {{_qualifiedTable}}
                WHERE status = 0
                  AND available_at <= {0}
                ORDER BY available_at, created_at
                LIMIT {1}
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {{_qualifiedTable}} m
            SET status = 1,
                claimed_at = {0},
                worker_id = {2}
            FROM cte
            WHERE m.id = cte.id
            RETURNING m.*;
            """;

        var sw = Stopwatch.StartNew();
        var claimed = await context
            .OutboxMessages.FromSqlRaw(sql, now, batchSize, workerId)
            .AsNoTracking()
            .ToListAsync(ct);
        sw.Stop();

        _metrics.RecordClaimDuration(workerId, sw.Elapsed, claimed.Count);

        if (claimed.Count > 0)
        {
            _logger.LogDebug(
                "Worker {WorkerId} claimed {Count} messages in {ElapsedMs:F1}ms",
                workerId,
                claimed.Count,
                sw.Elapsed.TotalMilliseconds
            );
        }

        return claimed;
    }

    public async Task BulkCompleteAsync(
        IReadOnlyCollection<OutboxMutation> mutations,
        CancellationToken ct = default
    )
    {
        if (mutations.Count == 0)
            return;

        var sw = Stopwatch.StartNew();
        await using var context = await _contextFactory.CreateDbContextAsync(ct);
        await using var tx = await context.Database.BeginTransactionAsync(ct);

        // ExecuteUpdateAsync per mutation — keyed on PK so each is a single fast UPDATE.
        // Wrapped in one transaction so the whole batch commits atomically.
        foreach (var m in mutations)
        {
            await context
                .OutboxMessages.Where(x => x.Id == m.Id)
                .ExecuteUpdateAsync(
                    s =>
                        s.SetProperty(x => x.Status, m.Status)
                            .SetProperty(x => x.RetryCount, m.RetryCount)
                            .SetProperty(x => x.AvailableAt, m.AvailableAt)
                            .SetProperty(x => x.ProcessedAt, m.ProcessedAt)
                            .SetProperty(x => x.LastError, m.LastError)
                            .SetProperty(x => x.ClaimedAt, (DateTime?)null)
                            .SetProperty(x => x.WorkerId, (string?)null),
                    ct
                );
        }

        await tx.CommitAsync(ct);
        sw.Stop();

        _metrics.RecordCompleteDuration(sw.Elapsed, mutations.Count);
    }

    public async Task<int> RecoverExpiredLeasesAsync(
        TimeSpan leaseTimeout,
        CancellationToken ct = default
    )
    {
        await using var context = await _contextFactory.CreateDbContextAsync(ct);
        var cutoff = _time.GetUtcNow().UtcDateTime - leaseTimeout;

        var recovered = await context
            .OutboxMessages.Where(m =>
                m.Status == OutboxStatus.Processing && m.ClaimedAt != null && m.ClaimedAt < cutoff
            )
            .ExecuteUpdateAsync(
                s =>
                    s.SetProperty(m => m.Status, OutboxStatus.Pending)
                        .SetProperty(m => m.ClaimedAt, (DateTime?)null)
                        .SetProperty(m => m.WorkerId, (string?)null)
                        .SetProperty(m => m.LastError, "Lease expired; recovered for retry"),
                ct
            );

        if (recovered > 0)
        {
            _logger.LogWarning(
                "Recovered {Count} messages with leases expired before {Cutoff:O}",
                recovered,
                cutoff
            );
        }

        return recovered;
    }

    public async Task<OutboxBacklog> GetBacklogAsync(CancellationToken ct = default)
    {
        await using var context = await _contextFactory.CreateDbContextAsync(ct);

        var counts = await context
            .OutboxMessages.AsNoTracking()
            .GroupBy(m => m.Status)
            .Select(g => new { Status = g.Key, Count = g.Count() })
            .ToListAsync(ct);

        var oldestPending = await context
            .OutboxMessages.AsNoTracking()
            .Where(m => m.Status == OutboxStatus.Pending)
            .OrderBy(m => m.CreatedAt)
            .Select(m => (DateTime?)m.CreatedAt)
            .FirstOrDefaultAsync(ct);

        var pending = counts.FirstOrDefault(c => c.Status == OutboxStatus.Pending)?.Count ?? 0;
        var processing =
            counts.FirstOrDefault(c => c.Status == OutboxStatus.Processing)?.Count ?? 0;
        var failed = counts.FirstOrDefault(c => c.Status == OutboxStatus.Failed)?.Count ?? 0;

        var ageSeconds = oldestPending.HasValue
            ? (int)(_time.GetUtcNow().UtcDateTime - oldestPending.Value).TotalSeconds
            : 0;

        return new OutboxBacklog(pending, processing, failed, ageSeconds);
    }

    public async Task<int> RequeueAsync(Guid messageId, CancellationToken ct = default)
    {
        await using var context = await _contextFactory.CreateDbContextAsync(ct);
        var now = _time.GetUtcNow().UtcDateTime;

        return await context
            .OutboxMessages.Where(m => m.Id == messageId && m.Status == OutboxStatus.Failed)
            .ExecuteUpdateAsync(
                s =>
                    s.SetProperty(m => m.Status, OutboxStatus.Pending)
                        .SetProperty(m => m.RetryCount, 0)
                        .SetProperty(m => m.AvailableAt, now)
                        .SetProperty(m => m.LastError, (string?)null)
                        .SetProperty(m => m.ClaimedAt, (DateTime?)null)
                        .SetProperty(m => m.WorkerId, (string?)null),
                ct
            );
    }

    private static string Quote(string ident) => "\"" + ident.Replace("\"", "\"\"") + "\"";
}
