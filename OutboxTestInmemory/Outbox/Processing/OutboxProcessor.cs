using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Outbox.Configuration;
using OutboxTestInmemory.Outbox.Telemetry;

namespace OutboxTestInmemory.Outbox.Processing;

/// <summary>
/// Worker loop: claim → dispatch fanout per tenant → bulk-complete. The hosted
/// service spawns N concurrent <see cref="RunWorkerAsync"/> calls, each with
/// a stable workerId for lease attribution.
/// </summary>
internal sealed class OutboxProcessor
{
    private readonly IOutboxStore _store;
    private readonly OutboxDispatcher _dispatcher;
    private readonly OutboxMetrics _metrics;
    private readonly OutboxOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<OutboxProcessor> _logger;

    public OutboxProcessor(
        IOutboxStore store,
        OutboxDispatcher dispatcher,
        OutboxMetrics metrics,
        IOptions<OutboxOptions> options,
        TimeProvider time,
        ILogger<OutboxProcessor> logger
    )
    {
        _store = store;
        _dispatcher = dispatcher;
        _metrics = metrics;
        _options = options.Value;
        _time = time;
        _logger = logger;
    }

    public async Task RunWorkerAsync(string workerId, CancellationToken ct)
    {
        _logger.LogInformation("Worker {WorkerId} started", workerId);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var claimed = await _store.ClaimBatchAsync(workerId, _options.BatchSize, ct);

                if (claimed.Count == 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(_options.IdleBackoffMs), _time, ct);
                    continue;
                }

                _metrics.RecordBatchClaimed(workerId, claimed.Count);
                await ProcessBatchAsync(claimed, ct);
                await Task.Delay(TimeSpan.FromMilliseconds(_options.PollIntervalMs), _time, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _metrics.RecordWorkerError(workerId);
                _logger.LogError(ex, "Worker {WorkerId} loop error; backing off", workerId);
                await SafeDelay(TimeSpan.FromMilliseconds(_options.IdleBackoffMs), ct);
            }
        }

        _logger.LogInformation("Worker {WorkerId} stopped", workerId);
    }

    private async Task ProcessBatchAsync(IReadOnlyList<OutboxMessage> batch, CancellationToken ct)
    {
        var byTenant = batch.GroupBy(m => m.TenantId).ToList();
        var mutations = new ConcurrentBag<OutboxMutation>();

        // Per-tenant FIFO with bounded cross-tenant parallelism gives fair scheduling
        // without letting one slow tenant monopolize a worker.
        await Parallel.ForEachAsync(
            byTenant,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = _options.MaxTenantConcurrency,
                CancellationToken = ct,
            },
            async (partition, token) =>
            {
                foreach (var message in partition)
                {
                    var mutation = await _dispatcher.DispatchAsync(message, token);
                    mutations.Add(mutation);
                }
            }
        );

        // Persist outcomes even if cancellation has been requested — leaving messages
        // stuck in Processing is worse than one extra DB roundtrip on shutdown.
        await _store.BulkCompleteAsync(mutations.ToArray(), CancellationToken.None);
    }

    private async Task SafeDelay(TimeSpan delay, CancellationToken ct)
    {
        try
        {
            await Task.Delay(delay, _time, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }
}
