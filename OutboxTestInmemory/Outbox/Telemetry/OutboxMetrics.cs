using System.Diagnostics.Metrics;

namespace OutboxTestInmemory.Outbox.Telemetry;

/// <summary>
/// Owns the library's <see cref="Meter"/> and exposes typed Record* methods so
/// internal callers don't construct tag arrays inline.
/// </summary>
internal sealed class OutboxMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _processed;
    private readonly Counter<long> _failed;
    private readonly Counter<long> _retried;
    private readonly Counter<long> _deadLettered;
    private readonly Counter<long> _claimed;
    private readonly Counter<long> _leaseRecovered;
    private readonly Counter<long> _workerErrors;
    private readonly Histogram<double> _duration;
    private readonly Histogram<double> _queueDelay;
    private readonly Histogram<double> _claimDuration;
    private readonly Histogram<double> _completeDuration;

    public OutboxMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(OutboxDiagnostics.MeterName);

        _processed = _meter.CreateCounter<long>(
            "outbox.messages.processed",
            unit: "{message}",
            description: "Messages processed successfully"
        );

        _failed = _meter.CreateCounter<long>(
            "outbox.messages.failed",
            unit: "{message}",
            description: "Messages that failed during processing"
        );

        _retried = _meter.CreateCounter<long>(
            "outbox.messages.retried",
            unit: "{message}",
            description: "Messages scheduled for retry after a transient failure"
        );

        _deadLettered = _meter.CreateCounter<long>(
            "outbox.messages.dead_lettered",
            unit: "{message}",
            description: "Messages that exhausted retries or hit a permanent failure"
        );

        _claimed = _meter.CreateCounter<long>(
            "outbox.messages.claimed",
            unit: "{message}",
            description: "Messages claimed for processing"
        );

        _leaseRecovered = _meter.CreateCounter<long>(
            "outbox.lease.recovered",
            unit: "{message}",
            description: "Messages recovered from expired worker leases"
        );

        _workerErrors = _meter.CreateCounter<long>(
            "outbox.worker.errors",
            unit: "{error}",
            description: "Errors raised in the worker loop"
        );

        _duration = _meter.CreateHistogram<double>(
            "outbox.message.duration",
            unit: "ms",
            description: "Handler invocation duration"
        );

        _queueDelay = _meter.CreateHistogram<double>(
            "outbox.message.queue_delay",
            unit: "ms",
            description: "Delay between AvailableAt and pickup by the dispatcher (claim queueing + worker fan-out)"
        );

        _claimDuration = _meter.CreateHistogram<double>(
            "outbox.claim.duration",
            unit: "ms",
            description: "Time spent in ClaimBatchAsync (DB-side claim + fetch)"
        );

        _completeDuration = _meter.CreateHistogram<double>(
            "outbox.complete.duration",
            unit: "ms",
            description: "Time spent in BulkCompleteAsync (DB-side bulk update of mutations)"
        );
    }

    public void RecordProcessed(string eventType, string tenantId, TimeSpan elapsed)
    {
        var tags = Tags(eventType, tenantId);
        _processed.Add(1, tags);
        _duration.Record(elapsed.TotalMilliseconds, tags);
    }

    public void RecordFailed(string eventType, string tenantId, bool permanent) =>
        _failed.Add(
            1,
            new KeyValuePair<string, object?>("event_type", eventType),
            new KeyValuePair<string, object?>("tenant_id", tenantId),
            new KeyValuePair<string, object?>("kind", permanent ? "permanent" : "transient")
        );

    public void RecordRetry(string eventType, string tenantId) =>
        _retried.Add(1, Tags(eventType, tenantId));

    public void RecordDeadLettered(string eventType, string tenantId) =>
        _deadLettered.Add(1, Tags(eventType, tenantId));

    public void RecordBatchClaimed(string workerId, int count) =>
        _claimed.Add(count, new KeyValuePair<string, object?>("worker_id", workerId));

    public void RecordLeaseRecovered(int count) => _leaseRecovered.Add(count);

    public void RecordWorkerError(string workerId) =>
        _workerErrors.Add(1, new KeyValuePair<string, object?>("worker_id", workerId));

    public void RecordQueueDelay(string eventType, string tenantId, TimeSpan delay) =>
        _queueDelay.Record(delay.TotalMilliseconds, Tags(eventType, tenantId));

    public void RecordClaimDuration(string workerId, TimeSpan duration, int batchSize) =>
        _claimDuration.Record(
            duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("worker_id", workerId),
            new KeyValuePair<string, object?>("batch_size", batchSize)
        );

    public void RecordCompleteDuration(TimeSpan duration, int batchSize) =>
        _completeDuration.Record(
            duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("batch_size", batchSize)
        );

    private static KeyValuePair<string, object?>[] Tags(string eventType, string tenantId) =>
        new[]
        {
            new KeyValuePair<string, object?>("event_type", eventType),
            new KeyValuePair<string, object?>("tenant_id", tenantId),
        };

    public void Dispose() => _meter.Dispose();
}
