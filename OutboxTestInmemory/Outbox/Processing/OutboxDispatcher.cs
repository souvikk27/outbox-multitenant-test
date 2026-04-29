using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Outbox.Configuration;
using OutboxTestInmemory.Outbox.Telemetry;

namespace OutboxTestInmemory.Outbox.Processing;

/// <summary>
/// Per-message orchestration: resolve handler, apply timeout, invoke, classify
/// failures, and translate the outcome into an <see cref="OutboxMutation"/>.
/// The worker only knows about batches; the dispatcher owns the dispatch contract.
/// </summary>
internal sealed class OutboxDispatcher
{
    private readonly OutboxRegistry _registry;
    private readonly IOutboxErrorClassifier _classifier;
    private readonly IServiceProvider _services;
    private readonly OutboxMetrics _metrics;
    private readonly OutboxOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<OutboxDispatcher> _logger;

    public OutboxDispatcher(
        OutboxRegistry registry,
        IOutboxErrorClassifier classifier,
        IServiceProvider services,
        OutboxMetrics metrics,
        IOptions<OutboxOptions> options,
        TimeProvider time,
        ILogger<OutboxDispatcher> logger
    )
    {
        _registry = registry;
        _classifier = classifier;
        _services = services;
        _metrics = metrics;
        _options = options.Value;
        _time = time;
        _logger = logger;
    }

    public async Task<OutboxMutation> DispatchAsync(OutboxMessage message, CancellationToken ct)
    {
        using var activity = OutboxDiagnostics.ActivitySource.StartActivity(
            "Outbox.Dispatch",
            ActivityKind.Consumer
        );
        activity?.SetTag("outbox.message_id", message.Id);
        activity?.SetTag("outbox.event_type", message.EventType);
        activity?.SetTag("outbox.tenant_id", message.TenantId);
        activity?.SetTag("outbox.retry_count", message.RetryCount);

        // Queue delay = time the message sat past its AvailableAt waiting for a worker.
        // Spikes here mean claim throughput is below ingestion rate; dial up WorkerCount/BatchSize.
        var queueDelay = _time.GetUtcNow().UtcDateTime - message.AvailableAt;
        if (queueDelay > TimeSpan.Zero)
            _metrics.RecordQueueDelay(message.EventType, message.TenantId, queueDelay);

        if (!_registry.TryGet(message.EventType, out var invoker))
        {
            // Unknown event type is permanent — a handler isn't going to appear later.
            _logger.LogError(
                "No handler registered for event type {EventType} (message {MessageId})",
                message.EventType,
                message.Id
            );
            return ToFailed(message, $"No handler registered for event type '{message.EventType}'");
        }

        var startedAt = _time.GetUtcNow();
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(_options.HandlerTimeoutSeconds));

            // Use a scope so handlers can resolve scoped dependencies (e.g. DbContext).
            await using var scope = _services.CreateAsyncScope();
            await invoker.InvokeAsync(scope.ServiceProvider, message, timeoutCts.Token);

            var elapsed = _time.GetUtcNow() - startedAt;
            _metrics.RecordProcessed(message.EventType, message.TenantId, elapsed);
            activity?.SetStatus(ActivityStatusCode.Ok);

            return new OutboxMutation(
                Id: message.Id,
                Status: OutboxStatus.Processed,
                RetryCount: message.RetryCount,
                AvailableAt: message.AvailableAt,
                ProcessedAt: _time.GetUtcNow().UtcDateTime,
                LastError: null
            );
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            return Classify(message, ex);
        }
    }

    private OutboxMutation Classify(OutboxMessage message, Exception ex)
    {
        var attempt = message.RetryCount + 1;
        var classification = _classifier.Classify(ex, message, attempt);

        switch (classification)
        {
            case OutboxErrorClassification.Cancelled:
                _logger.LogDebug(
                    "Message {MessageId} cancelled mid-flight; returning to Pending",
                    message.Id
                );
                return new OutboxMutation(
                    Id: message.Id,
                    Status: OutboxStatus.Pending,
                    RetryCount: message.RetryCount,
                    AvailableAt: _time.GetUtcNow().UtcDateTime,
                    ProcessedAt: null,
                    LastError: "Cancelled during shutdown"
                );

            case OutboxErrorClassification.Permanent:
                _metrics.RecordFailed(message.EventType, message.TenantId, permanent: true);
                _logger.LogError(
                    ex,
                    "Message {MessageId} ({EventType}) permanently failed",
                    message.Id,
                    message.EventType
                );
                return ToFailed(message, ex.Message);

            case OutboxErrorClassification.Transient:
            default:
                _metrics.RecordFailed(message.EventType, message.TenantId, permanent: false);
                if (attempt >= _options.MaxRetries)
                {
                    _logger.LogError(
                        ex,
                        "Message {MessageId} exhausted retries ({Attempt}/{Max})",
                        message.Id,
                        attempt,
                        _options.MaxRetries
                    );
                    return ToFailed(message, $"Exhausted retries: {ex.Message}");
                }
                _logger.LogWarning(
                    ex,
                    "Message {MessageId} transient failure (attempt {Attempt}/{Max})",
                    message.Id,
                    attempt,
                    _options.MaxRetries
                );
                return ToRetry(message, attempt, ex.Message);
        }
    }

    private OutboxMutation ToRetry(OutboxMessage message, int attempt, string error)
    {
        // Exponential backoff with full jitter, capped.
        var capSeconds = Math.Min(Math.Pow(2, attempt), _options.MaxBackoffSeconds);
        var jitterSeconds = Random.Shared.NextDouble() * capSeconds;
        var nextAvailable = _time.GetUtcNow().UtcDateTime.AddSeconds(jitterSeconds);

        _metrics.RecordRetry(message.EventType, message.TenantId);

        return new OutboxMutation(
            Id: message.Id,
            Status: OutboxStatus.Pending,
            RetryCount: attempt,
            AvailableAt: nextAvailable,
            ProcessedAt: null,
            LastError: Truncate(error, 2000)
        );
    }

    private OutboxMutation ToFailed(OutboxMessage message, string error)
    {
        _metrics.RecordDeadLettered(message.EventType, message.TenantId);
        return new OutboxMutation(
            Id: message.Id,
            Status: OutboxStatus.Failed,
            RetryCount: message.RetryCount + 1,
            AvailableAt: message.AvailableAt,
            ProcessedAt: null,
            LastError: Truncate(error, 2000)
        );
    }

    private static string Truncate(string s, int max) => s.Length <= max ? s : s.Substring(0, max);
}
