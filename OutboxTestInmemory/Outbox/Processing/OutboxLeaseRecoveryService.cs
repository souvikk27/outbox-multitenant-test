using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Outbox.Configuration;
using OutboxTestInmemory.Outbox.Telemetry;

namespace OutboxTestInmemory.Outbox.Processing;

/// <summary>
/// Independent sweep that releases messages stuck in <c>Processing</c> past the lease
/// timeout — the price of a worker crashing mid-batch is one extra dispatch attempt,
/// not a stuck message.
/// </summary>
internal sealed class OutboxLeaseRecoveryService : BackgroundService
{
    private readonly IOutboxStore _store;
    private readonly OutboxMetrics _metrics;
    private readonly OutboxOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<OutboxLeaseRecoveryService> _logger;

    public OutboxLeaseRecoveryService(
        IOutboxStore store,
        OutboxMetrics metrics,
        IOptions<OutboxOptions> options,
        TimeProvider time,
        ILogger<OutboxLeaseRecoveryService> logger
    )
    {
        _store = store;
        _metrics = metrics;
        _options = options.Value;
        _time = time;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var leaseTimeout = TimeSpan.FromSeconds(_options.LeaseTimeoutSeconds);
        var interval = TimeSpan.FromSeconds(_options.LeaseRecoveryIntervalSeconds);

        _logger.LogInformation(
            "Lease recovery started (timeout {Timeout}, interval {Interval})",
            leaseTimeout,
            interval
        );

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var recovered = await _store.RecoverExpiredLeasesAsync(leaseTimeout, stoppingToken);
                if (recovered > 0)
                    _metrics.RecordLeaseRecovered(recovered);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lease recovery sweep failed");
            }

            try
            {
                await Task.Delay(interval, _time, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }
}
