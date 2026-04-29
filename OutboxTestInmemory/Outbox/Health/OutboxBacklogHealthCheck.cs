using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Outbox.Configuration;

namespace OutboxTestInmemory.Outbox.Health;

internal sealed class OutboxBacklogHealthCheck : IHealthCheck
{
    private readonly IOutboxStore _store;
    private readonly OutboxOptions _options;

    public OutboxBacklogHealthCheck(IOutboxStore store, IOptions<OutboxOptions> options)
    {
        _store = store;
        _options = options.Value;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var backlog = await _store.GetBacklogAsync(cancellationToken);
            var data = new Dictionary<string, object>
            {
                ["pending"] = backlog.Pending,
                ["processing"] = backlog.Processing,
                ["failed"] = backlog.Failed,
                ["oldest_pending_age_seconds"] = backlog.OldestPendingAgeSeconds,
            };

            if (backlog.Pending >= _options.BacklogUnhealthyThreshold)
            {
                return HealthCheckResult.Unhealthy(
                    $"Pending backlog {backlog.Pending} exceeds {_options.BacklogUnhealthyThreshold}",
                    data: data
                );
            }

            if (backlog.Pending >= _options.BacklogWarningThreshold)
            {
                return HealthCheckResult.Degraded(
                    $"Pending backlog {backlog.Pending} exceeds {_options.BacklogWarningThreshold}",
                    data: data
                );
            }

            return HealthCheckResult.Healthy("Outbox backlog within thresholds", data);
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Unable to read outbox backlog", ex);
        }
    }
}
