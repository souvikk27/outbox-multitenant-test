using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxTestInmemory.Outbox.Configuration;

namespace OutboxTestInmemory.Outbox.Processing;

internal sealed class OutboxHostedService : BackgroundService
{
    private readonly OutboxProcessor _processor;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxHostedService> _logger;
    private readonly string _instanceId;

    public OutboxHostedService(
        OutboxProcessor processor,
        IOptions<OutboxOptions> options,
        ILogger<OutboxHostedService> logger
    )
    {
        _processor = processor;
        _options = options.Value;
        _logger = logger;
        _instanceId = $"{Environment.MachineName}:{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Outbox processing starting with {WorkerCount} workers on {Instance}",
            _options.WorkerCount,
            _instanceId
        );

        var workers = Enumerable
            .Range(0, _options.WorkerCount)
            .Select(i => _processor.RunWorkerAsync($"{_instanceId}/w{i}", stoppingToken))
            .ToArray();

        try
        {
            await Task.WhenAll(workers);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected on graceful shutdown.
        }

        _logger.LogInformation("Outbox processing stopped");
    }
}
