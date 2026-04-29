using OutboxTestInmemory.Outbox.Abstractions;

namespace OutboxTestInmemory.Sample.Email;

public sealed class EmailHandler : IOutboxHandler<EmailPayload>
{
    private static readonly Random Rnd = new();
    private readonly ILogger<EmailHandler> _logger;

    public EmailHandler(ILogger<EmailHandler> logger)
    {
        _logger = logger;
    }

    public async Task HandleAsync(OutboxHandlerContext<EmailPayload> ctx, CancellationToken ct)
    {
        var delay = SimulatedLatency();

        _logger.LogInformation(
            "Sending email #{Index} to {To} for tenant {TenantId} (latency {DelayMs}ms, attempt {Attempt})",
            ctx.Payload.Index, ctx.Payload.To, ctx.TenantId, delay, ctx.RetryCount + 1);

        await Task.Delay(delay, ct);

        var roll = Rnd.NextDouble();
        if (roll < 0.05)
        {
            // Permanent: malformed address, etc. Default classifier treats InvalidOperation as permanent.
            throw new InvalidOperationException("Simulated permanent failure (e.g. invalid address)");
        }
        if (roll < 0.30)
        {
            // Transient: SMTP timeout. Default classifier treats TimeoutException as transient.
            throw new TimeoutException("Simulated transient SMTP timeout");
        }
    }

    private static int SimulatedLatency()
    {
        var roll = Rnd.NextDouble();
        return roll switch
        {
            < 0.6 => Rnd.Next(10, 50),
            < 0.85 => Rnd.Next(50, 200),
            < 0.97 => Rnd.Next(500, 2000),
            _ => Rnd.Next(3000, 5000),
        };
    }
}
