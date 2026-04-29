using System.ComponentModel.DataAnnotations;

namespace OutboxTestInmemory.Outbox.Configuration;

public class OutboxOptions
{
    public const string SectionName = "Outbox";

    /// <summary>Physical table name. Defaults to "outbox_messages".</summary>
    [Required]
    public string TableName { get; set; } = "outbox_messages";

    /// <summary>Optional schema for the outbox table.</summary>
    public string? Schema { get; set; }

    [Range(1, 64)]
    public int WorkerCount { get; set; } = 4;

    [Range(1, 1000)]
    public int BatchSize { get; set; } = 100;

    [Range(1, 64)]
    public int MaxTenantConcurrency { get; set; } = 8;

    [Range(1, 100)]
    public int MaxRetries { get; set; } = 5;

    [Range(100, 60_000)]
    public int PollIntervalMs { get; set; } = 500;

    [Range(100, 60_000)]
    public int IdleBackoffMs { get; set; } = 2_000;

    [Range(1, 600)]
    public int HandlerTimeoutSeconds { get; set; } = 5;

    [Range(10, 3_600)]
    public int LeaseTimeoutSeconds { get; set; } = 120;

    [Range(10, 3_600)]
    public int LeaseRecoveryIntervalSeconds { get; set; } = 30;

    [Range(0, 1_000_000)]
    public int BacklogWarningThreshold { get; set; } = 10_000;

    [Range(0, 10_000_000)]
    public int BacklogUnhealthyThreshold { get; set; } = 100_000;

    [Range(1, 600)]
    public int MaxBackoffSeconds { get; set; } = 300;
}
