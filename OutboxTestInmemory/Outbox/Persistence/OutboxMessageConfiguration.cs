using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using OutboxTestInmemory.Outbox.Abstractions;

namespace OutboxTestInmemory.Outbox.Persistence;

internal sealed class OutboxMessageConfiguration : IEntityTypeConfiguration<OutboxMessage>
{
    private readonly string _tableName;
    private readonly string? _schema;

    public OutboxMessageConfiguration(string tableName, string? schema)
    {
        _tableName = tableName;
        _schema = schema;
    }

    public void Configure(EntityTypeBuilder<OutboxMessage> builder)
    {
        builder.ToTable(_tableName, _schema);

        builder.HasKey(m => m.Id);
        builder.Property(m => m.Id).HasColumnName("id");
        builder.Property(m => m.TenantId).HasColumnName("tenant_id").IsRequired().HasMaxLength(128);
        builder
            .Property(m => m.EventType)
            .HasColumnName("event_type")
            .IsRequired()
            .HasMaxLength(128);
        builder.Property(m => m.Payload).HasColumnName("payload").IsRequired();
        builder.Property(m => m.Metadata).HasColumnName("metadata");
        builder.Property(m => m.Status).HasColumnName("status").HasConversion<int>();
        builder.Property(m => m.RetryCount).HasColumnName("retry_count");
        builder.Property(m => m.AvailableAt).HasColumnName("available_at");
        builder.Property(m => m.CreatedAt).HasColumnName("created_at");
        builder.Property(m => m.ProcessedAt).HasColumnName("processed_at");
        builder.Property(m => m.ClaimedAt).HasColumnName("claimed_at");
        builder.Property(m => m.WorkerId).HasColumnName("worker_id").HasMaxLength(64);
        builder.Property(m => m.LastError).HasColumnName("last_error");

        // Partial indexes keep the hot path tiny: only Pending rows are scanned during claim,
        // only Processing rows are scanned during lease recovery.
        builder
            .HasIndex(m => new { m.Status, m.AvailableAt })
            .HasDatabaseName("ix_outbox_messages_pending_due")
            .HasFilter("status = 0");

        builder
            .HasIndex(m => new { m.Status, m.ClaimedAt })
            .HasDatabaseName("ix_outbox_messages_processing_claimed")
            .HasFilter("status = 1");

        builder.HasIndex(m => m.TenantId).HasDatabaseName("ix_outbox_messages_tenant_id");
    }
}

public static class OutboxModelBuilderExtensions
{
    /// <summary>
    /// Wire the outbox table into your DbContext. Call from <c>OnModelCreating</c>.
    /// Pass a custom table name / schema if you have project-wide naming conventions.
    /// </summary>
    public static ModelBuilder ApplyOutboxConfiguration(
        this ModelBuilder modelBuilder,
        string tableName = "outbox_messages",
        string? schema = null
    )
    {
        modelBuilder.ApplyConfiguration(new OutboxMessageConfiguration(tableName, schema));
        return modelBuilder;
    }
}
