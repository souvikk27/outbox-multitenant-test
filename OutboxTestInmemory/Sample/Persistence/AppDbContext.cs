using Microsoft.EntityFrameworkCore;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Outbox.Persistence;

namespace OutboxTestInmemory.Sample.Persistence;

/// <summary>
/// The consumer's DbContext. In a real app this also holds business entities;
/// the only outbox concession is implementing <see cref="IOutboxDbContext"/> and
/// calling <c>ApplyOutboxConfiguration</c>.
/// </summary>
public class AppDbContext : DbContext, IOutboxDbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options)
        : base(options) { }

    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyOutboxConfiguration();
    }
}
