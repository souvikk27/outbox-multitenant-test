using Microsoft.EntityFrameworkCore;

namespace OutboxTestInmemory.Outbox.Abstractions;

/// <summary>
/// Marker contract for application DbContexts that host the outbox table.
/// Apps own the DbContext lifecycle; the library co-locates its messages
/// inside it so producers can enqueue inside their existing business transaction.
/// </summary>
public interface IOutboxDbContext
{
    DbSet<OutboxMessage> OutboxMessages { get; }
}
