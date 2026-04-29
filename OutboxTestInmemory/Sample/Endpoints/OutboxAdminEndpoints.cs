using Microsoft.EntityFrameworkCore;
using OutboxTestInmemory.Outbox.Abstractions;
using OutboxTestInmemory.Sample.Email;
using OutboxTestInmemory.Sample.Persistence;

namespace OutboxTestInmemory.Sample.Endpoints;

public static class OutboxAdminEndpoints
{
    private static readonly string[] Tenants = ["tenant-a", "tenant-b", "tenant-c"];

    public static IEndpointRouteBuilder MapOutboxAdmin(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/outbox");

        group.MapPost("/seed", SeedAsync);
        group.MapGet("/status", StatusAsync);
        group.MapPost("/dlq/requeue/{id:guid}", RequeueAsync);

        return app;
    }

    private static async Task<IResult> SeedAsync(
        int? count,
        IDbContextFactory<AppDbContext> factory,
        IOutboxPublisher publisher,
        CancellationToken ct
    )
    {
        var n = count ?? 100;
        if (n is < 1 or > 100_000)
            return Results.BadRequest(new { Error = "count must be 1..100000" });

        var rnd = Random.Shared;
        await using var db = await factory.CreateDbContextAsync(ct);

        for (var i = 0; i < n; i++)
        {
            var tenant = Tenants[rnd.Next(Tenants.Length)];
            publisher.Enqueue(
                db,
                new EmailPayload(
                    To: $"user{i}@example.com",
                    Subject: "Hello",
                    Body: "This is a test",
                    Index: i),
                new OutboxPublishOptions { TenantId = tenant }
            );
        }

        await db.SaveChangesAsync(ct);
        return Results.Ok(new { Seeded = n });
    }

    private static async Task<IResult> StatusAsync(IOutboxStore store, CancellationToken ct)
    {
        var backlog = await store.GetBacklogAsync(ct);
        return Results.Ok(backlog);
    }

    private static async Task<IResult> RequeueAsync(Guid id, IOutboxStore store, CancellationToken ct)
    {
        var n = await store.RequeueAsync(id, ct);
        return n == 0
            ? Results.NotFound(new { Error = "No failed message with that id" })
            : Results.Ok(new { Requeued = n });
    }
}
