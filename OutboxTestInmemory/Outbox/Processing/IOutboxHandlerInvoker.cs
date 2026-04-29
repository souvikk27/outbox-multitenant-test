using OutboxTestInmemory.Outbox.Abstractions;

namespace OutboxTestInmemory.Outbox.Processing;

/// <summary>
/// Bridges the runtime <see cref="OutboxMessage"/> to a strongly-typed handler call.
/// One concrete invoker is created per registered <c>(TPayload, THandler)</c> pair,
/// so the dispatch loop avoids per-call reflection.
/// </summary>
internal interface IOutboxHandlerInvoker
{
    Task InvokeAsync(IServiceProvider serviceProvider, OutboxMessage message, CancellationToken ct);
}
