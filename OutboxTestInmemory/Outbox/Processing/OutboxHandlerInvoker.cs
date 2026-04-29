using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using OutboxTestInmemory.Outbox.Abstractions;

namespace OutboxTestInmemory.Outbox.Processing;

internal sealed class OutboxHandlerInvoker<TPayload, THandler> : IOutboxHandlerInvoker
    where THandler : class, IOutboxHandler<TPayload>
{
    private static readonly IReadOnlyDictionary<string, string> EmptyMetadata =
        new Dictionary<string, string>();

    private readonly IOutboxSerializer _serializer;

    public OutboxHandlerInvoker(IOutboxSerializer serializer)
    {
        _serializer = serializer;
    }

    public async Task InvokeAsync(
        IServiceProvider serviceProvider,
        OutboxMessage message,
        CancellationToken ct
    )
    {
        var payload = (TPayload)_serializer.Deserialize(message.Payload, typeof(TPayload));
        var metadata = ParseMetadata(message.Metadata);

        var ctx = new OutboxHandlerContext<TPayload>(
            MessageId: message.Id,
            TenantId: message.TenantId,
            EventType: message.EventType,
            Payload: payload,
            RetryCount: message.RetryCount,
            CreatedAt: message.CreatedAt,
            Metadata: metadata
        );

        var handler = serviceProvider.GetRequiredService<THandler>();
        await handler.HandleAsync(ctx, ct);
    }

    private static IReadOnlyDictionary<string, string> ParseMetadata(string? raw)
    {
        if (string.IsNullOrEmpty(raw))
            return EmptyMetadata;
        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, string>>(raw)
                ?? (IReadOnlyDictionary<string, string>)EmptyMetadata;
        }
        catch (JsonException)
        {
            return EmptyMetadata;
        }
    }
}
