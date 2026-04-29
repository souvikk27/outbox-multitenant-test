using System.Text.Json;
using OutboxTestInmemory.Outbox.Abstractions;

namespace OutboxTestInmemory.Outbox.Processing;

/// <summary>
/// Default <see cref="IOutboxSerializer"/>. JSON via <see cref="JsonSerializer"/>
/// with web defaults (camelCase, case-insensitive deserialization).
/// </summary>
internal sealed class JsonOutboxSerializer : IOutboxSerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public string Serialize(object payload, Type payloadType) =>
        JsonSerializer.Serialize(payload, payloadType, Options);

    public object Deserialize(string payload, Type payloadType) =>
        JsonSerializer.Deserialize(payload, payloadType, Options)
        ?? throw new InvalidOperationException(
            $"Payload deserialized to null for type '{payloadType.FullName}'."
        );
}
