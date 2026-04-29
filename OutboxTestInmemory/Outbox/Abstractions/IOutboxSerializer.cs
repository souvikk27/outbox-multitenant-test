namespace OutboxTestInmemory.Outbox.Abstractions;

/// <summary>
/// Converts typed payloads to/from the string stored in <see cref="OutboxMessage.Payload"/>.
/// Default impl is JSON; swap it via <c>OutboxBuilder.UseSerializer&lt;T&gt;()</c> for protobuf,
/// MessagePack, or any custom format.
/// </summary>
public interface IOutboxSerializer
{
    string Serialize(object payload, Type payloadType);

    object Deserialize(string payload, Type payloadType);
}
