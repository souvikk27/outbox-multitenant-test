using OutboxTestInmemory.Outbox.Abstractions;

namespace OutboxTestInmemory.Outbox.Processing;

/// <summary>
/// Conservative defaults: timeouts and IO/HTTP problems retry, argument/serialization
/// failures don't, OperationCanceled during shutdown returns to Pending.
/// Apps with domain knowledge should replace this via <c>OutboxBuilder.UseErrorClassifier&lt;T&gt;</c>.
/// </summary>
internal sealed class DefaultOutboxErrorClassifier : IOutboxErrorClassifier
{
    public OutboxErrorClassification Classify(
        Exception exception,
        OutboxMessage message,
        int attempt
    ) =>
        exception switch
        {
            OperationCanceledException => OutboxErrorClassification.Cancelled,

            ArgumentException => OutboxErrorClassification.Permanent,
            FormatException => OutboxErrorClassification.Permanent,
            InvalidOperationException => OutboxErrorClassification.Permanent,
            NotSupportedException => OutboxErrorClassification.Permanent,
            System.Text.Json.JsonException => OutboxErrorClassification.Permanent,

            TimeoutException => OutboxErrorClassification.Transient,
            HttpRequestException => OutboxErrorClassification.Transient,
            IOException => OutboxErrorClassification.Transient,

            _ => OutboxErrorClassification.Transient,
        };
}
