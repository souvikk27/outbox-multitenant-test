namespace OutboxTestInmemory.Outbox.Abstractions;

/// <summary>
/// Decides what to do with an exception thrown by a handler.
/// Apps swap this in to encode domain knowledge (e.g. SQL deadlock = transient,
/// validation failure = permanent, 4xx HTTP = permanent, 5xx HTTP = transient).
/// </summary>
public interface IOutboxErrorClassifier
{
    OutboxErrorClassification Classify(Exception exception, OutboxMessage message, int attempt);
}

public enum OutboxErrorClassification
{
    /// <summary>Schedule a retry with backoff. Counts toward MaxRetries.</summary>
    Transient,

    /// <summary>Skip retries; mark Failed immediately. Use for validation / 4xx / no-handler.</summary>
    Permanent,

    /// <summary>The work was cancelled mid-flight (shutdown). Return to Pending without consuming an attempt.</summary>
    Cancelled,
}
