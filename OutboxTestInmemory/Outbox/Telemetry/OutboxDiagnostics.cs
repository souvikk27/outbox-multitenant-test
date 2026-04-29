using System.Diagnostics;

namespace OutboxTestInmemory.Outbox.Telemetry;

/// <summary>
/// Shared diagnostic primitives. The <see cref="ActivitySource"/> name is the public
/// contract for tracing — apps subscribe to it via OpenTelemetry's <c>AddSource</c>.
/// </summary>
public static class OutboxDiagnostics
{
    public const string ActivitySourceName = "OutboxTestInmemory.Outbox";
    public const string MeterName = "OutboxTestInmemory.Outbox";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
