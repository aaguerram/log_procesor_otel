using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Observability;

/// <summary>
/// Traduce el enum Protobuf <see cref="TelemetryType"/> al literal canónico usado en cabeceras
/// Kafka y nombres de colección (<c>Trace</c> / <c>Metric</c> / <c>Log</c>).
/// </summary>
public static class TelemetryTypeMapper
{
    public const string DefaultLabel = "Trace";

    public static string ToLabel(TelemetryType telemetryType) => telemetryType switch
    {
        TelemetryType.Trace => "Trace",
        TelemetryType.Metric => "Metric",
        TelemetryType.Log => "Log",
        _ => DefaultLabel
    };
}
