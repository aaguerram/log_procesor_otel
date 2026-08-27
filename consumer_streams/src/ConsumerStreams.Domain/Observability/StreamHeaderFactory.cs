using System.Globalization;
using ConsumerStreams.Domain.Models;

namespace ConsumerStreams.Domain.Observability;

/// <summary>
/// Construye los diccionarios de cabeceras Kafka para los eventos procesados y para la ruta de error/DLQ,
/// partiendo de las cabeceras entrantes. Extraído del caso de uso para poder verificarlo en aislamiento.
/// </summary>
public static class StreamHeaderFactory
{
    public static Dictionary<string, string> ForProcessedEvent(
        IReadOnlyDictionary<string, string>? inboundHeaders,
        string vaultTokenId,
        string serviceName,
        string telemetryLabel,
        string targetCollection,
        ProcessedTransactionEvent processedEvent)
    {
        var headers = Copy(inboundHeaders);
        headers[StreamHeaders.StreamProcessor] = StreamHeaders.StreamProcessorValue;
        headers[StreamHeaders.DecryptionAlgorithm] = StreamHeaders.DecryptionAlgorithmValue;
        headers[StreamHeaders.VaultToken] = string.IsNullOrEmpty(vaultTokenId) ? "NONE" : vaultTokenId;
        headers[StreamHeaders.ServiceName] = serviceName;
        headers[StreamHeaders.TelemetryType] = telemetryLabel;
        headers[StreamHeaders.TargetCollection] = targetCollection;
        headers[StreamHeaders.ProcessedStatus] = processedEvent.ProcessedStatus ?? "UNKNOWN";
        headers[StreamHeaders.RiskLevel] = processedEvent.RiskLevel ?? "LOW";
        headers[StreamHeaders.LatencyMs] = processedEvent.ProcessingLatencyMs.ToString("F2", CultureInfo.InvariantCulture);
        return headers;
    }

    public static Dictionary<string, string> ForError(
        IReadOnlyDictionary<string, string>? inboundHeaders,
        Exception failure,
        string sourceTopic,
        DateTimeOffset occurredAt)
    {
        var headers = Copy(inboundHeaders);
        headers[StreamHeaders.ErrorType] = failure.GetType().Name;
        headers[StreamHeaders.ErrorMessage] = failure.Message;
        headers[StreamHeaders.ErrorTimestamp] = occurredAt.ToString("O");
        headers[StreamHeaders.SourceTopic] = sourceTopic;
        headers[StreamHeaders.ErrorHandler] = StreamHeaders.ErrorHandlerValue;
        return headers;
    }

    private static Dictionary<string, string> Copy(IReadOnlyDictionary<string, string>? source)
        => source is null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : new Dictionary<string, string>(source, StringComparer.Ordinal);
}
