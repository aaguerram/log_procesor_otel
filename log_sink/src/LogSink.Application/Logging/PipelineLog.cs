using Microsoft.Extensions.Logging;

namespace LogSink.Application.Logging;

/// <summary>
/// Registro estructurado source-generated para el caso de uso de bulk sink. Evita la asignación
/// del arreglo <c>params object?[]</c> y el boxing de tipos de valor (advertencia CA1873),
/// requisito para Native AOT.
/// </summary>
internal static partial class PipelineLog
{
    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Iniciando Bulk Sink Pipeline en .NET 10 Native AOT | Tópico: '{Topic}' | Lote Máx: {BatchSize} docs | Ventana: {WaitMs} ms")]
    public static partial void PipelineStarting(ILogger logger, string topic, int batchSize, double waitMs);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "💾 [Bulk Sink Cosmos DB] Persistidos: {Success}/{Total} docs | DLQ: {Dlq} docs | RUs: {RequestUnits:F1} " +
                  "| Latencia Bulk: {BulkLatencyMs:F2} ms (Pipeline: {PipelineMs:F2} ms)")]
    public static partial void BatchPersisted(
        ILogger logger, int success, int total, int dlq, double requestUnits, double bulkLatencyMs, double pipelineMs);
}
