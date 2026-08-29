using Microsoft.Extensions.Logging;

namespace ConsumerStreams.Application.Logging;

/// <summary>
/// Datos de un evento procesado con éxito, agrupados en un único valor para no exceder el límite
/// de parámetros del método de logging generado (regla S107). <see cref="ToString"/> reproduce
/// exactamente el detalle que antes ocupaba placeholders individuales.
/// </summary>
public readonly record struct ProcessedEventLog(
    string? TransactionId,
    decimal Amount,
    string? RiskLevel,
    int FraudScore,
    double PipelineElapsedMs,
    string TargetTopic,
    string? DispersedKey)
{
    public override string ToString()
        => $"Txn: {TransactionId} | Monto: ${Amount} | Riesgo: {RiskLevel} ({FraudScore} pts) | "
         + $"Pipeline: {PipelineElapsedMs:F2} ms ➔ '{TargetTopic}' [DispersedKey: {DispersedKey}]";
}

/// <summary>
/// Registro estructurado de alto rendimiento (source-generated) para el pipeline de streaming.
/// Cada método genera una ruta fuertemente tipada que evita la asignación del arreglo
/// <c>params object?[]</c> y el boxing de los tipos de valor: requisito para Native AOT y
/// para eliminar la advertencia CA1873.
/// </summary>
internal static partial class PipelineLog
{
    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Iniciando pipeline de streaming reactivo cifrado Protobuf: '{Source}' ➔ '{Target}' | DLQ Error: '{ErrorTopic}'")]
    public static partial void PipelineStarting(ILogger logger, string source, string target, string errorTopic);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "✔ [AES-GCM Decrypted & Processed] {Entry}")]
    public static partial void EventProcessed(ILogger logger, ProcessedEventLog entry);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "❌ Error procesando evento en streaming para key '{Key}'. Redirigiendo a cola DLQ/Error: '{ErrorTopic}'")]
    public static partial void EventProcessingFailed(ILogger logger, Exception exception, string? key, string errorTopic);

    [LoggerMessage(
        Level = LogLevel.Critical,
        Message = "❌ [FATAL DLQ ERROR] Fallo crítico al publicar en la cola de error '{ErrorTopic}'")]
    public static partial void DlqPublishFatal(ILogger logger, Exception exception, string errorTopic);
}
