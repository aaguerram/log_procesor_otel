namespace LogSink.Domain.Models;

/// <summary>
/// Resultado agregado de un lote de inserción masiva (Bulk).
/// </summary>
public record BulkSinkResult(
    int TotalProcessed,
    int TotalSuccessful,
    int TotalFailed,
    int TotalDlqSent,
    double ElapsedMilliseconds,
    double RequestUnitsConsumed = 0.0);
