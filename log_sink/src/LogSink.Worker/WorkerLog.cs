using Microsoft.Extensions.Logging;

namespace LogSink.Worker;

/// <summary>
/// Registro estructurado source-generated para el worker. Consolida el arranque en un único
/// evento (evita la ráfaga de llamadas Information, regla S6664) y elimina la asignación de
/// <c>params object?[]</c> exigida por Native AOT.
/// </summary>
internal static partial class WorkerLog
{
    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "🚀 [Cosmos DB Bulk Sink AOT] Servicio de persistencia masiva iniciado | Origen: '{Source}' " +
                  "| Lote: {BatchSize} docs | Ventana: {TimeoutMs} ms | Endpoint: '{Endpoint}' | BD/Tabla: '{Database}' / '{Container}'")]
    public static partial void BulkSinkStarting(
        ILogger logger, string source, int batchSize, int timeoutMs, string endpoint, string database, string container);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Bulk Sink detenido adecuadamente por solicitud de cancelación.")]
    public static partial void BulkSinkStopped(ILogger logger);
}
