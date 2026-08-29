using Microsoft.Extensions.Logging;

namespace ConsumerStreams.Worker;

/// <summary>
/// Registro estructurado source-generated para el servicio en segundo plano. Consolida el arranque
/// en un único evento (evita la ráfaga de llamadas Information) y elimina la asignación de
/// <c>params object?[]</c> exigida por Native AOT.
/// </summary>
internal static partial class WorkerLog
{
    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "🚀 [Kafka Streaming AOT] Procesador de flujo iniciado | Origen: '{Source}' ➔ Destino: '{Target}' " +
                  "| DLQ/Error: '{ErrorTopic}' | Consumer Group: '{Group}' | Bootstrap: '{Servers}'")]
    public static partial void StreamingProcessorStarted(
        ILogger logger, string source, string target, string errorTopic, string group, string servers);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Error en el ciclo de streaming. Reintentando reconexión en 5 segundos...")]
    public static partial void StreamingCycleError(ILogger logger, Exception exception);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Pipeline de streaming detenido de manera controlada.")]
    public static partial void PipelineStopped(ILogger logger);
}
