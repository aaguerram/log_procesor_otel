using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace LogSink.Infrastructure.Logging;

/// <summary>
/// Registro estructurado source-generated para los adaptadores de infraestructura
/// (Kafka batch consumer / DLQ producer / Cosmos DB / Key Vault / resiliencia).
/// Cada método genera una ruta fuertemente tipada que evita la asignación de
/// <c>params object?[]</c> y el boxing de enums y tipos de valor (advertencia CA1873),
/// requisito para Native AOT.
/// </summary>
internal static partial class InfrastructureLog
{
    // ---- Batch consumer de Kafka -------------------------------------------------
    [LoggerMessage(Level = LogLevel.Error, Message = "Error en Kafka Batch Consumer [{Code}]: {Reason}")]
    public static partial void BatchConsumerError(ILogger logger, ErrorCode code, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "✔ Particiones asignadas a Batch Consumer: [{Partitions}]")]
    public static partial void PartitionsAssigned(ILogger logger, string partitions);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Batch Consumer Adapter inicializado para grupo '{Group}' en servidores: {Servers}")]
    public static partial void BatchConsumerInitialized(ILogger logger, string group, string servers);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Suscrito exitosamente a 30 particiones del tópico: '{Topic}' (Lote Máx: {BatchSize}, Ventana: {WaitMs} ms)")]
    public static partial void BatchConsumerSubscribed(ILogger logger, string topic, int batchSize, double waitMs);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Advertencia al hacer commit de offsets en Kafka")]
    public static partial void OffsetCommitWarning(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error consumiendo de Kafka: {Reason}")]
    public static partial void BatchConsumeError(ILogger logger, Exception exception, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error inesperado en Batch Consumer Loop")]
    public static partial void BatchLoopUnexpected(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "Batch Consumer detenido correctamente.")]
    public static partial void BatchConsumerStopped(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error cerrando el batch consumer de Kafka")]
    public static partial void BatchConsumerCloseFailed(ILogger logger, Exception exception);

    // ---- Producer DLQ ----------------------------------------------------------
    [LoggerMessage(Level = LogLevel.Error, Message = "DLQ Producer Kafka Error [{Code}]: {Reason}")]
    public static partial void DlqProducerError(ILogger logger, ErrorCode code, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "DLQ Producer Adapter inicializado para bootstrap servers: {Servers}")]
    public static partial void DlqProducerInitialized(ILogger logger, string servers);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "⚠️ [DLQ ITEM PRODUCED] Mensaje fallido enviado de forma independiente a la DLQ '{Topic}' " +
                  "[Partición {Partition}, Offset {Offset}] | Key: {Key}")]
    public static partial void DlqItemProduced(ILogger logger, string topic, int partition, long offset, string key);

    [LoggerMessage(Level = LogLevel.Error, Message = "❌ [FATAL DLQ ERROR] Fallo al publicar mensaje en la cola DLQ '{Topic}': {Reason}")]
    public static partial void DlqPublishFailed(ILogger logger, Exception exception, string topic, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error cerrando el DLQ producer de Kafka")]
    public static partial void DlqProducerCloseFailed(ILogger logger, Exception exception);

    // ---- Azure Key Vault -----------------------------------------------------
    [LoggerMessage(Level = LogLevel.Information,
        Message = "🌐 [Cache Miss / TTL Expirado] Descargando credenciales de Cosmos DB de Azure Key Vault para Token '{Token}'...")]
    public static partial void VaultCredentialsResolving(ILogger logger, string token);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "✔ Credenciales de Cosmos DB almacenadas en RAM con TTL de 1 hora (válidas hasta {Expires}) para Token '{Token}'")]
    public static partial void VaultCredentialsCached(ILogger logger, DateTimeOffset expires, string token);

    // ---- Cosmos DB bulk sink ----------------------------------------------
    [LoggerMessage(Level = LogLevel.Error,
        Message = "❌ Fallo en inserción para PartitionKey '{Key}' hacia colección '{Collection}'. Redirigiendo a DLQ...")]
    public static partial void CosmosInsertFailed(ILogger logger, Exception exception, string key, string? collection);

    [LoggerMessage(Level = LogLevel.Critical, Message = "❌ [FATAL DLQ ERROR] Error al enviar documento a la DLQ '{Topic}'")]
    public static partial void CosmosDlqFatal(ILogger logger, Exception exception, string topic);

    // ---- Pipeline de resiliencia (Polly) --------------------------------
    [LoggerMessage(Level = LogLevel.Warning,
        Message = "⚠️ [RETRY #{Attempt}] Reintentando inserción en Cosmos DB tras fallo transitorio. Espera: {DelaySeconds}s. Causa: {Error}")]
    public static partial void ResilienceRetry(ILogger logger, int attempt, double delaySeconds, string error);

    [LoggerMessage(Level = LogLevel.Critical,
        Message = "🔴 [CIRCUIT BREAKER OPEN] El circuito hacia Azure Cosmos DB se ha ABIERTO por {BreakDurationSeconds}s. " +
                  "Los mensajes serán derivados directamente a DLQ.")]
    public static partial void CircuitBreakerOpened(ILogger logger, double breakDurationSeconds);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "🟢 [CIRCUIT BREAKER CLOSED] El circuito hacia Azure Cosmos DB se ha RESTABLECIDO. Inserciones normales reanudadas.")]
    public static partial void CircuitBreakerClosed(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "🟡 [CIRCUIT BREAKER HALF-OPEN] Evaluando recuperación de Cosmos DB...")]
    public static partial void CircuitBreakerHalfOpened(ILogger logger);
}
