using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace ConsumerStreams.Infrastructure.Logging;

/// <summary>
/// Registro estructurado source-generated para los adaptadores de infraestructura (Kafka / Vault / caché).
/// Cada método emite una ruta fuertemente tipada que evita la asignación del arreglo
/// <c>params object?[]</c> y el boxing de enums y tipos de valor (advertencia CA1873), en línea
/// con las restricciones de compilación Native AOT.
/// </summary>
internal static partial class InfrastructureLog
{
    // ---- Kafka: manejadores de error de los clientes --------------------------------
    [LoggerMessage(Level = LogLevel.Error, Message = "Source Consumer Kafka Error [{Code}]: {Reason}")]
    public static partial void SourceConsumerError(ILogger logger, ErrorCode code, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Sink Producer Kafka Error [{Code}]: {Reason}")]
    public static partial void SinkProducerError(ILogger logger, ErrorCode code, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "DLQ Producer Kafka Error [{Code}]: {Reason}")]
    public static partial void DlqProducerError(ILogger logger, ErrorCode code, string reason);

    // ---- Consumer de origen --------------------------------------------------------
    [LoggerMessage(Level = LogLevel.Information,
        Message = "Source Consumer Adapter (Protobuf Binary) inicializado para grupo '{Group}' en servidores: {Servers}")]
    public static partial void SourceConsumerInitialized(ILogger logger, string group, string servers);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Suscrito exitosamente al flujo binario del tópico de origen: '{Topic}'")]
    public static partial void SourceConsumerSubscribed(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error consumiendo evento de Kafka: {Reason}")]
    public static partial void SourceConsumeException(ILogger logger, Exception exception, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Excepción inesperada en el ciclo de consumo del stream.")]
    public static partial void SourceConsumeUnexpected(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "Ciclo de consumo de stream finalizado para '{Topic}'")]
    public static partial void SourceConsumerFinished(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error cerrando el source consumer de Kafka")]
    public static partial void SourceConsumerCloseFailed(ILogger logger, Exception exception);

    // ---- Producer de destino -----------------------------------------------------
    [LoggerMessage(Level = LogLevel.Information, Message = "Sink Producer Adapter inicializado para bootstrap servers: {Servers}")]
    public static partial void SinkProducerInitialized(ILogger logger, string servers);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Evento reenviado a '{Topic}' [Partición {Partition}, Offset {Offset}]")]
    public static partial void EventForwarded(ILogger logger, string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Error, Message = "Fallo al reenviar evento procesado a '{Topic}': {Reason}")]
    public static partial void EventForwardFailed(ILogger logger, Exception exception, string topic, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error cerrando el sink producer de Kafka")]
    public static partial void SinkProducerCloseFailed(ILogger logger, Exception exception);

    // ---- Producer DLQ ---------------------------------------------------------
    [LoggerMessage(Level = LogLevel.Information, Message = "DLQ Producer Adapter inicializado para bootstrap servers: {Servers}")]
    public static partial void DlqProducerInitialized(ILogger logger, string servers);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "⚠️ [DLQ/ERROR PRODUCED] Sobre Protobuf con error publicado en '{Topic}' [Partición {Partition}, Offset {Offset}]")]
    public static partial void DlqEnvelopePublished(ILogger logger, string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Error, Message = "Fallo al publicar sobre Protobuf en cola de error '{Topic}': {Reason}")]
    public static partial void DlqPublishFailed(ILogger logger, Exception exception, string topic, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error cerrando el DLQ producer de Kafka")]
    public static partial void DlqProducerCloseFailed(ILogger logger, Exception exception);

    // ---- Azure Key Vault ----------------------------------------------------
    [LoggerMessage(Level = LogLevel.Information,
        Message = "🌐 [Cache Miss / TTL Expirado] Resolviendo clave de Azure Key Vault para Token '{Token}' [Thumbprint: {Thumbprint}]...")]
    public static partial void VaultKeyResolving(ILogger logger, string token, string thumbprint);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "✔ Clave de Key Vault almacenada en RAM con TTL de 1 hora (válida hasta {Expires}) para Token '{Token}'")]
    public static partial void VaultKeyCached(ILogger logger, DateTimeOffset expires, string token);

    // ---- Caché de contratos OpenAPI --------------------------------------
    [LoggerMessage(Level = LogLevel.Information,
        Message = "[CONTRACT CACHE] Compilando nuevo contrato Swagger (Fingerprint: {Key})")]
    public static partial void ContractCompiling(ILogger logger, string key);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "[CONTRACT EVICTION] Contrato inactivo >10 min desalojado: {Contract} v{Version}.")]
    public static partial void ContractEvicted(ILogger logger, string contract, string version);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "[CONTRACT CACHE] Limpieza completada. Desalojados: {Evicted}, Activos: {Active}")]
    public static partial void ContractCacheCleanup(ILogger logger, int evicted, int active);
}
