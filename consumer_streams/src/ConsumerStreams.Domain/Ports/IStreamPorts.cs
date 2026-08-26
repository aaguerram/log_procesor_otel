using ConsumerStreams.Domain.Models;

namespace ConsumerStreams.Domain.Ports;

/// <summary>
/// Puerto de entrada (Inbound Port) para la suscripción continua al flujo de eventos binarios Protobuf en Kafka.
/// </summary>
public interface IStreamConsumerPort
{
    Task StartStreamingAsync(
        string sourceTopic,
        Func<string?, byte[], IDictionary<string, string>?, CancellationToken, Task<bool>> onMessageReceived,
        CancellationToken cancellationToken);
}

/// <summary>
/// Puerto de salida (Outbound Port) para la publicación en el tópico de destino de Kafka.
/// </summary>
public interface IStreamProducerPort
{
    Task<bool> ForwardEventAsync(
        string targetTopic,
        string? key,
        string jsonPayload,
        IDictionary<string, string>? headers,
        CancellationToken cancellationToken);
}

/// <summary>
/// Puerto de Dominio para transformación, validación y enriquecimiento del evento transaccional.
/// </summary>
public interface ITransactionTransformerPort
{
    ProcessedTransactionEvent TransformAndEnrich(RawTransactionEvent rawEvent);
}
