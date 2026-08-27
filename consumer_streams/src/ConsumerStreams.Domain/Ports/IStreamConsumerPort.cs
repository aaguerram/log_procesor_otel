namespace ConsumerStreams.Domain.Ports;

/// <summary>
/// Puerto de entrada (Inbound Port) para el consumo continuo del flujo de eventos binarios Protobuf en Kafka.
/// </summary>
public interface IStreamConsumerPort
{
    Task StartStreamingAsync(
        string sourceTopic,
        Func<string?, byte[], IDictionary<string, string>?, CancellationToken, Task<bool>> onMessageReceived,
        CancellationToken cancellationToken);
}
