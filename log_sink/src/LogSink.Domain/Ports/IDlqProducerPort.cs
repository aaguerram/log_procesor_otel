namespace LogSink.Domain.Ports;

/// <summary>
/// Puerto de salida para publicar mensajes fallidos de forma independiente en la cola DLQ de procesamiento.
/// </summary>
public interface IDlqProducerPort
{
    Task<bool> SendToDlqAsync(
        string dlqTopic,
        string partitionKey,
        string rawJson,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default);
}
