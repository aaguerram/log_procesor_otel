namespace ConsumerStreams.Domain.Ports;

/// <summary>
/// Puerto de salida para publicar el evento procesado (JSON en claro) en el tópico de destino.
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
