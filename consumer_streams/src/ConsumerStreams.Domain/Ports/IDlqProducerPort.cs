using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Ports;

/// <summary>
/// Puerto de salida dedicado a la publicación de mensajes fallidos (poison pill) en el
/// tópico de error / DLQ como sobre binario <see cref="EncryptedErrorPayloadEnvelope"/>.
/// </summary>
public interface IDlqProducerPort
{
    Task<bool> PublishErrorEnvelopeAsync(
        string errorTopic,
        string? key,
        EncryptedErrorPayloadEnvelope errorEnvelope,
        IDictionary<string, string>? headers,
        CancellationToken cancellationToken);
}
