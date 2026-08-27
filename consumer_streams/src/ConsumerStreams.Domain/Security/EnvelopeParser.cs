using Google.Protobuf;
using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Security;

/// <summary>
/// Intenta interpretar los bytes crudos de Kafka como un <see cref="EncryptedPayloadEnvelope"/>
/// Protobuf. Si no lo son (mensaje JSON/UTF-8 legacy), devuelve <c>false</c> sin lanzar.
/// </summary>
public static class EnvelopeParser
{
    public static bool TryParse(ReadOnlySpan<byte> rawBytes, out EncryptedPayloadEnvelope envelope)
    {
        try
        {
            envelope = EncryptedPayloadEnvelope.Parser.ParseFrom(rawBytes);
            return true;
        }
        catch (InvalidProtocolBufferException)
        {
            envelope = null!;
            return false;
        }
    }
}
