using ConsumerStreams.Domain.Security;
using ConsumerStreams.Domain.Tests.TestSupport;
using Google.Protobuf;

namespace ConsumerStreams.Domain.Tests.Security;

public class EnvelopeParserTests
{
    [Fact]
    public void TryParse_RealProtobufBytes_ReturnsTrueAndRoundTrips()
    {
        var original = Envelopes.Valid();
        var bytes = original.ToByteArray();

        var parsed = EnvelopeParser.TryParse(bytes, out var envelope);

        Assert.True(parsed);
        Assert.Equal(original.TransactionId, envelope.TransactionId);
        Assert.Equal(original.ServiceName, envelope.ServiceName);
        Assert.Equal(original.Data, envelope.Data);
    }

    [Fact]
    public void TryParse_MalformedBytes_ReturnsFalseWithoutThrowing()
    {
        // varint del campo 1 sin terminar (bit de continuación colgado)
        var malformed = new byte[] { 0x08, 0xFF, 0xFF, 0xFF };

        var parsed = EnvelopeParser.TryParse(malformed, out var envelope);

        Assert.False(parsed);
        Assert.Null(envelope);
    }

    [Fact]
    public void TryParse_EmptyBytes_ReturnsTrueWithDefaultEnvelope()
    {
        // Protobuf considera un buffer vacío como un mensaje con todos los campos por defecto:
        // el pipeline lo rechaza después vía EnvelopeValidator.
        var parsed = EnvelopeParser.TryParse([], out var envelope);

        Assert.True(parsed);
        Assert.False(EnvelopeValidator.IsValid(envelope));
    }
}
