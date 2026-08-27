using ConsumerStreams.Domain.Security;
using ConsumerStreams.Domain.Tests.TestSupport;
using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Tests.Security;

public class DlqEnvelopeFactoryTests
{
    private static readonly DateTimeOffset Now = new(2026, 8, 27, 10, 0, 0, TimeSpan.Zero);
    private static readonly Exception Failure = new InvalidOperationException("descifrado falló");

    [Fact]
    public void Create_WithSourceEnvelope_ReusesItsMetadata()
    {
        var source = Envelopes.Valid(e => e.Swagger = "openapi: 3.0.0");

        var dlq = DlqEnvelopeFactory.Create(source, [], "k1", Failure, Now, "ERR-FALLBACK");

        Assert.Equal(source.Data, dlq.Data);
        Assert.Equal(source.Nonce, dlq.Nonce);
        Assert.Equal(source.TransactionId, dlq.TransactionId);
        Assert.Equal(source.VaultTokenId, dlq.VaultTokenId);
        Assert.Equal(source.TimestampUnixMs, dlq.TimestampUnixMs);
        Assert.Equal(source.Swagger, dlq.Swagger);
        Assert.Equal(TelemetryType.Trace, dlq.TelemetryType);
        Assert.Contains("InvalidOperationException", dlq.ErrorDetail);
        Assert.Contains("descifrado falló", dlq.ErrorDetail);
    }

    [Fact]
    public void Create_WithoutSourceEnvelope_UsesSafePlaceholders()
    {
        var raw = new byte[] { 9, 9, 9 };

        var dlq = DlqEnvelopeFactory.Create(null, raw, messageKey: null, Failure, Now, "ERR-FALLBACK");

        Assert.Equal(raw, dlq.Data.ToByteArray());
        Assert.Equal(12, dlq.Nonce.Length);
        Assert.Equal(16, dlq.AuthTag.Length);
        Assert.Equal(1, dlq.AlgorithmVersion);
        Assert.Equal("NONE", dlq.CertThumbprint);
        Assert.Equal("NONE", dlq.VaultTokenId);
        Assert.Equal("ERR-FALLBACK", dlq.TransactionId);
        Assert.Equal(Now.ToUnixTimeMilliseconds(), dlq.TimestampUnixMs);
        Assert.Equal(TelemetryType.Log, dlq.TelemetryType);
        Assert.Equal("Unknown.Service", dlq.ServiceName);
    }

    [Fact]
    public void Create_WithoutEnvelopeButWithKey_PrefersMessageKeyOverFallback()
    {
        var dlq = DlqEnvelopeFactory.Create(null, [], "message-key-1", Failure, Now, "ERR-FALLBACK");

        Assert.Equal("message-key-1", dlq.TransactionId);
    }
}
