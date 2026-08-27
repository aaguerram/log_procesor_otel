using Google.Protobuf;
using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Tests.TestSupport;

/// <summary>Fábricas de sobres Protobuf válidos para las pruebas.</summary>
public static class Envelopes
{
    public static EncryptedPayloadEnvelope Valid(Action<EncryptedPayloadEnvelope>? customize = null)
    {
        var envelope = new EncryptedPayloadEnvelope
        {
            Data = ByteString.CopyFrom(new byte[] { 1, 2, 3, 4 }),
            Nonce = ByteString.CopyFrom(new byte[12]),
            AuthTag = ByteString.CopyFrom(new byte[16]),
            AlgorithmVersion = 1,
            CertThumbprint = "AA11BB22",
            VaultTokenId = "TKN-KV-PRODUBANCO-V1",
            TransactionId = "TXN-20260827-ABC123",
            TimestampUnixMs = 1_800_000_000_000,
            TelemetryType = TelemetryType.Trace,
            ServiceName = "Transfer.Mspx.Prometeus.Management",
            Swagger = string.Empty
        };

        customize?.Invoke(envelope);
        return envelope;
    }
}
