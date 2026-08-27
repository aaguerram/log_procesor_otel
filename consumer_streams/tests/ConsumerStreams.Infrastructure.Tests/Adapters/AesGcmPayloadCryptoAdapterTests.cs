using System.Security.Cryptography;
using System.Text;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Adapters;
using Google.Protobuf;
using Produbanco.Security.V1;

namespace ConsumerStreams.Infrastructure.Tests.Adapters;

public class AesGcmPayloadCryptoAdapterTests
{
    private readonly AesGcmPayloadCryptoAdapter _adapter = new();
    private static readonly byte[] Key = SHA256.HashData("test-key"u8.ToArray());

    private static EncryptedPayloadEnvelope Encrypt(string json, string transactionId, byte[] key)
    {
        var nonce = new byte[12];
        RandomNumberGenerator.Fill(nonce);
        var plaintext = Encoding.UTF8.GetBytes(json);
        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[16];

        using var aes = new AesGcm(key, 16);
        aes.Encrypt(nonce, plaintext, ciphertext, tag, Encoding.UTF8.GetBytes(transactionId));

        return new EncryptedPayloadEnvelope
        {
            Data = ByteString.CopyFrom(ciphertext),
            Nonce = ByteString.CopyFrom(nonce),
            AuthTag = ByteString.CopyFrom(tag),
            AlgorithmVersion = 1,
            TransactionId = transactionId,
            CertThumbprint = "x",
            VaultTokenId = "x",
            TimestampUnixMs = 1,
            TelemetryType = TelemetryType.Trace,
            ServiceName = "svc"
        };
    }

    private static VaultKeyMaterial Material(byte[] key) => new()
    {
        VaultTokenId = "t", CertThumbprint = "c", KeyVersion = "1", AesKey256 = key
    };

    [Fact]
    public void DecryptEnvelopeToJson_RoundTripsThePlaintext()
    {
        const string json = """{"TransactionId":"TXN-9","Amount":123.45}""";
        var envelope = Encrypt(json, "TXN-9", Key);

        Assert.Equal(json, _adapter.DecryptEnvelopeToJson(envelope, Material(Key)));
    }

    [Fact]
    public void DecryptEnvelopeToJson_WrongKey_ThrowsAuthenticationTagMismatch()
    {
        var envelope = Encrypt("{}", "TXN-1", Key);
        var wrongKey = SHA256.HashData("other"u8.ToArray());

        Assert.Throws<AuthenticationTagMismatchException>(() => _adapter.DecryptEnvelopeToJson(envelope, Material(wrongKey)));
    }

    [Fact]
    public void DecryptEnvelopeToJson_TamperedTransactionIdBreaksAssociatedData()
    {
        var envelope = Encrypt("{}", "TXN-1", Key);
        envelope.TransactionId = "TXN-TAMPERED";

        Assert.Throws<AuthenticationTagMismatchException>(() => _adapter.DecryptEnvelopeToJson(envelope, Material(Key)));
    }
}
