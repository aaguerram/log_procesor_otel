using System.Security.Cryptography;
using System.Text;
using ConsumerStreams.Domain.Ports;
using Produbanco.Security.V1;

namespace ConsumerStreams.Infrastructure.Adapters;

/// <summary>
/// Adaptador de descifrado AES-256-GCM de ultra-alta velocidad y compatible con Native AOT en .NET 10.
/// </summary>
public class AesGcmPayloadCryptoAdapter : IPayloadCryptoPort
{
    public string DecryptEnvelopeToJson(EncryptedPayloadEnvelope envelope, VaultKeyMaterial keyMaterial)
    {
        var decryptedBytes = new byte[envelope.Data.Length];

        // Descifrado directo con aceleración por hardware AES-NI usando Span y validación de Auth Tag
        using (var aesGcm = new AesGcm(keyMaterial.AesKey256, tagSizeInBytes: 16))
        {
            var associatedData = Encoding.UTF8.GetBytes(envelope.TransactionId);
            aesGcm.Decrypt(
                nonce: envelope.Nonce.Span,
                ciphertext: envelope.Data.Span,
                tag: envelope.AuthTag.Span,
                plaintext: decryptedBytes.AsSpan(),
                associatedData: associatedData.AsSpan());
        }

        return Encoding.UTF8.GetString(decryptedBytes);
    }
}
