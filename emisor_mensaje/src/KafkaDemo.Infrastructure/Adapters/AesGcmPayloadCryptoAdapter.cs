using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using KafkaDemo.Domain.Ports;
using Produbanco.Security.V1;

namespace KafkaDemo.Infrastructure.Adapters;

/// <summary>
/// Adaptador de cifrado y descifrado de ultra-alta velocidad utilizando AES-256-GCM y aceleración por hardware AES-NI.
/// </summary>
public class AesGcmPayloadCryptoAdapter : IPayloadCryptoPort
{
    private const int NonceSizeBytes = 12; // 96-bit nonce estándar para GCM (NIST SP 800-38D)
    private const int TagSizeBytes = 16;   // 128-bit authentication tag estándar

    public EncryptedPayloadEnvelope EncryptJsonToEnvelope(
        string jsonPayload,
        string eventId,
        string transactionId,
        string partitionKey,
        VaultKeyMaterial keyMaterial,
        IDictionary<string, string>? customHeaders = null,
        string? swaggerYaml = null,
        TelemetryType? telemetryType = null,
        string? serviceName = null)
    {
        var plaintextBytes = Encoding.UTF8.GetBytes(jsonPayload);
        var plaintextLength = plaintextBytes.Length;

        // 1. Generar Nonce criptográfico aleatorio único por mensaje
        var nonce = new byte[NonceSizeBytes];
        RandomNumberGenerator.Fill(nonce);

        var tag = new byte[TagSizeBytes];
        var ciphertext = new byte[plaintextLength];

        // 2. Cifrado autenticado por hardware AES-NI en CPU
        using (var aesGcm = new AesGcm(keyMaterial.AesKey256, TagSizeBytes))
        {
            var associatedData = Encoding.UTF8.GetBytes(transactionId);
            aesGcm.Encrypt(
                nonce.AsSpan(),
                plaintextBytes.AsSpan(),
                ciphertext.AsSpan(),
                tag.AsSpan(),
                associatedData.AsSpan());
        }

        // 3. Detectar o asignar tipo de señal de observabilidad OpenTelemetry (Trace, Metric, Log)
        var effectiveTelemetryType = telemetryType ?? DetectTelemetryType(jsonPayload);

        // 4. Empaquetado binario en Protocol Buffers Autosuficiente
        return new EncryptedPayloadEnvelope
        {
            Data = ByteString.CopyFrom(ciphertext),
            Nonce = ByteString.CopyFrom(nonce),
            AuthTag = ByteString.CopyFrom(tag),
            AlgorithmVersion = 1, // 1 = AES-256-GCM
            CertThumbprint = keyMaterial.CertThumbprint,
            VaultTokenId = keyMaterial.VaultTokenId,
            TransactionId = transactionId,
            TimestampUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Swagger = swaggerYaml ?? string.Empty,
            TelemetryType = effectiveTelemetryType,
            ServiceName = string.IsNullOrWhiteSpace(serviceName) ? "Transfer.Mspx.Prometeus.Management" : serviceName
        };
    }

    private static TelemetryType DetectTelemetryType(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return TelemetryType.Unspecified;

        if (json.Contains("TraceId", StringComparison.OrdinalIgnoreCase) || 
            json.Contains("SpanId", StringComparison.OrdinalIgnoreCase))
        {
            return TelemetryType.Trace;
        }

        if (json.Contains("metric", StringComparison.OrdinalIgnoreCase) ||
            json.Contains("resourceMetrics", StringComparison.OrdinalIgnoreCase))
        {
            return TelemetryType.Metric;
        }

        if (json.Contains("resourceLogs", StringComparison.OrdinalIgnoreCase) ||
            json.Contains("log_level", StringComparison.OrdinalIgnoreCase))
        {
            return TelemetryType.Log;
        }

        return TelemetryType.Trace;
    }

    public string DecryptEnvelopeToJson(EncryptedPayloadEnvelope envelope, VaultKeyMaterial keyMaterial)
    {
        var nonce = envelope.Nonce.ToByteArray();
        var tag = envelope.AuthTag.ToByteArray();
        var ciphertext = envelope.Data.ToByteArray();

        var decryptedBytes = new byte[ciphertext.Length];

        // 4. Descifrado y validación simultánea de integridad del Auth Tag
        using (var aesGcm = new AesGcm(keyMaterial.AesKey256, tag.Length))
        {
            var associatedData = Encoding.UTF8.GetBytes(envelope.TransactionId);
            aesGcm.Decrypt(
                nonce.AsSpan(),
                ciphertext.AsSpan(),
                tag.AsSpan(),
                decryptedBytes.AsSpan(),
                associatedData.AsSpan());
        }

        return Encoding.UTF8.GetString(decryptedBytes);
    }
}
