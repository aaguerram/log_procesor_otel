using Google.Protobuf;
using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Security;

/// <summary>
/// Construye el sobre <see cref="EncryptedErrorPayloadEnvelope"/> que se publica en el tópico de
/// error / DLQ. Reutiliza los metadatos del sobre original cuando existen y rellena valores
/// seguros cuando el mensaje entrante no era un sobre válido.
/// </summary>
public static class DlqEnvelopeFactory
{
    public static EncryptedErrorPayloadEnvelope Create(
        EncryptedPayloadEnvelope? sourceEnvelope,
        ReadOnlySpan<byte> rawBytes,
        string? messageKey,
        Exception failure,
        DateTimeOffset occurredAt,
        string fallbackTransactionId)
    {
        return new EncryptedErrorPayloadEnvelope
        {
            Data = sourceEnvelope?.Data ?? ByteString.CopyFrom(rawBytes),
            Nonce = sourceEnvelope?.Nonce ?? ByteString.CopyFrom(new byte[EnvelopeValidator.NonceSizeBytes]),
            AuthTag = sourceEnvelope?.AuthTag ?? ByteString.CopyFrom(new byte[EnvelopeValidator.AuthTagSizeBytes]),
            AlgorithmVersion = sourceEnvelope?.AlgorithmVersion ?? 1,
            CertThumbprint = sourceEnvelope?.CertThumbprint ?? "NONE",
            VaultTokenId = sourceEnvelope?.VaultTokenId ?? "NONE",
            TransactionId = Coalesce(sourceEnvelope?.TransactionId, messageKey, fallbackTransactionId),
            TimestampUnixMs = sourceEnvelope is { TimestampUnixMs: > 0 }
                ? sourceEnvelope.TimestampUnixMs
                : occurredAt.ToUnixTimeMilliseconds(),
            Swagger = sourceEnvelope?.Swagger ?? string.Empty,
            TelemetryType = sourceEnvelope?.TelemetryType ?? TelemetryType.Log,
            ServiceName = sourceEnvelope is { ServiceName.Length: > 0 } ? sourceEnvelope.ServiceName : "Unknown.Service",
            ErrorDetail = $"{failure.GetType().FullName}: {failure.Message}\n{failure.StackTrace}"
        };
    }

    private static string Coalesce(params string?[] candidates)
        => candidates.FirstOrDefault(c => !string.IsNullOrWhiteSpace(c)) ?? string.Empty;
}
