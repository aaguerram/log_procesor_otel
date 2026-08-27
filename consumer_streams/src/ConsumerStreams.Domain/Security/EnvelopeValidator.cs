using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Security;

/// <summary>
/// Valida los invariantes del sobre <see cref="EncryptedPayloadEnvelope"/>: todos los campos son
/// obligatorios salvo <c>swagger</c>. Antes esta regla estaba duplicada en el emisor y en el
/// caso de uso del pipeline; ahora vive una sola vez en el dominio.
/// </summary>
public static class EnvelopeValidator
{
    public const int NonceSizeBytes = 12;
    public const int AuthTagSizeBytes = 16;

    /// <summary>Lanza <see cref="InvalidOperationException"/> si algún campo obligatorio falta o es inválido.</summary>
    public static void Validate(EncryptedPayloadEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        Require(envelope.Data is { Length: > 0 }, "El campo 'data' del sobre protobuf es obligatorio y no puede estar vacío.");
        Require(envelope.Nonce is { Length: NonceSizeBytes },
            $"El campo 'nonce' es obligatorio y debe tener exactamente {NonceSizeBytes} bytes (recibido: {envelope.Nonce?.Length ?? 0}).");
        Require(envelope.AuthTag is { Length: AuthTagSizeBytes },
            $"El campo 'auth_tag' es obligatorio y debe tener exactamente {AuthTagSizeBytes} bytes (recibido: {envelope.AuthTag?.Length ?? 0}).");
        Require(envelope.AlgorithmVersion > 0, "El campo 'algorithm_version' es obligatorio y debe ser mayor a 0 (1 = AES-256-GCM).");
        Require(!string.IsNullOrWhiteSpace(envelope.CertThumbprint), "El campo 'cert_thumbprint' es obligatorio.");
        Require(!string.IsNullOrWhiteSpace(envelope.VaultTokenId), "El campo 'vault_token_id' es obligatorio.");
        Require(!string.IsNullOrWhiteSpace(envelope.TransactionId), "El campo 'transaction_id' es obligatorio.");
        Require(envelope.TimestampUnixMs > 0, "El campo 'timestamp_unix_ms' es obligatorio y debe ser mayor a 0.");
        Require(envelope.TelemetryType != TelemetryType.Unspecified,
            "El campo 'telemetry_type' es obligatorio y debe ser Trace (1), Metric (2) o Log (3).");
        Require(!string.IsNullOrWhiteSpace(envelope.ServiceName), "El campo 'service_name' es obligatorio.");

        // Nota: envelope.Swagger es el ÚNICO campo opcional permitido.
    }

    /// <summary>Versión no lanzadora, útil para clasificar mensajes.</summary>
    public static bool IsValid(EncryptedPayloadEnvelope? envelope)
    {
        if (envelope is null)
        {
            return false;
        }

        try
        {
            Validate(envelope);
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }
}
