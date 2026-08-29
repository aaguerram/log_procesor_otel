using System.Text;
using ConsumerStreams.Domain.DataProtection;
using ConsumerStreams.Domain.Observability;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Domain.Security;
using Produbanco.Security.V1;

namespace ConsumerStreams.Application.Services;

/// <summary>
/// Encapsula el tramo criptográfico del pipeline: validación del sobre, resolución de la clave en
/// Vault (con caché RAM), descifrado AES-256-GCM y enmascarado <c>x-log-data-protection</c>.
/// Agrupa a estos tres colaboradores para que el caso de uso orquestador no supere el límite de
/// parámetros del constructor y para poder verificar el descifrado de forma aislada.
/// </summary>
public sealed class EnvelopeDecryptionService(
    IVaultTokenProviderPort vaultTokenPort,
    IPayloadCryptoPort cryptoPort,
    PayloadMaskingService maskingService)
{
    private const string LegacyServiceName = "Transfer.Mspx.Prometeus.Management";

    /// <summary>Descifra el sobre (o toma el texto plano legacy) y aplica el enmascarado si procede.</summary>
    public async Task<DecodedMessage> DecryptAndMaskAsync(
        EncryptedPayloadEnvelope? envelope,
        byte[] rawBytes,
        CancellationToken cancellationToken)
    {
        if (envelope is null)
        {
            return new DecodedMessage(Encoding.UTF8.GetString(rawBytes), LegacyServiceName, "Trace", "NONE");
        }

        EnvelopeValidator.Validate(envelope);

        var keyMaterial = await vaultTokenPort.ResolveKeyByTokenAsync(envelope.VaultTokenId, envelope.CertThumbprint, cancellationToken);
        var decryptedJson = cryptoPort.DecryptEnvelopeToJson(envelope, keyMaterial);
        decryptedJson = maskingService.ApplyIfApplicable(envelope, decryptedJson);

        return new DecodedMessage(
            decryptedJson,
            envelope.ServiceName,
            TelemetryTypeMapper.ToLabel(envelope.TelemetryType),
            envelope.VaultTokenId);
    }
}

/// <summary>Resultado del descifrado: JSON en claro más metadatos de servicio y telemetría.</summary>
public readonly record struct DecodedMessage(string Json, string ServiceName, string TelemetryLabel, string VaultToken);
