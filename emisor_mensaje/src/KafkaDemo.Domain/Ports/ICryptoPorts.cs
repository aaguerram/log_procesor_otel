using Produbanco.Security.V1;

namespace KafkaDemo.Domain.Ports;

/// <summary>
/// Representa el material criptográfico y los metadatos tokenizados recuperados de Azure Key Vault.
/// </summary>
public record VaultKeyMaterial
{
    public required string VaultTokenId { get; init; }
    public required string CertThumbprint { get; init; }
    public required string KeyVersion { get; init; }
    public required byte[] AesKey256 { get; init; }
}

/// <summary>
/// Puerto de dominio para gestión y tokenización de certificados/claves desde Azure Key Vault.
/// </summary>
public interface IVaultTokenProviderPort
{
    Task<VaultKeyMaterial> GetOrCreateEncryptionKeyAsync(string certificateName, CancellationToken cancellationToken = default);
    Task<VaultKeyMaterial> ResolveKeyByTokenAsync(string vaultTokenId, string certThumbprint, CancellationToken cancellationToken = default);
}

/// <summary>
/// Puerto de dominio para cifrado y descifrado de alto rendimiento con AES-256-GCM (Zero Allocation).
/// </summary>
public interface IPayloadCryptoPort
{
    EncryptedPayloadEnvelope EncryptJsonToEnvelope(
        string jsonPayload,
        string eventId,
        string transactionId,
        string partitionKey,
        VaultKeyMaterial keyMaterial,
        IDictionary<string, string>? customHeaders = null,
        string? swaggerYaml = null);

    string DecryptEnvelopeToJson(
        EncryptedPayloadEnvelope envelope,
        VaultKeyMaterial keyMaterial);
}
