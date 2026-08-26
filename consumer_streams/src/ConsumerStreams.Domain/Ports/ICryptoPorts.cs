using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Ports;

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
/// Puerto de dominio para resolución de claves y certificados tokenizados desde Azure Key Vault.
/// </summary>
public interface IVaultTokenProviderPort
{
    Task<VaultKeyMaterial> ResolveKeyByTokenAsync(string vaultTokenId, string certThumbprint, CancellationToken cancellationToken = default);
}

/// <summary>
/// Puerto de dominio para descifrado de alto rendimiento con AES-256-GCM (Zero Allocation).
/// </summary>
public interface IPayloadCryptoPort
{
    string DecryptEnvelopeToJson(
        EncryptedPayloadEnvelope envelope,
        VaultKeyMaterial keyMaterial);
}
