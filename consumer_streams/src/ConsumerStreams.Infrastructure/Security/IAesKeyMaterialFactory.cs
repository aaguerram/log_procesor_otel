using ConsumerStreams.Domain.Ports;

namespace ConsumerStreams.Infrastructure.Security;

/// <summary>
/// Obtiene el material criptográfico (clave AES-256) asociado a un token de Azure Key Vault.
/// Se aísla en su propia abstracción para que la caché (<see cref="Adapters.AzureKeyVaultTokenAdapter"/>)
/// no conozca cómo se resuelve la clave y para poder sustituir la estrategia (demo vs. Key Vault real).
/// </summary>
public interface IAesKeyMaterialFactory
{
    VaultKeyMaterial Create(string vaultTokenId, string certThumbprint);
}
