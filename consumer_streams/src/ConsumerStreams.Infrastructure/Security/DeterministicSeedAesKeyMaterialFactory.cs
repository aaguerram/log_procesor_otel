using System.Security.Cryptography;
using System.Text;
using ConsumerStreams.Domain.Ports;

namespace ConsumerStreams.Infrastructure.Security;

/// <summary>
/// Estrategia de DEMO: deriva la clave AES-256 de una semilla fija compartida con el emisor
/// (<c>SHA-256("PRODUBANCO-SECRET-KEY-SEED-produbanco-encryption-cert-2026")</c>).
/// No es apta para producción; en un entorno real esta implementación se sustituye por una
/// que descargue la clave desde Azure Key Vault.
/// </summary>
public sealed class DeterministicSeedAesKeyMaterialFactory : IAesKeyMaterialFactory
{
    internal const string SharedSeed = "PRODUBANCO-SECRET-KEY-SEED-produbanco-encryption-cert-2026";
    private const string KeyVersion = "2026.1";

    private static readonly byte[] Aes256Key = SHA256.HashData(Encoding.UTF8.GetBytes(SharedSeed));

    public VaultKeyMaterial Create(string vaultTokenId, string certThumbprint) => new()
    {
        VaultTokenId = vaultTokenId,
        CertThumbprint = certThumbprint,
        KeyVersion = KeyVersion,
        AesKey256 = Aes256Key
    };
}
