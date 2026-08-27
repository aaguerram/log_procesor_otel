using System.Collections.Concurrent;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Security;
using Microsoft.Extensions.Logging;

namespace ConsumerStreams.Infrastructure.Adapters;

/// <summary>
/// Resuelve el material criptográfico asociado a un token de Azure Key Vault y lo cachea en RAM
/// con TTL de 1 hora. La derivación concreta de la clave la hace <see cref="IAesKeyMaterialFactory"/>;
/// este adaptador sólo añade la caché. El reloj se inyecta para pruebas deterministas.
/// </summary>
public sealed class AzureKeyVaultTokenAdapter(
    IAesKeyMaterialFactory keyMaterialFactory,
    TimeProvider timeProvider,
    ILogger<AzureKeyVaultTokenAdapter> logger) : IVaultTokenProviderPort
{
    private static readonly TimeSpan CacheTtl = TimeSpan.FromHours(1);
    private readonly ConcurrentDictionary<string, CachedVaultEntry> _keyCache = new();

    private sealed record CachedVaultEntry(VaultKeyMaterial Material, DateTimeOffset ExpiresAtUtc);

    public Task<VaultKeyMaterial> ResolveKeyByTokenAsync(string vaultTokenId, string certThumbprint, CancellationToken cancellationToken = default)
    {
        var now = timeProvider.GetUtcNow();

        if (_keyCache.TryGetValue(vaultTokenId, out var cached) && now < cached.ExpiresAtUtc)
        {
            return Task.FromResult(cached.Material);
        }

        logger.LogInformation(
            "🌐 [Cache Miss / TTL Expirado] Resolviendo clave de Azure Key Vault para Token '{Token}' [Thumbprint: {Thumbprint}]...",
            vaultTokenId, certThumbprint);

        var material = keyMaterialFactory.Create(vaultTokenId, certThumbprint);
        var expiresAt = now.Add(CacheTtl);
        _keyCache[vaultTokenId] = new CachedVaultEntry(material, expiresAt);

        logger.LogInformation("✔ Clave de Key Vault almacenada en RAM con TTL de 1 hora (válida hasta {Expires}) para Token '{Token}'",
            expiresAt, vaultTokenId);

        return Task.FromResult(material);
    }
}
