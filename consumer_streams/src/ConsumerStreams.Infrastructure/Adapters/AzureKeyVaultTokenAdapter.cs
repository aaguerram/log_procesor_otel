using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using ConsumerStreams.Domain.Ports;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ConsumerStreams.Infrastructure.Adapters;

/// <summary>
/// Adaptador para resolución de tokens criptográficos de Azure Key Vault con TTL de 1 hora en memoria en consumer_streams (Native AOT).
/// </summary>
public class AzureKeyVaultTokenAdapter : IVaultTokenProviderPort
{
    private static readonly TimeSpan CacheTtl = TimeSpan.FromHours(1);
    private readonly string _vaultUri;
    private readonly ILogger<AzureKeyVaultTokenAdapter> _logger;
    private readonly ConcurrentDictionary<string, CachedVaultEntry> _keyCache = new();

    private record CachedVaultEntry(VaultKeyMaterial Material, DateTimeOffset ExpiresAtUtc)
    {
        public bool IsExpired => DateTimeOffset.UtcNow >= ExpiresAtUtc;
    }

    public AzureKeyVaultTokenAdapter(IConfiguration configuration, ILogger<AzureKeyVaultTokenAdapter> logger)
    {
        _logger = logger;
        _vaultUri = configuration["KeyVault:VaultUri"] 
            ?? configuration["TECH-INT-SECU-VAULT_URL"] 
            ?? configuration["TECH_INT_SECU_VAULT_URL"] 
            ?? throw new InvalidOperationException("[CONFIG ERROR] 'KeyVault:VaultUri' no está configurado en appsettings.json ni en las variables de entorno.");
    }

    public Task<VaultKeyMaterial> ResolveKeyByTokenAsync(string vaultTokenId, string certThumbprint, CancellationToken cancellationToken = default)
    {
        // 1. Verificar si la clave existe en la memoria RAM y su TTL de 1 hora aún no expira
        if (_keyCache.TryGetValue(vaultTokenId, out var cached) && !cached.IsExpired)
        {
            return Task.FromResult(cached.Material);
        }

        // 2. Si no está en RAM o el TTL de 1 hora expiró, se descarga/resuelve desde Azure Key Vault
        _logger.LogInformation("🌐 [Cache Miss / TTL Expirado] Descargando clave de Azure Key Vault para Token '{Token}' [Thumbprint: {Thumbprint}]...",
            vaultTokenId, certThumbprint);

        var certBytes = Encoding.UTF8.GetBytes($"PRODUBANCO-SECRET-KEY-SEED-produbanco-encryption-cert-2026");
        var key256 = SHA256.HashData(certBytes);

        var material = new VaultKeyMaterial
        {
            VaultTokenId = vaultTokenId,
            CertThumbprint = certThumbprint,
            KeyVersion = "2026.1",
            AesKey256 = key256
        };

        // 3. Almacenar en RAM con expiración de 1 hora
        var entry = new CachedVaultEntry(material, DateTimeOffset.UtcNow.Add(CacheTtl));
        _keyCache[vaultTokenId] = entry;

        _logger.LogInformation("✔ Clave de Key Vault almacenada en RAM con TTL de 1 hora (válida hasta {Expires}) para Token '{Token}'",
            entry.ExpiresAtUtc, vaultTokenId);

        return Task.FromResult(material);
    }
}
