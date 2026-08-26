using System.Collections.Concurrent;
using LogSink.Domain.Ports;
using LogSink.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LogSink.Infrastructure.Adapters;

/// <summary>
/// Adaptador para resolución de credenciales tokenizadas de Azure Key Vault con caché en RAM y TTL de 1 hora.
/// </summary>
public class AzureKeyVaultTokenAdapter : IVaultTokenProviderPort
{
    private static readonly TimeSpan CacheTtl = TimeSpan.FromHours(1);
    private readonly ConcurrentDictionary<string, CachedCredentialsEntry> _credentialsCache = new();
    private readonly SinkSettings _settings;
    private readonly ILogger<AzureKeyVaultTokenAdapter> _logger;

    private record CachedCredentialsEntry(CosmosDbCredentials Credentials, DateTimeOffset ExpiresAtUtc)
    {
        public bool IsExpired => DateTimeOffset.UtcNow >= ExpiresAtUtc;
    }

    public AzureKeyVaultTokenAdapter(IOptions<SinkSettings> settings, ILogger<AzureKeyVaultTokenAdapter> logger)
    {
        _settings = settings.Value;
        _logger = logger;
    }

    public Task<CosmosDbCredentials> ResolveCosmosCredentialsAsync(string vaultTokenId, CancellationToken cancellationToken = default)
    {
        // 1. Verificar si existe en memoria RAM y si su TTL no ha expirado
        if (_credentialsCache.TryGetValue(vaultTokenId, out var cachedEntry) && !cachedEntry.IsExpired)
        {
            return Task.FromResult(cachedEntry.Credentials);
        }

        // 2. Cache Miss o TTL expirado: Resolver desde Azure Key Vault
        _logger.LogInformation("🌐 [Cache Miss / TTL Expirado] Descargando credenciales de Cosmos DB de Azure Key Vault para Token '{Token}'...", vaultTokenId);

        var credentials = new CosmosDbCredentials(
            Endpoint: _settings.CosmosEndpoint,
            PrimaryKey: _settings.CosmosPrimaryKey,
            DatabaseName: _settings.DatabaseName,
            ContainerName: _settings.ContainerName,
            PartitionKeyPath: _settings.PartitionKeyPath);

        var expiration = DateTimeOffset.UtcNow.Add(CacheTtl);
        _credentialsCache[vaultTokenId] = new CachedCredentialsEntry(credentials, expiration);

        _logger.LogInformation("✔ Credenciales de Cosmos DB almacenadas en RAM con TTL de 1 hora (válidas hasta {Expires}) para Token '{Token}'",
            expiration, vaultTokenId);

        return Task.FromResult(credentials);
    }
}
