using System.Collections.Concurrent;
using LogSink.Domain.Ports;
using LogSink.Infrastructure.Configuration;
using LogSink.Infrastructure.Logging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LogSink.Infrastructure.Adapters;

/// <summary>
/// Resuelve las credenciales tokenizadas de Cosmos DB desde Azure Key Vault, con caché en RAM
/// y TTL de 1 hora. El reloj se inyecta como <see cref="TimeProvider"/> para permitir pruebas
/// deterministas de la expiración.
/// </summary>
public sealed class AzureKeyVaultTokenAdapter(
    IOptions<SinkSettings> settings,
    TimeProvider timeProvider,
    ILogger<AzureKeyVaultTokenAdapter> logger) : IVaultTokenProviderPort
{
    private static readonly TimeSpan CacheTtl = TimeSpan.FromHours(1);

    private readonly SinkSettings _settings = settings.Value;
    private readonly ConcurrentDictionary<string, CachedCredentialsEntry> _credentialsCache = new();

    private sealed record CachedCredentialsEntry(CosmosDbCredentials Credentials, DateTimeOffset ExpiresAtUtc);

    public Task<CosmosDbCredentials> ResolveCosmosCredentialsAsync(string vaultTokenId, CancellationToken cancellationToken = default)
    {
        var now = timeProvider.GetUtcNow();

        if (_credentialsCache.TryGetValue(vaultTokenId, out var cached) && now < cached.ExpiresAtUtc)
        {
            return Task.FromResult(cached.Credentials);
        }

        InfrastructureLog.VaultCredentialsResolving(logger, vaultTokenId);

        var credentials = new CosmosDbCredentials(
            Endpoint: _settings.CosmosEndpoint,
            PrimaryKey: _settings.CosmosPrimaryKey,
            DatabaseName: _settings.DatabaseName,
            ContainerName: _settings.ContainerName,
            PartitionKeyPath: _settings.PartitionKeyPath);

        var expiresAt = now.Add(CacheTtl);
        _credentialsCache[vaultTokenId] = new CachedCredentialsEntry(credentials, expiresAt);

        InfrastructureLog.VaultCredentialsCached(logger, expiresAt, vaultTokenId);

        return Task.FromResult(credentials);
    }
}
