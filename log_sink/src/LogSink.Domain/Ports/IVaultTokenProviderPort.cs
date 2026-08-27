namespace LogSink.Domain.Ports;

/// <summary>
/// Puerto de resolución de credenciales tokenizadas de Azure Key Vault con TTL de 1 hora.
/// </summary>
public interface IVaultTokenProviderPort
{
    Task<CosmosDbCredentials> ResolveCosmosCredentialsAsync(
        string vaultTokenId,
        CancellationToken cancellationToken = default);
}

/// <summary>Credenciales resueltas para Azure Cosmos DB.</summary>
public record CosmosDbCredentials(
    string Endpoint,
    string PrimaryKey,
    string DatabaseName,
    string ContainerName,
    string PartitionKeyPath);
