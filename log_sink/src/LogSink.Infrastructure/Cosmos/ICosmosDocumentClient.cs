using LogSink.Domain.Ports;

namespace LogSink.Infrastructure.Cosmos;

/// <summary>
/// Cliente de bajo nivel del protocolo REST de Azure Cosmos DB para una sola operación de upsert.
/// </summary>
public interface ICosmosDocumentClient
{
    /// <summary>
    /// Realiza un upsert del JSON indicado en la colección resuelta.
    /// </summary>
    /// <returns>Las Request Units (RU) consumidas por la operación.</returns>
    /// <exception cref="CosmosTransientException">Fallo transitorio (429 / 5xx) que debe reintentarse.</exception>
    /// <exception cref="InvalidOperationException">Error no recuperable (4xx distinto de 429).</exception>
    Task<double> UpsertDocumentAsync(
        CosmosDbCredentials credentials,
        string? targetCollection,
        string partitionKey,
        string rawJson,
        CancellationToken cancellationToken);
}
