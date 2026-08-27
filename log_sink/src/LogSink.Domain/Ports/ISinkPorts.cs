using LogSink.Domain.Models;

namespace LogSink.Domain.Ports;

/// <summary>
/// Puerto de consumo por lotes (Micro-Batching) desde Kafka.
/// </summary>
public interface IBatchConsumerPort
{
    Task StartBatchConsumerAsync(
        string topic,
        int maxBatchSize,
        TimeSpan maxWaitTime,
        Func<IReadOnlyList<KafkaBatchItem>, CancellationToken, Task<bool>> onBatchReceived,
        CancellationToken cancellationToken);
}

/// <summary>
/// Elemento consumido de Kafka en un lote.
/// </summary>
public record KafkaBatchItem(
    string Key,
    byte[] RawBytes,
    string RawJson,
    int Partition,
    long Offset,
    IReadOnlyDictionary<string, string> Headers);

/// <summary>
/// Puerto de persistencia masiva (Bulk Sink) hacia Azure Cosmos DB / DocumentDB.
/// </summary>
public interface IDocumentDbBulkSinkPort
{
    Task<BulkSinkResult> BulkInsertLogsAsync(
        IReadOnlyList<LogDocument> documents,
        CancellationToken cancellationToken = default);

    Task<BulkSinkResult> BulkInsertRawJsonLogsAsync(
        IReadOnlyList<(string RawJson, string PartitionKey)> items,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Puerto de resolución de credenciales tokenizadas de Azure Key Vault con TTL de 1 hora.
/// </summary>
public interface IVaultTokenProviderPort
{
    Task<CosmosDbCredentials> ResolveCosmosCredentialsAsync(
        string vaultTokenId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Credenciales resueltas para Azure Cosmos DB.
/// </summary>
public record CosmosDbCredentials(
    string Endpoint,
    string PrimaryKey,
    string DatabaseName,
    string ContainerName,
    string PartitionKeyPath);
