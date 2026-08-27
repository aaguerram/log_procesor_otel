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

/// <summary>Elemento consumido de Kafka en un lote.</summary>
public record KafkaBatchItem(
    string Key,
    byte[] RawBytes,
    string RawJson,
    int Partition,
    long Offset,
    IReadOnlyDictionary<string, string> Headers);

/// <summary>Elemento para inserción masiva con colección dinámica por Servicio + Tipo de Telemetría.</summary>
public record LogSinkItem(
    string RawJson,
    string PartitionKey,
    string? TargetCollection = null);
