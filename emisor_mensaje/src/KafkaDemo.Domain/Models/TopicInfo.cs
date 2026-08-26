namespace KafkaDemo.Domain.Models;

/// <summary>
/// Información detallada de una partición de un tópico.
/// </summary>
public record PartitionDetail
{
    public int PartitionId { get; init; }
    public int Leader { get; init; }
    public IReadOnlyList<int> Replicas { get; init; } = [];
    public IReadOnlyList<int> InSyncReplicas { get; init; } = [];
}

/// <summary>
/// Información de metadatos de un tópico en Kafka.
/// </summary>
public record TopicInfo
{
    public required string Name { get; init; }
    public int PartitionsCount { get; init; }
    public int ReplicationFactor { get; init; }
    public IReadOnlyList<PartitionDetail> Partitions { get; init; } = [];
    public bool IsInternal { get; init; }
}
