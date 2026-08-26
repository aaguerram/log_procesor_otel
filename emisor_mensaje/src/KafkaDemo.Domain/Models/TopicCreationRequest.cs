namespace KafkaDemo.Domain.Models;

/// <summary>
/// Parámetros para la creación de un nuevo tópico en Kafka.
/// </summary>
public record TopicCreationRequest
{
    public required string TopicName { get; init; }
    public int NumPartitions { get; init; } = 1;
    public short ReplicationFactor { get; init; } = 1;
    public IDictionary<string, string>? Configs { get; init; }
}
