namespace KafkaDemo.Domain.Models;

/// <summary>
/// Resultado del envío de un mensaje a Kafka.
/// </summary>
public record MessageResult
{
    public required string Topic { get; init; }
    public int Partition { get; init; }
    public long Offset { get; init; }
    public string Status { get; init; } = "Persisted";
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public string? Key { get; init; }
}
