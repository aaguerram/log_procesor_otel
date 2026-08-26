namespace KafkaDemo.Domain.Models;

/// <summary>
/// Representa un mensaje de dominio para ser publicado en Kafka (soporta texto JSON o binario Protobuf).
/// </summary>
public record KafkaMessage
{
    public required string Topic { get; init; }
    public string? Key { get; init; }
    public string? Value { get; init; }
    public byte[]? BinaryValue { get; init; }
    public bool IsBinary => BinaryValue != null && BinaryValue.Length > 0;
    public IDictionary<string, string>? Headers { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}
