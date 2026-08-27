using KafkaDemo.Domain.Models;

namespace KafkaDemo.Application.DTOs;

public record SendMessageRequestDto
{
    public required string Topic { get; init; }
    public string? Key { get; init; }
    public required string Value { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
    public string? TelemetryType { get; init; }
    public string? ServiceName { get; init; }
}

public record BatchSendResultDto
{
    public int TotalRequested { get; init; }
    public int TotalSent { get; init; }
    public required string TargetTopic { get; init; }
    public double ElapsedMilliseconds { get; init; }
    public IReadOnlyList<MessageResult> Results { get; init; } = [];
}

public record CreateTopicDto
{
    public required string TopicName { get; init; }
    public int Partitions { get; init; } = 3;
    public short ReplicationFactor { get; init; } = 1;
    public Dictionary<string, string>? Configs { get; init; }
}

public record ClusterHealthDto
{
    public bool IsConnected { get; init; }
    public int TotalTopics { get; init; }
    public string Status { get; init; } = "Unknown";
    public DateTime CheckedAt { get; init; } = DateTime.UtcNow;
}
