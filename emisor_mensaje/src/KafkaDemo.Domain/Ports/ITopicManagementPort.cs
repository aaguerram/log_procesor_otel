using KafkaDemo.Domain.Models;

namespace KafkaDemo.Domain.Ports;

/// <summary>
/// Puerto de salida para la administración de tópicos y clúster Kafka.
/// </summary>
public interface ITopicManagementPort
{
    Task<IReadOnlyList<TopicInfo>> GetTopicsAsync(bool includeInternal = false, CancellationToken cancellationToken = default);
    Task<bool> CreateTopicAsync(TopicCreationRequest request, CancellationToken cancellationToken = default);
    Task<bool> DeleteTopicAsync(string topicName, CancellationToken cancellationToken = default);
    Task<TopicInfo?> GetTopicDetailsAsync(string topicName, CancellationToken cancellationToken = default);
    Task<bool> PingAsync(CancellationToken cancellationToken = default);
}
