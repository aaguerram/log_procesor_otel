using KafkaDemo.Application.DTOs;
using KafkaDemo.Domain.Models;
using KafkaDemo.Domain.Ports;

namespace KafkaDemo.Application.UseCases;

/// <summary>
/// Caso de uso para la administración y monitoreo de tópicos de Kafka.
/// </summary>
public class ManageTopicsUseCase(ITopicManagementPort topicManagementPort)
{
    public async Task<IReadOnlyList<TopicInfo>> ListTopicsAsync(bool includeInternal = false, CancellationToken cancellationToken = default)
    {
        return await topicManagementPort.GetTopicsAsync(includeInternal, cancellationToken);
    }

    public async Task<bool> CreateTopicAsync(CreateTopicDto dto, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(dto.TopicName))
            throw new ArgumentException("El nombre del tópico es obligatorio.", nameof(dto.TopicName));

        var request = new TopicCreationRequest
        {
            TopicName = dto.TopicName.Trim(),
            NumPartitions = Math.Max(1, dto.Partitions),
            ReplicationFactor = (short)Math.Max(1, (int)dto.ReplicationFactor),
            Configs = dto.Configs
        };

        return await topicManagementPort.CreateTopicAsync(request, cancellationToken);
    }

    public async Task<bool> DeleteTopicAsync(string topicName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentException("El nombre del tópico no puede estar vacío.", nameof(topicName));

        return await topicManagementPort.DeleteTopicAsync(topicName.Trim(), cancellationToken);
    }

    public async Task<TopicInfo?> GetTopicDetailsAsync(string topicName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName)) return null;
        return await topicManagementPort.GetTopicDetailsAsync(topicName.Trim(), cancellationToken);
    }

    public async Task<ClusterHealthDto> CheckClusterHealthAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var isConnected = await topicManagementPort.PingAsync(cancellationToken);
            var topics = isConnected ? await topicManagementPort.GetTopicsAsync(includeInternal: false, cancellationToken) : [];

            return new ClusterHealthDto
            {
                IsConnected = isConnected,
                TotalTopics = topics.Count,
                Status = isConnected ? "Healthy" : "Degraded",
                CheckedAt = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            return new ClusterHealthDto
            {
                IsConnected = false,
                TotalTopics = 0,
                Status = $"Error: {ex.Message}",
                CheckedAt = DateTime.UtcNow
            };
        }
    }
}
