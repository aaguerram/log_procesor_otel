using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaDemo.Domain.Models;
using KafkaDemo.Domain.Ports;
using KafkaDemo.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Infrastructure.Adapters;

/// <summary>
/// Adaptador de infraestructura para la gestión de tópicos y estado del clúster mediante AdminClient.
/// </summary>
public class KafkaAdminAdapter : ITopicManagementPort, IDisposable
{
    private readonly IAdminClient _adminClient;
    private readonly ILogger<KafkaAdminAdapter> _logger;
    private bool _disposed;

    public KafkaAdminAdapter(IOptions<KafkaSettings> settingsOptions, ILogger<KafkaAdminAdapter> logger)
    {
        _logger = logger;
        var settings = settingsOptions.Value;

        var config = new AdminClientConfig
        {
            BootstrapServers = settings.BootstrapServers,
            ClientId = $"{settings.ClientId}-Admin",
            SocketTimeoutMs = settings.SocketTimeoutMs
        };

        _adminClient = new AdminClientBuilder(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka Admin Error [{Code}]: {Reason}", e.Code, e.Reason))
            .SetLogHandler((_, log) => _logger.LogDebug("Kafka Admin Log [{Level}]: {Message}", log.Level, log.Message))
            .Build();

        _logger.LogInformation("Kafka Admin Adapter inicializado para servidores: {Servers}", settings.BootstrapServers);
    }

    public Task<IReadOnlyList<TopicInfo>> GetTopicsAsync(bool includeInternal = false, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            var metadata = _adminClient.GetMetadata(TimeSpan.FromSeconds(8));
            var list = new List<TopicInfo>();

            foreach (var topic in metadata.Topics)
            {
                var isInternal = topic.Topic.StartsWith("__") || topic.Topic.StartsWith("_");
                if (!includeInternal && isInternal)
                    continue;

                var partitions = topic.Partitions.Select(p => new PartitionDetail
                {
                    PartitionId = p.PartitionId,
                    Leader = p.Leader,
                    Replicas = p.Replicas,
                    InSyncReplicas = p.InSyncReplicas
                }).ToList();

                var replicationFactor = partitions.Count > 0 ? partitions[0].Replicas.Count : 1;

                list.Add(new TopicInfo
                {
                    Name = topic.Topic,
                    PartitionsCount = partitions.Count,
                    ReplicationFactor = replicationFactor,
                    Partitions = partitions,
                    IsInternal = isInternal
                });
            }

            return Task.FromResult<IReadOnlyList<TopicInfo>>(list.OrderBy(t => t.Name).ToList());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error al consultar la lista de tópicos de Kafka");
            return Task.FromResult<IReadOnlyList<TopicInfo>>([]);
        }
    }

    public async Task<bool> CreateTopicAsync(TopicCreationRequest request, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var topicSpec = new TopicSpecification
        {
            Name = request.TopicName,
            NumPartitions = request.NumPartitions,
            ReplicationFactor = request.ReplicationFactor,
            Configs = request.Configs != null ? new Dictionary<string, string>(request.Configs) : null
        };

        try
        {
            await _adminClient.CreateTopicsAsync([topicSpec]);
            _logger.LogInformation("Tópico '{Topic}' creado exitosamente con {Partitions} particiones y factor {Replication}",
                request.TopicName, request.NumPartitions, request.ReplicationFactor);
            return true;
        }
        catch (CreateTopicsException ex)
        {
            var result = ex.Results.FirstOrDefault();
            if (result != null && result.Error.Code == ErrorCode.TopicAlreadyExists)
            {
                _logger.LogWarning("El tópico '{Topic}' ya existía previamente.", request.TopicName);
                return true;
            }

            _logger.LogError(ex, "Error al crear el tópico '{Topic}': {Reason}", request.TopicName, result?.Error.Reason ?? ex.Message);
            throw new InvalidOperationException($"Error al crear el tópico: {result?.Error.Reason ?? ex.Message}", ex);
        }
    }

    public async Task<bool> DeleteTopicAsync(string topicName, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            await _adminClient.DeleteTopicsAsync([topicName]);
            _logger.LogInformation("Tópico '{Topic}' eliminado exitosamente.", topicName);
            return true;
        }
        catch (DeleteTopicsException ex)
        {
            var result = ex.Results.FirstOrDefault();
            _logger.LogError(ex, "Error al eliminar el tópico '{Topic}': {Reason}", topicName, result?.Error.Reason ?? ex.Message);
            throw new InvalidOperationException($"Error al eliminar el tópico: {result?.Error.Reason ?? ex.Message}", ex);
        }
    }

    public async Task<TopicInfo?> GetTopicDetailsAsync(string topicName, CancellationToken cancellationToken = default)
    {
        var topics = await GetTopicsAsync(includeInternal: true, cancellationToken);
        return topics.FirstOrDefault(t => t.Name.Equals(topicName, StringComparison.OrdinalIgnoreCase));
    }

    public Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            var metadata = _adminClient.GetMetadata(TimeSpan.FromSeconds(4));
            return Task.FromResult(metadata.Brokers.Count > 0);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Ping a clúster Kafka falló");
            return Task.FromResult(false);
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                _adminClient.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error cerrando el Kafka AdminClient");
            }
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}
