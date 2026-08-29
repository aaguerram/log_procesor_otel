using Confluent.Kafka;
using LogSink.Domain.Ports;
using LogSink.Infrastructure.Configuration;
using LogSink.Infrastructure.Logging;
using LogSink.Infrastructure.Messaging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LogSink.Infrastructure.Adapters;

/// <summary>
/// Adaptador de salida para publicación individual de eventos fallidos en la cola DLQ de Kafka.
/// Compatible con .NET 10 Native AOT sin reflexión.
/// </summary>
public sealed class KafkaDlqProducerAdapter : IDlqProducerPort, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaDlqProducerAdapter> _logger;
    private bool _disposed;

    public KafkaDlqProducerAdapter(
        IOptions<SinkSettings> settingsOptions,
        ILogger<KafkaDlqProducerAdapter> logger)
    {
        _logger = logger;
        var settings = settingsOptions.Value;

        var config = new ProducerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            ClientId = "LogSink-DLQProducer-AOT",
            Acks = Acks.All,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => InfrastructureLog.DlqProducerError(_logger, e.Code, e.Reason))
            .Build();

        InfrastructureLog.DlqProducerInitialized(_logger, settings.BootstrapServers);
    }

    public async Task<bool> SendToDlqAsync(
        string dlqTopic,
        string partitionKey,
        string rawJson,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var message = new Message<string, string>
        {
            Key = partitionKey ?? string.Empty,
            Value = rawJson ?? string.Empty,
            Headers = KafkaHeaderMapper.ToKafkaHeaders(headers),
            Timestamp = new Timestamp(DateTime.UtcNow)
        };

        try
        {
            var deliveryReport = await _producer.ProduceAsync(dlqTopic, message, cancellationToken);
            InfrastructureLog.DlqItemProduced(
                _logger, deliveryReport.Topic, deliveryReport.Partition.Value, deliveryReport.Offset.Value, partitionKey ?? string.Empty);
            return true;
        }
        catch (ProduceException<string, string> ex)
        {
            InfrastructureLog.DlqPublishFailed(_logger, ex, dlqTopic, ex.Error.Reason);
            return false;
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                _producer.Flush(TimeSpan.FromSeconds(3));
                _producer.Dispose();
            }
            catch (Exception ex)
            {
                InfrastructureLog.DlqProducerCloseFailed(_logger, ex);
            }
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}
