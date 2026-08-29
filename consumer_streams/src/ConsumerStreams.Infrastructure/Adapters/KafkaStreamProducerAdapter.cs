using Confluent.Kafka;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Configuration;
using ConsumerStreams.Infrastructure.Logging;
using ConsumerStreams.Infrastructure.Messaging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ConsumerStreams.Infrastructure.Adapters;

/// <summary>
/// Adaptador de salida para publicar el evento procesado (JSON en claro) en el tópico de destino.
/// </summary>
public sealed class KafkaStreamProducerAdapter : IStreamProducerPort, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaStreamProducerAdapter> _logger;
    private bool _disposed;

    public KafkaStreamProducerAdapter(IOptions<KafkaStreamSettings> settingsOptions, ILogger<KafkaStreamProducerAdapter> logger)
    {
        _logger = logger;
        var settings = settingsOptions.Value;

        var config = new ProducerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            ClientId = "ConsumerStreams-SinkProducer-AOT",
            Acks = Acks.All,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => InfrastructureLog.SinkProducerError(_logger, e.Code, e.Reason))
            .Build();

        InfrastructureLog.SinkProducerInitialized(_logger, settings.BootstrapServers);
    }

    public async Task<bool> ForwardEventAsync(
        string targetTopic,
        string? key,
        string jsonPayload,
        IDictionary<string, string>? headers,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var message = new Message<string, string>
        {
            Key = key ?? string.Empty,
            Value = jsonPayload,
            Headers = KafkaHeaderMapper.ToKafkaHeaders(headers),
            Timestamp = new Timestamp(DateTime.UtcNow)
        };

        try
        {
            var report = await _producer.ProduceAsync(targetTopic, message, cancellationToken);
            InfrastructureLog.EventForwarded(_logger, report.Topic, report.Partition.Value, report.Offset.Value);
            return true;
        }
        catch (ProduceException<string, string> ex)
        {
            InfrastructureLog.EventForwardFailed(_logger, ex, targetTopic, ex.Error.Reason);
            return false;
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        try
        {
            _producer.Flush(TimeSpan.FromSeconds(3));
            _producer.Dispose();
        }
        catch (Exception ex)
        {
            InfrastructureLog.SinkProducerCloseFailed(_logger, ex);
        }

        GC.SuppressFinalize(this);
    }
}
