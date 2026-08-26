using System.Text;
using Confluent.Kafka;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ConsumerStreams.Infrastructure.Adapters;

/// <summary>
/// Adaptador de salida (Sink Adapter) para enviar eventos procesados a Kafka.
/// </summary>
public class KafkaStreamProducerAdapter : IStreamProducerPort, IDisposable
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
            .SetErrorHandler((_, e) => _logger.LogError("Sink Producer Kafka Error [{Code}]: {Reason}", e.Code, e.Reason))
            .Build();

        _logger.LogInformation("Sink Producer Adapter inicializado para bootstrap servers: {Servers}", settings.BootstrapServers);
    }

    public async Task<bool> ForwardEventAsync(
        string targetTopic,
        string? key,
        string jsonPayload,
        IDictionary<string, string>? headers,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var kafkaHeaders = new Headers();
        if (headers != null)
        {
            foreach (var (k, v) in headers)
            {
                if (v != null)
                {
                    kafkaHeaders.Add(k, Encoding.UTF8.GetBytes(v));
                }
            }
        }

        var message = new Message<string, string>
        {
            Key = key ?? string.Empty,
            Value = jsonPayload,
            Headers = kafkaHeaders,
            Timestamp = new Timestamp(DateTime.UtcNow)
        };

        try
        {
            var deliveryReport = await _producer.ProduceAsync(targetTopic, message, cancellationToken);
            _logger.LogDebug("Evento reenviado a '{Topic}' [Partición {Partition}, Offset {Offset}]",
                deliveryReport.Topic, deliveryReport.Partition.Value, deliveryReport.Offset.Value);
            return true;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Fallo al reenviar evento procesado a '{Topic}': {Reason}", targetTopic, ex.Error.Reason);
            return false;
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                _producer.Flush(TimeSpan.FromSeconds(5));
                _producer.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error cerrando el sink producer de Kafka");
            }
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}
