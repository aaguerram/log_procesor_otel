using Confluent.Kafka;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Configuration;
using ConsumerStreams.Infrastructure.Messaging;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Produbanco.Security.V1;

namespace ConsumerStreams.Infrastructure.Adapters;

/// <summary>
/// Adaptador de salida dedicado a publicar sobres <see cref="EncryptedErrorPayloadEnvelope"/>
/// binarios en el tópico de error / DLQ.
/// </summary>
public sealed class KafkaDlqProducerAdapter : IDlqProducerPort, IDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly ILogger<KafkaDlqProducerAdapter> _logger;
    private bool _disposed;

    public KafkaDlqProducerAdapter(IOptions<KafkaStreamSettings> settingsOptions, ILogger<KafkaDlqProducerAdapter> logger)
    {
        _logger = logger;
        var settings = settingsOptions.Value;

        var config = new ProducerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            ClientId = "ConsumerStreams-DlqProducer-AOT",
            Acks = Acks.All,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogError("DLQ Producer Kafka Error [{Code}]: {Reason}", e.Code, e.Reason))
            .Build();

        _logger.LogInformation("DLQ Producer Adapter inicializado para bootstrap servers: {Servers}", settings.BootstrapServers);
    }

    public async Task<bool> PublishErrorEnvelopeAsync(
        string errorTopic,
        string? key,
        EncryptedErrorPayloadEnvelope errorEnvelope,
        IDictionary<string, string>? headers,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var message = new Message<string, byte[]>
        {
            Key = key ?? string.Empty,
            Value = errorEnvelope.ToByteArray(),
            Headers = KafkaHeaderMapper.ToKafkaHeaders(headers),
            Timestamp = new Timestamp(DateTime.UtcNow)
        };

        try
        {
            var report = await _producer.ProduceAsync(errorTopic, message, cancellationToken);
            _logger.LogInformation("⚠️ [DLQ/ERROR PRODUCED] Sobre Protobuf con error publicado en '{Topic}' [Partición {Partition}, Offset {Offset}]",
                report.Topic, report.Partition.Value, report.Offset.Value);
            return true;
        }
        catch (ProduceException<string, byte[]> ex)
        {
            _logger.LogError(ex, "Fallo al publicar sobre Protobuf en cola de error '{Topic}': {Reason}", errorTopic, ex.Error.Reason);
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
            _logger.LogWarning(ex, "Error cerrando el DLQ producer de Kafka");
        }

        GC.SuppressFinalize(this);
    }
}
