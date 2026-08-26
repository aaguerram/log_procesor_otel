using System.Text;
using Confluent.Kafka;
using KafkaDemo.Domain.Models;
using KafkaDemo.Domain.Ports;
using KafkaDemo.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Infrastructure.Adapters;

/// <summary>
/// Adaptador de infraestructura para el envío de mensajes (texto o binario Protobuf) mediante Confluent.Kafka.
/// </summary>
public class KafkaProducerAdapter : IMessageProducerPort, IDisposable
{
    private readonly IProducer<string, string> _stringProducer;
    private readonly IProducer<string, byte[]> _byteProducer;
    private readonly ILogger<KafkaProducerAdapter> _logger;
    private bool _disposed;

    public KafkaProducerAdapter(IOptions<KafkaSettings> settingsOptions, ILogger<KafkaProducerAdapter> logger)
    {
        _logger = logger;
        var settings = settingsOptions.Value;

        var acksEnum = settings.Acks?.ToLowerInvariant() switch
        {
            "0" or "none" => Acks.None,
            "1" or "leader" => Acks.Leader,
            _ => Acks.All
        };

        var config = new ProducerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            ClientId = settings.ClientId,
            Acks = acksEnum,
            EnableIdempotence = settings.EnableIdempotence,
            MessageTimeoutMs = settings.MessageTimeoutMs,
            RequestTimeoutMs = settings.RequestTimeoutMs,
            SocketTimeoutMs = settings.SocketTimeoutMs,
            MessageSendMaxRetries = settings.MessageSendMaxRetries,
            RetryBackoffMs = settings.RetryBackoffMs
        };

        _stringProducer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka String Producer Error [{Code}]: {Reason}", e.Code, e.Reason))
            .Build();

        _byteProducer = new ProducerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka Binary Producer Error [{Code}]: {Reason}", e.Code, e.Reason))
            .Build();

        _logger.LogInformation("Kafka Producer Adapter (Dual String/Binary) inicializado para servidores: {Servers}", settings.BootstrapServers);
    }

    public async Task<MessageResult> SendMessageAsync(KafkaMessage message, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var headers = new Headers();
        if (message.Headers != null)
        {
            foreach (var (key, value) in message.Headers)
            {
                if (value != null)
                {
                    headers.Add(key, Encoding.UTF8.GetBytes(value));
                }
            }
        }

        try
        {
            if (message.IsBinary && message.BinaryValue != null)
            {
                var byteMsg = new Message<string, byte[]>
                {
                    Key = message.Key ?? string.Empty,
                    Value = message.BinaryValue,
                    Headers = headers,
                    Timestamp = new Timestamp(message.Timestamp)
                };

                var deliveryReport = await _byteProducer.ProduceAsync(message.Topic, byteMsg, cancellationToken);
                _logger.LogInformation("Protobuf publicado exitosamente en '{Topic}' [Partición: {Partition}, Offset: {Offset}] ({Bytes} bytes)",
                    deliveryReport.Topic, deliveryReport.Partition.Value, deliveryReport.Offset.Value, message.BinaryValue.Length);

                return new MessageResult
                {
                    Topic = deliveryReport.Topic,
                    Partition = deliveryReport.Partition.Value,
                    Offset = deliveryReport.Offset.Value,
                    Status = deliveryReport.Status.ToString(),
                    Timestamp = deliveryReport.Timestamp.UtcDateTime,
                    Key = message.Key
                };
            }
            else
            {
                var strMsg = new Message<string, string>
                {
                    Key = message.Key ?? string.Empty,
                    Value = message.Value ?? string.Empty,
                    Headers = headers,
                    Timestamp = new Timestamp(message.Timestamp)
                };

                var deliveryReport = await _stringProducer.ProduceAsync(message.Topic, strMsg, cancellationToken);
                _logger.LogInformation("Mensaje publicado exitosamente en '{Topic}' [Partición: {Partition}, Offset: {Offset}]",
                    deliveryReport.Topic, deliveryReport.Partition.Value, deliveryReport.Offset.Value);

                return new MessageResult
                {
                    Topic = deliveryReport.Topic,
                    Partition = deliveryReport.Partition.Value,
                    Offset = deliveryReport.Offset.Value,
                    Status = deliveryReport.Status.ToString(),
                    Timestamp = deliveryReport.Timestamp.UtcDateTime,
                    Key = message.Key
                };
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error al producir mensaje en tópico '{Topic}'", message.Topic);
            throw new InvalidOperationException($"Fallo al enviar mensaje a Kafka ({ex.Message})", ex);
        }
    }

    public async Task<IReadOnlyList<MessageResult>> SendBatchAsync(IEnumerable<KafkaMessage> messages, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var results = new List<MessageResult>();
        foreach (var msg in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = await SendMessageAsync(msg, cancellationToken);
            results.Add(result);
        }

        _stringProducer.Flush(TimeSpan.FromSeconds(5));
        _byteProducer.Flush(TimeSpan.FromSeconds(5));
        return results;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                _stringProducer.Flush(TimeSpan.FromSeconds(5));
                _stringProducer.Dispose();
                _byteProducer.Flush(TimeSpan.FromSeconds(5));
                _byteProducer.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error durante la limpieza del Kafka Producer");
            }
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}
