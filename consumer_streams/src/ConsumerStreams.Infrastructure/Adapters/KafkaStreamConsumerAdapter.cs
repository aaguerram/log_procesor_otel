using Confluent.Kafka;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Configuration;
using ConsumerStreams.Infrastructure.Messaging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ConsumerStreams.Infrastructure.Adapters;

/// <summary>
/// Adaptador de entrada (Source Adapter) para el consumo continuo y reactivo de mensajes binarios Protobuf desde Kafka.
/// </summary>
public class KafkaStreamConsumerAdapter : IStreamConsumerPort, IDisposable
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly KafkaStreamSettings _settings;
    private readonly ILogger<KafkaStreamConsumerAdapter> _logger;
    private bool _disposed;

    public KafkaStreamConsumerAdapter(IOptions<KafkaStreamSettings> settingsOptions, ILogger<KafkaStreamConsumerAdapter> logger)
    {
        _logger = logger;
        _settings = settingsOptions.Value;

        var autoOffsetReset = _settings.AutoOffsetReset?.ToLowerInvariant() switch
        {
            "latest" => AutoOffsetReset.Latest,
            _ => AutoOffsetReset.Earliest
        };

        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.GroupId,
            AutoOffsetReset = autoOffsetReset,
            EnableAutoCommit = _settings.EnableAutoCommit,
            EnableAutoOffsetStore = false,
            SessionTimeoutMs = 15000,
            MaxPollIntervalMs = 300000
        };

        _consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Source Consumer Kafka Error [{Code}]: {Reason}", e.Code, e.Reason))
            .Build();

        _logger.LogInformation("Source Consumer Adapter (Protobuf Binary) inicializado para grupo '{Group}' en servidores: {Servers}",
            _settings.GroupId, _settings.BootstrapServers);
    }

    public async Task StartStreamingAsync(
        string sourceTopic,
        Func<string?, byte[], IDictionary<string, string>?, CancellationToken, Task<bool>> onMessageReceived,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _consumer.Subscribe(sourceTopic);
        _logger.LogInformation("Suscrito exitosamente al flujo binario del tópico de origen: '{Topic}'", sourceTopic);

        var pollTimeout = TimeSpan.FromMilliseconds(_settings.PollTimeoutMs);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(pollTimeout);
                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    continue;
                }

                var headers = KafkaHeaderMapper.ToDictionary(consumeResult.Message.Headers);

                // Invocación del procesamiento reactivo con los bytes binarios
                var success = await onMessageReceived(
                    consumeResult.Message.Key,
                    consumeResult.Message.Value,
                    headers,
                    cancellationToken);

                if (success)
                {
                    // Commit manual del offset tras confirmación
                    _consumer.Commit(consumeResult);
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Error consumiendo evento de Kafka: {Reason}", ex.Error.Reason);
                await Task.Delay(1000, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Excepción inesperada en el ciclo de consumo del stream.");
                await Task.Delay(1000, cancellationToken);
            }
        }

        _logger.LogInformation("Ciclo de consumo de stream finalizado para '{Topic}'", sourceTopic);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                _consumer.Close();
                _consumer.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error cerrando el source consumer de Kafka");
            }
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}
