using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using LogSink.Domain.Ports;
using LogSink.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LogSink.Infrastructure.Adapters;

/// <summary>
/// Adaptador de consumo por micro-lotes (hasta 500 mensajes) desde Kafka (30 particiones).
/// Acumula hasta 500 mensajes o ventana de 250 ms y ejecuta commit manual tras persistencia Bulk.
/// </summary>
public class KafkaBatchConsumerAdapter : IBatchConsumerPort, IDisposable
{
    private readonly SinkSettings _settings;
    private readonly ILogger<KafkaBatchConsumerAdapter> _logger;
    private readonly IConsumer<string, byte[]> _consumer;
    private bool _disposed;

    public KafkaBatchConsumerAdapter(IOptions<SinkSettings> settings, ILogger<KafkaBatchConsumerAdapter> logger)
    {
        _settings = settings.Value;
        _logger = logger;

        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            SessionTimeoutMs = 15000,
            MaxPollIntervalMs = 300000
        };

        _consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Error en Kafka Batch Consumer [{Code}]: {Reason}", e.Code, e.Reason))
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation("✔ Particiones asignadas a Batch Consumer: [{Partitions}]",
                    string.Join(", ", partitions.Select(p => $"Part-{p.Partition.Value}")));
            })
            .Build();

        _logger.LogInformation("Batch Consumer Adapter inicializado para grupo '{Group}' en servidores: {Servers}",
            _settings.GroupId, _settings.BootstrapServers);
    }

    public async Task StartBatchConsumerAsync(
        string topic,
        int maxBatchSize,
        TimeSpan maxWaitTime,
        Func<IReadOnlyList<KafkaBatchItem>, CancellationToken, Task<bool>> onBatchReceived,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _consumer.Subscribe(topic);
        _logger.LogInformation("Suscrito exitosamente a 30 particiones del tópico: '{Topic}' (Lote Máx: {BatchSize}, Ventana: {WaitMs} ms)",
            topic, maxBatchSize, maxWaitTime.TotalMilliseconds);

        var batchBuffer = new List<KafkaBatchItem>(maxBatchSize);
        var lastResults = new List<ConsumeResult<string, byte[]>>(maxBatchSize);
        var pollTimeout = TimeSpan.FromMilliseconds(50);
        var stopwatch = Stopwatch.StartNew();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(pollTimeout);
                if (result != null && !result.IsPartitionEOF && result.Message != null)
                {
                    if (batchBuffer.Count == 0) stopwatch.Restart();

                    batchBuffer.Add(MapToBatchItem(result));
                    lastResults.Add(result);
                }

                // Disparo de persistencia Bulk: si alcanzamos 500 registros o si se vence la ventana con datos en buffer
                if (batchBuffer.Count >= maxBatchSize || (batchBuffer.Count > 0 && stopwatch.Elapsed >= maxWaitTime))
                {
                    var success = await onBatchReceived(batchBuffer, cancellationToken);
                    if (success && lastResults.Count > 0)
                    {
                        try
                        {
                            var highestOffsets = lastResults
                                .GroupBy(r => r.TopicPartition)
                                .Select(g => new TopicPartitionOffset(g.Key, new Offset(g.Max(r => r.Offset.Value) + 1)));

                            _consumer.Commit(highestOffsets);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Advertencia al hacer commit de offsets en Kafka");
                        }
                    }

                    batchBuffer.Clear();
                    lastResults.Clear();
                    stopwatch.Restart();
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Error consumiendo de Kafka: {Reason}", ex.Error.Reason);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error inesperado en Batch Consumer Loop");
                await Task.Delay(50, cancellationToken);
            }
        }

        _logger.LogInformation("Batch Consumer detenido correctamente.");
    }

    private static KafkaBatchItem MapToBatchItem(ConsumeResult<string, byte[]> result)
    {
        var headersDict = new Dictionary<string, string>();
        if (result.Message.Headers != null)
        {
            foreach (var h in result.Message.Headers)
            {
                headersDict[h.Key] = Encoding.UTF8.GetString(h.GetValueBytes());
            }
        }

        var json = Encoding.UTF8.GetString(result.Message.Value);

        return new KafkaBatchItem(
            Key: result.Message.Key ?? string.Empty,
            RawBytes: result.Message.Value,
            RawJson: json,
            Partition: result.Partition.Value,
            Offset: result.Offset.Value,
            Headers: headersDict);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            try { _consumer.Close(); } catch { }
            _consumer.Dispose();
        }
    }
}
