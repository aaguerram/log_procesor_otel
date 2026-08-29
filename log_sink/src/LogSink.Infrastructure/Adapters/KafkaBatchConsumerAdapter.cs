using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using LogSink.Domain.Ports;
using LogSink.Infrastructure.Configuration;
using LogSink.Infrastructure.Logging;
using LogSink.Infrastructure.Messaging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LogSink.Infrastructure.Adapters;

/// <summary>
/// Adaptador de consumo por micro-lotes (hasta 500 mensajes) desde Kafka (30 particiones).
/// Acumula hasta 500 mensajes o ventana de 250 ms y ejecuta commit manual tras persistencia Bulk.
/// </summary>
public sealed class KafkaBatchConsumerAdapter : IBatchConsumerPort, IDisposable
{
    private readonly ILogger<KafkaBatchConsumerAdapter> _logger;
    private readonly IConsumer<string, byte[]> _consumer;
    private bool _disposed;

    public KafkaBatchConsumerAdapter(IOptions<SinkSettings> settingsOptions, ILogger<KafkaBatchConsumerAdapter> logger)
    {
        _logger = logger;
        var settings = settingsOptions.Value;

        var config = new ConsumerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            GroupId = settings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            SessionTimeoutMs = 15000,
            MaxPollIntervalMs = 300000
        };

        _consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) => InfrastructureLog.BatchConsumerError(_logger, e.Code, e.Reason))
            .SetPartitionsAssignedHandler((_, partitions) => LogPartitionsAssigned(partitions))
            .Build();

        InfrastructureLog.BatchConsumerInitialized(_logger, settings.GroupId, settings.BootstrapServers);
    }

    // El formateo de la lista de particiones sólo se ejecuta si el nivel Information está activo,
    // evitando la asignación del string cuando el log está deshabilitado (regla CA1873).
    private void LogPartitionsAssigned(List<TopicPartition> partitions)
    {
        if (_logger.IsEnabled(LogLevel.Information))
        {
            InfrastructureLog.PartitionsAssigned(_logger, string.Join(", ", partitions.Select(p => $"Part-{p.Partition.Value}")));
        }
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
        InfrastructureLog.BatchConsumerSubscribed(_logger, topic, maxBatchSize, maxWaitTime.TotalMilliseconds);

        var batchBuffer = new List<KafkaBatchItem>(maxBatchSize);
        var lastResults = new List<ConsumeResult<string, byte[]>>(maxBatchSize);
        var pollTimeout = TimeSpan.FromMilliseconds(50);
        var stopwatch = Stopwatch.StartNew();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(pollTimeout);
                Accumulate(result, batchBuffer, lastResults, stopwatch);

                if (ShouldFlush(batchBuffer, stopwatch, maxWaitTime, maxBatchSize))
                {
                    await FlushBatchAsync(batchBuffer, lastResults, onBatchReceived, stopwatch, cancellationToken);
                }
            }
            catch (ConsumeException ex)
            {
                InfrastructureLog.BatchConsumeError(_logger, ex, ex.Error.Reason);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                InfrastructureLog.BatchLoopUnexpected(_logger, ex);
                await Task.Delay(50, cancellationToken);
            }
        }

        InfrastructureLog.BatchConsumerStopped(_logger);
    }

    private static void Accumulate(
        ConsumeResult<string, byte[]>? result,
        List<KafkaBatchItem> batchBuffer,
        List<ConsumeResult<string, byte[]>> lastResults,
        Stopwatch stopwatch)
    {
        if (result is null || result.IsPartitionEOF || result.Message is null)
        {
            return;
        }

        if (batchBuffer.Count == 0) stopwatch.Restart();

        batchBuffer.Add(MapToBatchItem(result));
        lastResults.Add(result);
    }

    // Disparo de persistencia Bulk: si alcanzamos el tope de registros o si se vence la ventana con datos en buffer.
    private static bool ShouldFlush(List<KafkaBatchItem> batchBuffer, Stopwatch stopwatch, TimeSpan maxWaitTime, int maxBatchSize)
        => batchBuffer.Count >= maxBatchSize || (batchBuffer.Count > 0 && stopwatch.Elapsed >= maxWaitTime);

    private async Task FlushBatchAsync(
        List<KafkaBatchItem> batchBuffer,
        List<ConsumeResult<string, byte[]>> lastResults,
        Func<IReadOnlyList<KafkaBatchItem>, CancellationToken, Task<bool>> onBatchReceived,
        Stopwatch stopwatch,
        CancellationToken cancellationToken)
    {
        var success = await onBatchReceived(batchBuffer, cancellationToken);
        if (success && lastResults.Count > 0)
        {
            CommitHighestOffsets(lastResults);
        }

        batchBuffer.Clear();
        lastResults.Clear();
        stopwatch.Restart();
    }

    private void CommitHighestOffsets(List<ConsumeResult<string, byte[]>> lastResults)
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
            InfrastructureLog.OffsetCommitWarning(_logger, ex);
        }
    }

    private static KafkaBatchItem MapToBatchItem(ConsumeResult<string, byte[]> result)
    {
        return new KafkaBatchItem(
            Key: result.Message.Key ?? string.Empty,
            RawBytes: result.Message.Value,
            RawJson: Encoding.UTF8.GetString(result.Message.Value),
            Partition: result.Partition.Value,
            Offset: result.Offset.Value,
            Headers: KafkaHeaderMapper.ToDictionary(result.Message.Headers));
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
            _consumer.Close();
        }
        catch (Exception ex)
        {
            InfrastructureLog.BatchConsumerCloseFailed(_logger, ex);
        }

        _consumer.Dispose();
        GC.SuppressFinalize(this);
    }
}
