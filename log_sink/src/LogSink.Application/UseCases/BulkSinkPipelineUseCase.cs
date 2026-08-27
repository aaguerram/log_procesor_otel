using System.Diagnostics;
using LogSink.Domain.Ports;
using LogSink.Domain.Services;
using Microsoft.Extensions.Logging;

namespace LogSink.Application.UseCases;

/// <summary>
/// Caso de uso que orquesta el micro-batching de 500 documentos:
/// 1. Lectura de lotes de hasta 500 eventos desde Kafka (30 particiones).
/// 2. Resolución de la colección destino a partir de cabeceras.
/// 3. Inserción masiva paralela en Azure Cosmos DB / DocumentDB (Bulk Mode HTTP).
/// 4. Commit transaccional de offsets en Kafka.
/// </summary>
public class BulkSinkPipelineUseCase(
    IBatchConsumerPort consumerPort,
    IDocumentDbBulkSinkPort cosmosSinkPort,
    TargetCollectionResolver targetCollectionResolver,
    ILogger<BulkSinkPipelineUseCase> logger)
{
    public const int DefaultBatchSize = 500;
    public static readonly TimeSpan DefaultWaitWindow = TimeSpan.FromMilliseconds(250);

    public async Task ExecuteBulkSinkPipelineAsync(
        string sourceTopic,
        int batchSize,
        TimeSpan waitWindow,
        CancellationToken cancellationToken)
    {
        logger.LogInformation("Iniciando Bulk Sink Pipeline en .NET 10 Native AOT | Tópico: '{Topic}' | Lote Máx: {BatchSize} docs | Ventana: {WaitMs} ms",
            sourceTopic, batchSize, waitWindow.TotalMilliseconds);

        await consumerPort.StartBatchConsumerAsync(
            sourceTopic,
            batchSize,
            waitWindow,
            ProcessBatchAsync,
            cancellationToken);
    }

    private async Task<bool> ProcessBatchAsync(IReadOnlyList<KafkaBatchItem> batchItems, CancellationToken cancellationToken)
    {
        if (batchItems.Count == 0)
        {
            return true;
        }

        var stopwatch = Stopwatch.StartNew();
        var sinkItems = MapToSinkItems(batchItems);

        if (sinkItems.Count == 0)
        {
            return true;
        }

        var result = await cosmosSinkPort.BulkInsertRawJsonLogsAsync(sinkItems, cancellationToken);
        stopwatch.Stop();

        logger.LogInformation("💾 [Bulk Sink Cosmos DB] Persistidos: {Success}/{Total} docs | DLQ: {Dlq} docs | RUs: {RUs:F1} | Latencia Bulk: {Latency:F2} ms (Pipeline: {TotalMs:F2} ms)",
            result.TotalSuccessful, result.TotalProcessed, result.TotalDlqSent, result.RequestUnitsConsumed, result.ElapsedMilliseconds, stopwatch.Elapsed.TotalMilliseconds);

        // Retorna true para confirmar el commit de offsets en Kafka (los fallidos ya fueron dirigidos a DLQ).
        return true;
    }

    private List<LogSinkItem> MapToSinkItems(IReadOnlyList<KafkaBatchItem> batchItems)
    {
        var sinkItems = new List<LogSinkItem>(batchItems.Count);

        foreach (var item in batchItems)
        {
            if (string.IsNullOrWhiteSpace(item.RawJson))
            {
                continue;
            }

            sinkItems.Add(new LogSinkItem(
                RawJson: item.RawJson,
                PartitionKey: item.Key ?? "default",
                TargetCollection: targetCollectionResolver.Resolve(item.Headers)));
        }

        return sinkItems;
    }
}
