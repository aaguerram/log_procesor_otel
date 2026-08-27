using System.Diagnostics;
using System.Text.Json;
using LogSink.Application.Serialization;
using LogSink.Domain.Models;
using LogSink.Domain.Ports;
using Microsoft.Extensions.Logging;

namespace LogSink.Application.UseCases;

/// <summary>
/// Caso de uso que orquesta el micro-batching de 500 documentos:
/// 1. Lectura de lotes de hasta 500 eventos desde Kafka (30 particiones).
/// 2. Deserialización Native AOT sin reflexión.
/// 3. Inserción masiva paralela en Azure Cosmos DB / DocumentDB (Bulk Mode HTTP/2).
/// 4. Commit transaccional de offsets en Kafka.
/// </summary>
public class BulkSinkPipelineUseCase(
    IBatchConsumerPort consumerPort,
    IDocumentDbBulkSinkPort cosmosSinkPort,
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
            async (batchItems, ct) =>
            {
                if (batchItems.Count == 0) return true;

                var stopwatch = Stopwatch.StartNew();
                var rawItems = new List<LogSinkItem>(batchItems.Count);

                foreach (var item in batchItems)
                {
                    if (!string.IsNullOrWhiteSpace(item.RawJson))
                    {
                        string? targetCollection = null;
                        if (item.Headers.TryGetValue("x-target-collection", out var col) && !string.IsNullOrWhiteSpace(col))
                        {
                            targetCollection = col;
                        }
                        else if (item.Headers.TryGetValue("x-service-name", out var sName) && item.Headers.TryGetValue("x-telemetry-type", out var tType))
                        {
                            string sanitized = sName.Replace('.', '_');
                            targetCollection = $"{sanitized}_{tType}";
                        }

                        rawItems.Add(new LogSinkItem(
                            RawJson: item.RawJson,
                            PartitionKey: item.Key ?? "default",
                            TargetCollection: targetCollection));
                    }
                }

                if (rawItems.Count == 0) return true;

                // 2. Inserción masiva en modo Bulk en Cosmos DB con el JSON exacto recibido
                var result = await cosmosSinkPort.BulkInsertRawJsonLogsAsync(rawItems, ct);
                stopwatch.Stop();

                logger.LogInformation("💾 [Bulk Sink Cosmos DB] Persistidos: {Success}/{Total} docs | RUs: {RUs:F1} | Latencia Bulk: {Latency:F2} ms (Pipeline: {TotalMs:F2} ms)",
                    result.TotalSuccessful, result.TotalProcessed, result.RequestUnitsConsumed, result.ElapsedMilliseconds, stopwatch.Elapsed.TotalMilliseconds);

                // Retorna true para confirmar el commit de offsets en Kafka
                return result.TotalSuccessful > 0 || result.TotalFailed == 0;
            },
            cancellationToken);
    }
}
