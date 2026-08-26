using LogSink.Application.UseCases;
using LogSink.Infrastructure.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LogSink.Worker;

/// <summary>
/// Worker de servicio en segundo plano para consumo e inserción masiva en Azure Cosmos DB / DocumentDB.
/// Compilado como binario nativo C++/ELF en .NET 10 Native AOT.
/// </summary>
public class BulkSinkWorkerService(
    BulkSinkPipelineUseCase pipelineUseCase,
    IOptions<SinkSettings> settingsOptions,
    ILogger<BulkSinkWorkerService> logger) : BackgroundService
{
    private readonly SinkSettings _settings = settingsOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var sourceTopic = !string.IsNullOrWhiteSpace(_settings.SourceTopic)
            ? _settings.SourceTopic.Trim()
            : "produbanco-transactions-processed-v1";

        var batchSize = _settings.BatchSize > 0 ? _settings.BatchSize : 500;
        var timeoutMs = _settings.BatchTimeoutMs > 0 ? _settings.BatchTimeoutMs : 250;
        var waitWindow = TimeSpan.FromMilliseconds(timeoutMs);

        logger.LogInformation("🚀 [Cosmos DB Bulk Sink AOT] Iniciando servicio de persistencia masiva...");
        logger.LogInformation("   - Tópico Origen (Consumo):  '{Source}' (30 Particiones)", sourceTopic);
        logger.LogInformation("   - Tamaño de Lote (Bulk):    {BatchSize} documentos", batchSize);
        logger.LogInformation("   - Ventana de Espera Máx:    {Timeout} ms", timeoutMs);
        logger.LogInformation("   - Endpoint Cosmos DB:       '{Endpoint}'", _settings.CosmosEndpoint);
        logger.LogInformation("   - Base de Datos / Tabla:    '{Db}' / '{Coll}'", _settings.DatabaseName, _settings.ContainerName);

        try
        {
            await pipelineUseCase.ExecuteBulkSinkPipelineAsync(
                sourceTopic,
                batchSize,
                waitWindow,
                stoppingToken);
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Bulk Sink detenido adecuadamente por solicitud de cancelación.");
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Error fatal no controlado en Bulk Sink Worker Service");
            throw;
        }
    }
}
