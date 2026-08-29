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
        if (string.IsNullOrWhiteSpace(_settings.SourceTopic))
            throw new InvalidOperationException("[RUNTIME ERROR] LogSink:SourceTopic no está configurado.");

        var batchSize = _settings.BatchSize > 0 ? _settings.BatchSize : 500;
        var timeoutMs = _settings.BatchTimeoutMs > 0 ? _settings.BatchTimeoutMs : 250;
        var waitWindow = TimeSpan.FromMilliseconds(timeoutMs);

        WorkerLog.BulkSinkStarting(
            logger, _settings.SourceTopic, batchSize, timeoutMs,
            _settings.CosmosEndpoint, _settings.DatabaseName, _settings.ContainerName);

        try
        {
            await pipelineUseCase.ExecuteBulkSinkPipelineAsync(
                _settings.SourceTopic,
                batchSize,
                waitWindow,
                stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Cancelación esperada durante el apagado controlado del host.
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Error fatal no controlado en Bulk Sink Worker Service", ex);
        }

        WorkerLog.BulkSinkStopped(logger);
    }
}
