using ConsumerStreams.Application.UseCases;
using ConsumerStreams.Infrastructure.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ConsumerStreams.Worker;

/// <summary>
/// Servicio en segundo plano para la ejecución continua del pipeline de Kafka Streaming.
/// No posee valores quemados; lee la configuración validada en tiempo de inicio.
/// </summary>
public class StreamWorkerService(
    StreamProcessingPipelineUseCase pipelineUseCase,
    IOptions<KafkaStreamSettings> settingsOptions,
    ILogger<StreamWorkerService> logger) : BackgroundService
{
    private readonly KafkaStreamSettings _settings = settingsOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_settings.SourceTopic))
            throw new InvalidOperationException("[RUNTIME ERROR] SourceTopic no está configurado.");

        if (string.IsNullOrWhiteSpace(_settings.TargetTopic))
            throw new InvalidOperationException("[RUNTIME ERROR] TargetTopic no está configurado.");

        logger.LogInformation("🚀 [Kafka Streaming AOT] Iniciando procesador de flujo de eventos...");
        logger.LogInformation("   - Tópico Origen (Consumo): '{Source}'", _settings.SourceTopic);
        logger.LogInformation("   - Tópico Destino (Emisión): '{Target}'", _settings.TargetTopic);
        logger.LogInformation("   - Tópico Error / DLQ:       '{ErrorTopic}'", _settings.ErrorTopic);
        logger.LogInformation("   - Consumer Group:           '{Group}'", _settings.GroupId);
        logger.LogInformation("   - Servidores Bootstrap:     '{Servers}'", _settings.BootstrapServers);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await pipelineUseCase.ExecutePipelineAsync(
                    _settings.SourceTopic,
                    _settings.TargetTopic,
                    _settings.ErrorTopic,
                    stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                logger.LogInformation("Pipeline de streaming detenido de manera controlada.");
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error en el ciclo de streaming. Reintentando reconexión en 5 segundos...");
                try
                {
                    await Task.Delay(5000, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }
}
