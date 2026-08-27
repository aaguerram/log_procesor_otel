using ConsumerStreams.Application.UseCases;
using ConsumerStreams.Infrastructure.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ConsumerStreams.Worker;

/// <summary>
/// Servicio en segundo plano para la ejecución continua del pipeline de Kafka Streaming con tópicos por defecto.
/// </summary>
public class StreamWorkerService(
    StreamProcessingPipelineUseCase pipelineUseCase,
    IOptions<KafkaStreamSettings> settingsOptions,
    ILogger<StreamWorkerService> logger) : BackgroundService
{
    public const string DefaultSourceTopic = "tp.observability.application-log.emitted.v1";
    public const string DefaultTargetTopic = "tp.observability.application-log.processed.v1";

    private readonly KafkaStreamSettings _settings = settingsOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Resolución de tópicos con valores por defecto garantizados
        var sourceTopic = !string.IsNullOrWhiteSpace(_settings.SourceTopic)
            ? _settings.SourceTopic.Trim()
            : DefaultSourceTopic;

        var targetTopic = !string.IsNullOrWhiteSpace(_settings.TargetTopic)
            ? _settings.TargetTopic.Trim()
            : DefaultTargetTopic;

        var consumerGroup = !string.IsNullOrWhiteSpace(_settings.GroupId)
            ? _settings.GroupId.Trim()
            : "consumer-streams-default-group";

        logger.LogInformation("🚀 [Kafka Streaming AOT] Iniciando procesador de flujo de eventos...");
        logger.LogInformation("   - Tópico Origen (Consumo): '{Source}' {SourceNote}",
            sourceTopic, sourceTopic == DefaultSourceTopic ? "[Por Defecto]" : "[Configurado]");
        logger.LogInformation("   - Tópico Destino (Emisión): '{Target}' {TargetNote}",
            targetTopic, targetTopic == DefaultTargetTopic ? "[Por Defecto]" : "[Configurado]");
        logger.LogInformation("   - Consumer Group:           '{Group}'", consumerGroup);
        logger.LogInformation("   - Servidores Bootstrap:     '{Servers}'", _settings.BootstrapServers);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await pipelineUseCase.ExecutePipelineAsync(
                    sourceTopic,
                    targetTopic,
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
