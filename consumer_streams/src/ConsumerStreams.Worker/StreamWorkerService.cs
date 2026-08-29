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

        WorkerLog.StreamingProcessorStarted(
            logger, _settings.SourceTopic, _settings.TargetTopic, _settings.ErrorTopic, _settings.GroupId, _settings.BootstrapServers);

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
                break;
            }
            catch (Exception ex)
            {
                WorkerLog.StreamingCycleError(logger, ex);
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

        WorkerLog.PipelineStopped(logger);
    }
}
