using System.Diagnostics;
using System.Text.Json;
using ConsumerStreams.Application.Logging;
using ConsumerStreams.Application.Serialization;
using ConsumerStreams.Application.Services;
using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Observability;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Domain.Security;
using ConsumerStreams.Domain.Utils;
using Microsoft.Extensions.Logging;
using Produbanco.Security.V1;

namespace ConsumerStreams.Application.UseCases;

/// <summary>
/// Orquesta el pipeline de streaming cifrado: consumo Protobuf ➔ descifrado AES-256-GCM ➔
/// enmascarado <c>x-log-data-protection</c> ➔ enriquecimiento / scoring ➔ reenvío del JSON en claro.
/// Los mensajes envenenados (poison pill) se derivan al tópico de error confirmando su offset.
/// La orquestación se apoya en servicios de dominio pequeños y verificables por separado.
/// </summary>
public class StreamProcessingPipelineUseCase(
    IStreamConsumerPort consumerPort,
    IStreamProducerPort producerPort,
    IDlqProducerPort dlqProducerPort,
    ITransactionTransformerPort transformer,
    EnvelopeDecryptionService decryptionService,
    TimeProvider timeProvider,
    ILogger<StreamProcessingPipelineUseCase> logger)
{
    private const string SourceTopicForHeaders = "tp.observability.application-log.emitted.v1";

    public Task ExecutePipelineAsync(string sourceTopic, string targetTopic, string errorTopic, CancellationToken cancellationToken)
    {
        PipelineLog.PipelineStarting(logger, sourceTopic, targetTopic, errorTopic);

        return consumerPort.StartStreamingAsync(
            sourceTopic,
            (key, rawBytes, headers, ct) => ProcessMessageAsync(key, rawBytes, headers, targetTopic, errorTopic, ct),
            cancellationToken);
    }

    private async Task<bool> ProcessMessageAsync(
        string? key,
        byte[] rawBytes,
        IDictionary<string, string>? headers,
        string targetTopic,
        string errorTopic,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        _ = EnvelopeParser.TryParse(rawBytes, out var envelope);

        try
        {
            var decoded = await decryptionService.DecryptAndMaskAsync(envelope, rawBytes, cancellationToken);
            var processedEvent = EnrichPayload(decoded.Json);

            var partitionKey = UniformPartitionKeyGenerator.GenerateDispersedKey(processedEvent.OriginAccount ?? key);
            var outboundHeaders = StreamHeaderFactory.ForProcessedEvent(
                AsReadOnly(headers), decoded.VaultToken, decoded.ServiceName, decoded.TelemetryLabel,
                TargetCollectionResolver.Resolve(decoded.ServiceName, decoded.TelemetryLabel), processedEvent);

            var published = await producerPort.ForwardEventAsync(targetTopic, partitionKey, decoded.Json, outboundHeaders, cancellationToken);
            stopwatch.Stop();

            if (published)
            {
                PipelineLog.EventProcessed(logger, new ProcessedEventLog(
                    processedEvent.TransactionId, processedEvent.Amount, processedEvent.RiskLevel,
                    processedEvent.FraudScore, stopwatch.Elapsed.TotalMilliseconds, targetTopic, partitionKey));
            }

            return published;
        }
        catch (Exception ex)
        {
            PipelineLog.EventProcessingFailed(logger, ex, key, errorTopic);
            await RouteToErrorTopicAsync(errorTopic, key, rawBytes, envelope, ex, headers, cancellationToken);

            // Confirmar offset para no bloquear la partición (Poison Pill Handling).
            return true;
        }
    }

    private ProcessedTransactionEvent EnrichPayload(string decryptedJson)
    {
        var rawEvent = JsonSerializer.Deserialize(decryptedJson, StreamJsonContext.Default.RawTransactionEvent)
            ?? throw new InvalidOperationException($"El mensaje no pudo ser parseado como RawTransactionEvent: {decryptedJson}");

        return transformer.TransformAndEnrich(rawEvent with { RawPayloadJson = decryptedJson });
    }

    private async Task RouteToErrorTopicAsync(
        string errorTopic,
        string? key,
        byte[] rawBytes,
        EncryptedPayloadEnvelope? envelope,
        Exception failure,
        IDictionary<string, string>? headers,
        CancellationToken cancellationToken)
    {
        try
        {
            var now = timeProvider.GetUtcNow();
            var errorEnvelope = DlqEnvelopeFactory.Create(
                envelope, rawBytes, key, failure, now, fallbackTransactionId: $"ERR-{Guid.NewGuid():N}");

            var errorHeaders = StreamHeaderFactory.ForError(AsReadOnly(headers), failure, SourceTopicForHeaders, now);

            await dlqProducerPort.PublishErrorEnvelopeAsync(errorTopic, key ?? errorEnvelope.TransactionId, errorEnvelope, errorHeaders, cancellationToken);
        }
        catch (Exception dlqFailure)
        {
            PipelineLog.DlqPublishFatal(logger, dlqFailure, errorTopic);
        }
    }

    private static Dictionary<string, string>? AsReadOnly(IDictionary<string, string>? headers)
        => headers is null ? null : new Dictionary<string, string>(headers);
}
