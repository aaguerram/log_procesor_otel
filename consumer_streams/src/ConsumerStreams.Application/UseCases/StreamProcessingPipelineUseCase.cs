using System.Diagnostics;
using System.Text;
using System.Text.Json;
using ConsumerStreams.Application.Serialization;
using ConsumerStreams.Domain.DataProtection;
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
    IVaultTokenProviderPort vaultTokenPort,
    IPayloadCryptoPort cryptoPort,
    PayloadMaskingService maskingService,
    TimeProvider timeProvider,
    ILogger<StreamProcessingPipelineUseCase> logger)
{
    private const string SourceTopicForHeaders = "tp.observability.application-log.emitted.v1";

    public Task ExecutePipelineAsync(string sourceTopic, string targetTopic, string errorTopic, CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Iniciando pipeline de streaming reactivo cifrado Protobuf: '{Source}' ➔ '{Target}' | DLQ Error: '{ErrorTopic}'",
            sourceTopic, targetTopic, errorTopic);

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
        EnvelopeParser.TryParse(rawBytes, out var envelope);

        try
        {
            var decoded = await DecryptAndMaskAsync(envelope, rawBytes, cancellationToken);
            var processedEvent = EnrichPayload(decoded.Json);

            var partitionKey = UniformPartitionKeyGenerator.GenerateDispersedKey(processedEvent.OriginAccount ?? key);
            var outboundHeaders = StreamHeaderFactory.ForProcessedEvent(
                AsReadOnly(headers), decoded.VaultToken, decoded.ServiceName, decoded.TelemetryLabel,
                TargetCollectionResolver.Resolve(decoded.ServiceName, decoded.TelemetryLabel), processedEvent);

            var published = await producerPort.ForwardEventAsync(targetTopic, partitionKey, decoded.Json, outboundHeaders, cancellationToken);
            stopwatch.Stop();

            if (published)
            {
                logger.LogInformation(
                    "✔ [AES-GCM Decrypted & Processed] Txn: {TxnId} | Monto: ${Amount} | Riesgo: {Risk} ({Score} pts) | Pipeline: {Elapsed:F2} ms ➔ '{Target}' [DispersedKey: {Key}]",
                    processedEvent.TransactionId, processedEvent.Amount, processedEvent.RiskLevel,
                    processedEvent.FraudScore, stopwatch.Elapsed.TotalMilliseconds, targetTopic, partitionKey);
            }

            return published;
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "❌ Error procesando evento en streaming para key '{Key}'. Redirigiendo a cola DLQ/Error: '{ErrorTopic}'", key, errorTopic);
            await RouteToErrorTopicAsync(errorTopic, key, rawBytes, envelope, ex, headers, cancellationToken);

            // Confirmar offset para no bloquear la partición (Poison Pill Handling).
            return true;
        }
    }

    /// <summary>Descifra el sobre (o toma el texto plano legacy) y aplica el enmascarado si procede.</summary>
    private async Task<DecodedMessage> DecryptAndMaskAsync(
        EncryptedPayloadEnvelope? envelope,
        byte[] rawBytes,
        CancellationToken cancellationToken)
    {
        if (envelope is null)
        {
            return new DecodedMessage(Encoding.UTF8.GetString(rawBytes), "Transfer.Mspx.Prometeus.Management", "Trace", "NONE");
        }

        EnvelopeValidator.Validate(envelope);

        var keyMaterial = await vaultTokenPort.ResolveKeyByTokenAsync(envelope.VaultTokenId, envelope.CertThumbprint, cancellationToken);
        var decryptedJson = cryptoPort.DecryptEnvelopeToJson(envelope, keyMaterial);
        decryptedJson = maskingService.ApplyIfApplicable(envelope, decryptedJson);

        return new DecodedMessage(
            decryptedJson,
            envelope.ServiceName,
            TelemetryTypeMapper.ToLabel(envelope.TelemetryType),
            envelope.VaultTokenId);
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
            logger.LogCritical(dlqFailure, "❌ [FATAL DLQ ERROR] Fallo crítico al publicar en la cola de error '{ErrorTopic}'", errorTopic);
        }
    }

    private static IReadOnlyDictionary<string, string>? AsReadOnly(IDictionary<string, string>? headers)
        => headers is null ? null : new Dictionary<string, string>(headers);

    private readonly record struct DecodedMessage(string Json, string ServiceName, string TelemetryLabel, string VaultToken);
}
