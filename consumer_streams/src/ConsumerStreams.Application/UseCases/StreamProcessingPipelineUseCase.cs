using System.Diagnostics;
using System.Text;
using System.Text.Json;
using ConsumerStreams.Application.Serialization;
using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Domain.Utils;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Produbanco.Security.V1;

namespace ConsumerStreams.Application.UseCases;

/// <summary>
/// Caso de uso que orquesta el pipeline de streaming:
/// 1. Consumo reactivo de Protobuf binario desde 40 particiones.
/// 2. Resolución de tokens de Azure Key Vault con caché TTL de 1 hora.
/// 3. Descifrado AES-256-GCM por hardware (AES-NI).
/// 4. Enriquecimiento de Dominio.
/// 5. Publicación del JSON en claro con claves SplitMix64 en 30 particiones uniformes.
/// </summary>
public class StreamProcessingPipelineUseCase(
    IStreamConsumerPort consumerPort,
    IStreamProducerPort producerPort,
    ITransactionTransformerPort transformer,
    IVaultTokenProviderPort vaultTokenPort,
    IPayloadCryptoPort cryptoPort,
    IContractRulesCachePort contractRulesCache,
    ConsumerStreams.Domain.Configuration.DataProtectionRulesSettings dataProtectionSettings,
    ILogger<StreamProcessingPipelineUseCase> logger)
{
    public async Task ExecutePipelineAsync(
        string sourceTopic,
        string targetTopic,
        string errorTopic,
        CancellationToken cancellationToken)
    {
        logger.LogInformation("Iniciando pipeline de streaming reactivo cifrado Protobuf: '{Source}' (40 part.) ➔ '{Target}' (30 part.) | DLQ Error: '{ErrorTopic}'",
            sourceTopic, targetTopic, errorTopic);

        await consumerPort.StartStreamingAsync(sourceTopic, async (key, rawBytes, headers, ct) =>
        {
            var stopwatch = Stopwatch.StartNew();
            EncryptedPayloadEnvelope? currentEnvelope = null;

            try
            {
                string decryptedJson;
                string? tokenUsed = null;

                // 1. Intentar deserializar como Protobuf EncryptedPayloadEnvelope Autosuficiente
                string serviceName = "Transfer.Mspx.Prometeus.Management";
                string telemetryTypeStr = "Trace";

                try
                {
                    currentEnvelope = EncryptedPayloadEnvelope.Parser.ParseFrom(rawBytes);
                }
                catch (InvalidProtocolBufferException)
                {
                    // Si viene en texto JSON plano legacy
                    currentEnvelope = null;
                }

                if (currentEnvelope != null)
                {
                    // 1.1 Validación estricta: Todos los campos del proto excepto Swagger y ErrorDetail son obligatorios
                    ValidateMandatoryEnvelopeFields(currentEnvelope);

                    tokenUsed = currentEnvelope.VaultTokenId;
                    serviceName = currentEnvelope.ServiceName;

                    telemetryTypeStr = currentEnvelope.TelemetryType switch
                    {
                        TelemetryType.Trace => "Trace",
                        TelemetryType.Metric => "Metric",
                        TelemetryType.Log => "Log",
                        _ => "Trace"
                    };

                    // 2. Resolver la clave de Azure Key Vault a partir del token (Caché RAM TTL 1 hora)
                    var keyMaterial = await vaultTokenPort.ResolveKeyByTokenAsync(
                        currentEnvelope.VaultTokenId,
                        currentEnvelope.CertThumbprint,
                        ct);

                    // 3. Descifrado AES-256-GCM por hardware en CPU (~0.15 ms)
                    decryptedJson = cryptoPort.DecryptEnvelopeToJson(currentEnvelope, keyMaterial);

                    // 4. Aplicar políticas x-log-data-protection ÚNICAMENTE si el mensaje es de tipo TRACE
                    if (currentEnvelope.TelemetryType == TelemetryType.Trace && !string.IsNullOrEmpty(currentEnvelope.Swagger) && dataProtectionSettings.Enabled)
                    {
                        var rules = contractRulesCache.GetOrCompile(currentEnvelope.Swagger);
                        var maskedBytes = JsonStreamDataProtectionMasker.MaskPayload(
                            Encoding.UTF8.GetBytes(decryptedJson),
                            rules,
                            dataProtectionSettings);
                        decryptedJson = Encoding.UTF8.GetString(maskedBytes);
                    }
                }
                else
                {
                    // Fallback a texto UTF-8 plano
                    decryptedJson = Encoding.UTF8.GetString(rawBytes);
                }

                // 5. Deserialización segura Native AOT del JSON descifrado
                var rawEvent = JsonSerializer.Deserialize(decryptedJson, StreamJsonContext.Default.RawTransactionEvent);
                if (rawEvent == null)
                {
                    throw new InvalidOperationException($"El mensaje no pudo ser parseado como RawTransactionEvent: {decryptedJson}");
                }

                // Asignar el payload original completo
                rawEvent = rawEvent with { RawPayloadJson = decryptedJson };

                // 6. Transformación y lógica de negocio de dominio
                var processedEvent = transformer.TransformAndEnrich(rawEvent);

                // 7. Serialización segura Native AOT del JSON enriquecido en claro
                var processedJson = JsonSerializer.Serialize(processedEvent, StreamJsonContext.Default.ProcessedTransactionEvent);

                // 8. Enriquecimiento de cabeceras de trazabilidad, servicio, telemetría y seguridad
                string sanitizedServiceName = serviceName.Replace('.', '_');
                string targetCollection = $"{sanitizedServiceName}_{telemetryTypeStr}";

                var enrichedHeaders = new Dictionary<string, string>(headers ?? new Dictionary<string, string>())
                {
                    ["x-stream-processor"] = "ConsumerStreams.NativeAOT",
                    ["x-decryption-algorithm"] = "AES-256-GCM",
                    ["x-vault-token"] = tokenUsed ?? "NONE",
                    ["x-service-name"] = serviceName,
                    ["x-telemetry-type"] = telemetryTypeStr,
                    ["x-target-collection"] = targetCollection,
                    ["x-processed-status"] = processedEvent.ProcessedStatus ?? "UNKNOWN",
                    ["x-risk-level"] = processedEvent.RiskLevel ?? "LOW",
                    ["x-latency-ms"] = processedEvent.ProcessingLatencyMs.ToString("F2")
                };

                // 9. Reenvío del JSON en claro al tópico de destino (30 particiones balanceadas por SplitMix64)
                var partitionKey = UniformPartitionKeyGenerator.GenerateDispersedKey(processedEvent.OriginAccount ?? key);

                var published = await producerPort.ForwardEventAsync(
                    targetTopic,
                    partitionKey,
                    decryptedJson,
                    enrichedHeaders,
                    ct);

                stopwatch.Stop();

                if (published)
                {
                    logger.LogInformation("✔ [AES-GCM Decrypted & Processed] Txn: {TxnId} | Monto: ${Amount} | Riesgo: {Risk} ({Score} pts) | Pipeline: {Elapsed:F2} ms ➔ '{Target}' [DispersedKey: {Key}]",
                        processedEvent.TransactionId, processedEvent.Amount, processedEvent.RiskLevel, processedEvent.FraudScore, stopwatch.Elapsed.TotalMilliseconds, targetTopic, partitionKey);
                }

                return published;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "❌ Error procesando evento en streaming para key '{Key}'. Redirigiendo a cola DLQ/Error: '{ErrorTopic}'", key, errorTopic);
                await SendToDlqErrorTopicAsync(errorTopic, key, rawBytes, currentEnvelope, ex, headers, ct);
                // Retornar true para confirmar offset en Kafka y evitar bloqueo de la partición (Poison Pill Handling)
                return true;
            }
        }, cancellationToken);
    }

    private async Task<bool> SendToDlqErrorTopicAsync(
        string errorTopic,
        string? key,
        byte[] rawBytes,
        EncryptedPayloadEnvelope? envelope,
        Exception ex,
        IDictionary<string, string>? headers,
        CancellationToken ct)
    {
        try
        {
            var dlqEnvelope = new EncryptedErrorPayloadEnvelope
            {
                Data = envelope != null ? envelope.Data : ByteString.CopyFrom(rawBytes),
                Nonce = envelope != null ? envelope.Nonce : ByteString.CopyFrom(new byte[12]),
                AuthTag = envelope != null ? envelope.AuthTag : ByteString.CopyFrom(new byte[16]),
                AlgorithmVersion = envelope?.AlgorithmVersion ?? 1,
                CertThumbprint = envelope?.CertThumbprint ?? "NONE",
                VaultTokenId = envelope?.VaultTokenId ?? "NONE",
                TransactionId = envelope?.TransactionId ?? (key ?? $"ERR-{Guid.NewGuid():N}"),
                TimestampUnixMs = envelope?.TimestampUnixMs ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Swagger = envelope?.Swagger ?? string.Empty,
                TelemetryType = envelope?.TelemetryType ?? TelemetryType.Log,
                ServiceName = envelope?.ServiceName ?? "Unknown.Service",
                ErrorDetail = $"{ex.GetType().FullName}: {ex.Message}\n{ex.StackTrace}"
            };

            var errorHeaders = new Dictionary<string, string>(headers ?? new Dictionary<string, string>())
            {
                ["x-error-type"] = ex.GetType().Name,
                ["x-error-message"] = ex.Message,
                ["x-error-timestamp"] = DateTime.UtcNow.ToString("O"),
                ["x-source-topic"] = "tp.observability.application-log.emitted.v1",
                ["x-error-handler"] = "ConsumerStreams.DLQ"
            };

            byte[] dlqBytes = dlqEnvelope.ToByteArray();
            return await producerPort.ForwardProtobufAsync(
                errorTopic,
                key ?? dlqEnvelope.TransactionId,
                dlqBytes,
                errorHeaders,
                ct);
        }
        catch (Exception dlqEx)
        {
            logger.LogCritical(dlqEx, "❌ [FATAL DLQ ERROR] Fallo crítico al publicar en la cola de error '{ErrorTopic}'", errorTopic);
            return false;
        }
    }

    private static void ValidateMandatoryEnvelopeFields(EncryptedPayloadEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope, nameof(envelope));

        if (envelope.Data == null || envelope.Data.Length == 0)
            throw new InvalidOperationException("El campo 'data' del sobre protobuf es obligatorio y no puede estar vacío.");

        if (envelope.Nonce == null || envelope.Nonce.Length != 12)
            throw new InvalidOperationException($"El campo 'nonce' es obligatorio y debe tener exactamente 12 bytes (recibido: {envelope.Nonce?.Length ?? 0}).");

        if (envelope.AuthTag == null || envelope.AuthTag.Length != 16)
            throw new InvalidOperationException($"El campo 'auth_tag' es obligatorio y debe tener exactamente 16 bytes (recibido: {envelope.AuthTag?.Length ?? 0}).");

        if (envelope.AlgorithmVersion <= 0)
            throw new InvalidOperationException("El campo 'algorithm_version' es obligatorio y debe ser mayor a 0 (1 = AES-256-GCM).");

        if (string.IsNullOrWhiteSpace(envelope.CertThumbprint))
            throw new InvalidOperationException("El campo 'cert_thumbprint' es obligatorio.");

        if (string.IsNullOrWhiteSpace(envelope.VaultTokenId))
            throw new InvalidOperationException("El campo 'vault_token_id' es obligatorio.");

        if (string.IsNullOrWhiteSpace(envelope.TransactionId))
            throw new InvalidOperationException("El campo 'transaction_id' es obligatorio.");

        if (envelope.TimestampUnixMs <= 0)
            throw new InvalidOperationException("El campo 'timestamp_unix_ms' es obligatorio y debe ser mayor a 0.");

        if (envelope.TelemetryType == TelemetryType.Unspecified)
            throw new InvalidOperationException("El campo 'telemetry_type' es obligatorio y debe ser Trace (1), Metric (2) o Log (3).");

        if (string.IsNullOrWhiteSpace(envelope.ServiceName))
            throw new InvalidOperationException("El campo 'service_name' es obligatorio.");

        // NOTA: envelope.Swagger es el ÚNICO campo opcional permitido.
    }
}
