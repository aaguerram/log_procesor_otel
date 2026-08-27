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
    ILogger<StreamProcessingPipelineUseCase> logger)
{
    public async Task ExecutePipelineAsync(string sourceTopic, string targetTopic, CancellationToken cancellationToken)
    {
        logger.LogInformation("Iniciando pipeline de streaming reactivo cifrado Protobuf: '{Source}' (40 part.) ➔ '{Target}' (30 part.)", sourceTopic, targetTopic);

        await consumerPort.StartStreamingAsync(sourceTopic, async (key, rawBytes, headers, ct) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                string decryptedJson;
                string? tokenUsed = null;

                // 1. Intentar deserializar como Protobuf EncryptedPayloadEnvelope Autosuficiente
                try
                {
                    var envelope = EncryptedPayloadEnvelope.Parser.ParseFrom(rawBytes);
                    if (envelope != null && !string.IsNullOrEmpty(envelope.VaultTokenId) && envelope.Data.Length > 0)
                    {
                        tokenUsed = envelope.VaultTokenId;

                        // 2. Resolver la clave de Azure Key Vault a partir del token (Caché RAM TTL 1 hora)
                        var keyMaterial = await vaultTokenPort.ResolveKeyByTokenAsync(
                            envelope.VaultTokenId,
                            envelope.CertThumbprint,
                            ct);

                        // 3. Descifrado AES-256-GCM por hardware en CPU (~0.15 ms)
                        decryptedJson = cryptoPort.DecryptEnvelopeToJson(envelope, keyMaterial);
                    }
                    else
                    {
                        // Fallback a texto UTF-8 plano
                        decryptedJson = Encoding.UTF8.GetString(rawBytes);
                    }
                }
                catch (InvalidProtocolBufferException)
                {
                    // Si viene en texto JSON plano legacy
                    decryptedJson = Encoding.UTF8.GetString(rawBytes);
                }

                // 4. Deserialización segura Native AOT del JSON descifrado
                var rawEvent = JsonSerializer.Deserialize(decryptedJson, StreamJsonContext.Default.RawTransactionEvent);
                if (rawEvent == null)
                {
                    logger.LogWarning("Mensaje descifrado no pudo ser parseado como RawTransactionEvent: {Payload}", decryptedJson);
                    return false;
                }

                // Asignar el payload original completo
                rawEvent = rawEvent with { RawPayloadJson = decryptedJson };

                // 5. Transformación y lógica de negocio de dominio
                var processedEvent = transformer.TransformAndEnrich(rawEvent);

                // 6. Serialización segura Native AOT del JSON enriquecido en claro
                var processedJson = JsonSerializer.Serialize(processedEvent, StreamJsonContext.Default.ProcessedTransactionEvent);

                // 7. Enriquecimiento de cabeceras de trazabilidad y seguridad
                var enrichedHeaders = new Dictionary<string, string>(headers ?? new Dictionary<string, string>())
                {
                    ["x-stream-processor"] = "ConsumerStreams.NativeAOT",
                    ["x-decryption-algorithm"] = "AES-256-GCM",
                    ["x-vault-token"] = tokenUsed ?? "NONE",
                    ["x-processed-status"] = processedEvent.ProcessedStatus ?? "UNKNOWN",
                    ["x-risk-level"] = processedEvent.RiskLevel ?? "LOW",
                    ["x-latency-ms"] = processedEvent.ProcessingLatencyMs.ToString("F2")
                };

                // 8. Reenvío del JSON en claro al tópico de destino (30 particiones balanceadas por SplitMix64)
                var partitionKey = UniformPartitionKeyGenerator.GenerateDispersedKey(processedEvent.OriginAccount ?? key);

                var published = await producerPort.ForwardEventAsync(
                    targetTopic,
                    partitionKey,
                    processedJson,
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
                logger.LogError(ex, "Error procesando/descifrando evento en pipeline de streaming para key '{Key}'", key);
                return false;
            }
        }, cancellationToken);
    }
}
