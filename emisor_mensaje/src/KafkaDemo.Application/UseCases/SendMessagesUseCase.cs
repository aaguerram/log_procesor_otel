using System.Diagnostics;
using System.Text.Json;
using Google.Protobuf;
using KafkaDemo.Application.DTOs;
using KafkaDemo.Domain.Models;
using KafkaDemo.Domain.Ports;
using KafkaDemo.Domain.Utils;

namespace KafkaDemo.Application.UseCases;

/// <summary>
/// Caso de uso para la generación, cifrado AES-256-GCM con Azure Key Vault, dispersión SplitMix64 y publicación Protobuf hacia Kafka.
/// </summary>
public class SendMessagesUseCase(
    IMessageProducerPort producerPort,
    ITopicManagementPort topicManagementPort,
    IVaultTokenProviderPort vaultTokenPort,
    IPayloadCryptoPort cryptoPort)
{
    private static readonly string[] TransactionTypes = ["TRANSFER", "PAYMENT", "DEPOSIT", "WITHDRAWAL", "QR_PAYMENT"];
    private static readonly string[] Channels = ["MOBILE_APP", "WEB_BANKING", "ATM", "BRANCH", "API_GATEWAY"];
    private static readonly string[] Currencies = ["USD", "EUR"];

    /// <summary>
    /// Envía un mensaje individual cifrado con AES-256-GCM y clave de partición de alta dispersión (SplitMix64).
    /// </summary>
    public async Task<MessageResult> SendCustomMessageAsync(SendMessageRequestDto request, CancellationToken cancellationToken = default)
    {
        // 1. Obtener material de clave tokenizado de Azure Key Vault
        var keyMaterial = await vaultTokenPort.GetOrCreateEncryptionKeyAsync("produbanco-encryption-cert", cancellationToken);

        var eventId = Guid.NewGuid().ToString();
        var txnId = request.Key ?? $"TXN-{DateTime.UtcNow:yyyyMMdd}-{eventId[..6].ToUpper()}";
        
        // Generar clave de particionamiento con dispersión matemática de efecto avalancha
        var partitionKey = UniformPartitionKeyGenerator.GenerateDispersedKey(request.Key ?? txnId);

        // 2. Cifrar con AES-256-GCM y construir Envelope Protobuf Autosuficiente
        var envelope = cryptoPort.EncryptJsonToEnvelope(
            request.Value,
            eventId,
            txnId,
            partitionKey,
            keyMaterial,
            request.Headers);

        var protobufBytes = envelope.ToByteArray();

        var headers = new Dictionary<string, string>(request.Headers ?? new Dictionary<string, string>())
        {
            ["content-type"] = "application/x-protobuf",
            ["x-encryption-algorithm"] = "AES-256-GCM",
            ["x-vault-token"] = keyMaterial.VaultTokenId,
            ["x-cert-thumbprint"] = keyMaterial.CertThumbprint
        };

        var kafkaMessage = new KafkaMessage
        {
            Topic = request.Topic,
            Key = partitionKey,
            Value = Convert.ToBase64String(protobufBytes),
            BinaryValue = protobufBytes,
            Headers = headers,
            Timestamp = DateTime.UtcNow
        };

        return await producerPort.SendMessageAsync(kafkaMessage, cancellationToken);
    }

    /// <summary>
    /// Genera y envía un lote de mensajes cifrados en Protobuf con distribución uniforme en las 40 particiones.
    /// </summary>
    public async Task<BatchSendResultDto> GenerateAndSendBatchAsync(string topic, int count = 20, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topic))
            throw new ArgumentException("El nombre del tópico de destino es obligatorio y no puede ser nulo o vacío.", nameof(topic));

        var stopwatch = Stopwatch.StartNew();

        // 1. Obtener material de clave tokenizado de Azure Key Vault
        var keyMaterial = await vaultTokenPort.GetOrCreateEncryptionKeyAsync("produbanco-encryption-cert", cancellationToken);

        // 2. Asegurar que el tópico existe
        var existingTopics = await topicManagementPort.GetTopicsAsync(includeInternal: false, cancellationToken);
        if (!existingTopics.Any(t => t.Name.Equals(topic, StringComparison.OrdinalIgnoreCase)))
        {
            await topicManagementPort.CreateTopicAsync(new TopicCreationRequest
            {
                TopicName = topic,
                NumPartitions = 40,
                ReplicationFactor = 1
            }, cancellationToken);
        }

        // 3. Generar y cifrar los mensajes del lote
        var random = new Random();
        var messages = new List<KafkaMessage>(count);

        for (int i = 1; i <= count; i++)
        {
            var transactionId = $"TXN-{DateTime.UtcNow:yyyyMMdd}-{i:D4}-{Guid.NewGuid().ToString()[..6].ToUpper()}";
            var accountFrom = $"ACCT-{(10000000 + random.Next(1, 9999999))}";
            var accountTo = $"ACCT-{(20000000 + random.Next(1, 9999999))}";
            var amount = Math.Round((decimal)(random.NextDouble() * 2500 + 10), 2);
            var type = TransactionTypes[random.Next(TransactionTypes.Length)];
            var channel = Channels[random.Next(Channels.Length)];
            var currency = Currencies[random.Next(Currencies.Length)];

            // Generar clave de partición de alta dispersión única con SplitMix64
            var dispersedPartitionKey = UniformPartitionKeyGenerator.GenerateDispersedKey(accountFrom);

            var payload = new
            {
                EventId = Guid.NewGuid().ToString(),
                Sequence = i,
                TransactionId = transactionId,
                OriginAccount = accountFrom,
                DestinationAccount = accountTo,
                Amount = amount,
                Currency = currency,
                TransactionType = type,
                Channel = channel,
                Status = "COMPLETED",
                EmittedAt = DateTime.UtcNow,
                Metadata = new Dictionary<string, string>
                {
                    { "SourceSystem", "ProdubancoHexagonalAOT" },
                    { "Framework", ".NET 10" },
                    { "Encryption", "AES-256-GCM-Protobuf" },
                    { "PartitionKey", dispersedPartitionKey },
                    { "VaultToken", keyMaterial.VaultTokenId }
                }
            };

            var jsonValue = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = false });

            // Cifrado AES-256-GCM y serialización Protobuf
            var envelope = cryptoPort.EncryptJsonToEnvelope(
                jsonValue,
                payload.EventId,
                transactionId,
                dispersedPartitionKey,
                keyMaterial,
                new Dictionary<string, string> { { "index", i.ToString() } });

            var protobufBytes = envelope.ToByteArray();

            messages.Add(new KafkaMessage
            {
                Topic = topic,
                Key = dispersedPartitionKey,
                Value = Convert.ToBase64String(protobufBytes),
                BinaryValue = protobufBytes,
                Headers = new Dictionary<string, string>
                {
                    { "content-type", "application/x-protobuf" },
                    { "x-encryption-algorithm", "AES-256-GCM" },
                    { "x-vault-token", keyMaterial.VaultTokenId },
                    { "correlation-id", transactionId },
                    { "message-index", i.ToString() }
                },
                Timestamp = DateTime.UtcNow
            });
        }

        // 4. Enviar lote a través del puerto
        var results = await producerPort.SendBatchAsync(messages, cancellationToken);
        stopwatch.Stop();

        return new BatchSendResultDto
        {
            TotalRequested = count,
            TotalSent = results.Count,
            TargetTopic = topic,
            ElapsedMilliseconds = stopwatch.Elapsed.TotalMilliseconds,
            Results = results
        };
    }
}
