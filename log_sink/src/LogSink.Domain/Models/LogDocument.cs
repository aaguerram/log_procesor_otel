using System.Text.Json.Serialization;

namespace LogSink.Domain.Models;

/// <summary>
/// Modelo canónico de documento de log/auditoría almacenado en Azure Cosmos DB / DocumentDB.
/// Compatible con la estructura ProcessedTransactionEvent y Native AOT sin reflexión.
/// </summary>
public record LogDocument
{
    [JsonPropertyName("id")]
    public string Id { get; init; } = string.Empty;

    [JsonPropertyName("streamProcessId")]
    public string? StreamProcessId { get; init; }

    [JsonPropertyName("originalEventId")]
    public string? OriginalEventId { get; init; }

    [JsonPropertyName("transactionId")]
    public string? TransactionId { get; init; }

    [JsonPropertyName("originAccount")]
    public string? OriginAccount { get; init; }

    [JsonPropertyName("destinationAccount")]
    public string? DestinationAccount { get; init; }

    [JsonPropertyName("amount")]
    public decimal Amount { get; init; }

    [JsonPropertyName("currency")]
    public string? Currency { get; init; } = "USD";

    [JsonPropertyName("transactionType")]
    public string? TransactionType { get; init; }

    [JsonPropertyName("channel")]
    public string? Channel { get; init; }

    [JsonPropertyName("riskLevel")]
    public string? RiskLevel { get; init; } = "LOW";

    [JsonPropertyName("fraudScore")]
    public int FraudScore { get; init; }

    [JsonPropertyName("processedStatus")]
    public string? ProcessedStatus { get; init; }

    [JsonPropertyName("originalEmittedAt")]
    public DateTime? OriginalEmittedAt { get; init; }

    [JsonPropertyName("processedAt")]
    public DateTime ProcessedAt { get; init; } = DateTime.UtcNow;

    [JsonPropertyName("persistedAt")]
    public DateTime PersistedAt { get; init; } = DateTime.UtcNow;

    [JsonPropertyName("processingLatencyMs")]
    public double ProcessingLatencyMs { get; init; }

    [JsonPropertyName("auditMetadata")]
    public Dictionary<string, string>? AuditMetadata { get; init; }

    [JsonPropertyName("partitionKey")]
    public string PartitionKey { get; init; } = string.Empty;

    [JsonPropertyName("ttl")]
    public int? Ttl { get; init; } = 60 * 60 * 24 * 90; // 90 días por defecto
}

/// <summary>
/// Resultado del proceso de inserción en lote (Bulk).
/// </summary>
public record BulkSinkResult(
    int TotalProcessed,
    int TotalSuccessful,
    int TotalFailed,
    double ElapsedMilliseconds,
    double RequestUnitsConsumed = 0.0);
