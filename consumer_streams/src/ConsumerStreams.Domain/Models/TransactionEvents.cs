namespace ConsumerStreams.Domain.Models;

/// <summary>
/// Evento transaccional o traza de observabilidad de entrada consumida desde el tópico de origen en Kafka.
/// </summary>
public record RawTransactionEvent
{
    // Campos Estándar Transaccionales
    public string? EventId { get; init; }
    public int Sequence { get; init; }
    public string? TransactionId { get; init; }
    public string? OriginAccount { get; init; }
    public string? DestinationAccount { get; init; }
    public decimal Amount { get; init; }
    public string? Currency { get; init; }
    public string? TransactionType { get; init; }
    public string? Channel { get; init; }
    public string? Status { get; init; }
    public DateTime EmittedAt { get; init; }
    public Dictionary<string, string>? Metadata { get; init; }

    // Campos Estándar OpenTelemetry (OTel Traces)
    public string? TraceId { get; init; }
    public string? SpanId { get; init; }
    public string? ParentSpanId { get; init; }
    public string? Name { get; init; }
    public string? Kind { get; init; }
    public DateTime? StartTimeUtc { get; init; }
    public double? DurationMs { get; init; }
    public Dictionary<string, string>? Tags { get; init; }
}

/// <summary>
/// Evento transaccional o log de observabilidad enriquecido y procesado por el stream pipeline hacia el tópico de salida.
/// </summary>
public record ProcessedTransactionEvent
{
    public required string StreamProcessId { get; init; }
    public string? OriginalEventId { get; init; }
    public string? TransactionId { get; init; }
    public string? TraceId { get; init; }
    public string? SpanId { get; init; }
    public string? OriginAccount { get; init; }
    public string? DestinationAccount { get; init; }
    public decimal Amount { get; init; }
    public string? Currency { get; init; }
    public string? TransactionType { get; init; }
    public string? Channel { get; init; }
    public string? RiskLevel { get; init; }
    public int FraudScore { get; init; }
    public string? ProcessedStatus { get; init; }
    public DateTime OriginalEmittedAt { get; init; }
    public DateTime ProcessedAt { get; init; } = DateTime.UtcNow;
    public double ProcessingLatencyMs { get; init; }
    public Dictionary<string, string> AuditMetadata { get; init; } = [];
}
