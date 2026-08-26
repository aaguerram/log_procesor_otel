using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Ports;

namespace ConsumerStreams.Application.Services;

/// <summary>
/// Implementación de la lógica de dominio para enriquecimiento y análisis de riesgo en streaming.
/// </summary>
public class TransactionEnricher : ITransactionTransformerPort
{
    public ProcessedTransactionEvent TransformAndEnrich(RawTransactionEvent raw)
    {
        var now = DateTime.UtcNow;
        var latencyMs = raw.EmittedAt != default
            ? Math.Max(0.1, (now - raw.EmittedAt).TotalMilliseconds)
            : 1.0;

        // Reglas de negocio: Cálculo de Riesgo y Scoring de Fraude
        int score = 10;
        if (raw.Amount > 1500) score += 50;
        else if (raw.Amount > 500) score += 25;

        if (raw.Channel == "ATM" || raw.Channel == "MOBILE_APP") score += 10;
        if (raw.TransactionType == "WITHDRAWAL" || raw.TransactionType == "QR_PAYMENT") score += 15;

        score = Math.Clamp(score, 0, 100);

        string riskLevel = score switch
        {
            >= 60 => "HIGH",
            >= 30 => "MEDIUM",
            _ => "LOW"
        };

        string processedStatus = riskLevel == "HIGH" ? "FLAGGED_FOR_AUDIT" : "VERIFIED_AND_AUDITED";

        var audit = new Dictionary<string, string>
        {
            ["processor.engine"] = "KafkaStreaming-AOT",
            ["processor.runtime"] = ".NET 10 Native AOT",
            ["processor.node"] = Environment.MachineName,
            ["audit.score"] = score.ToString(),
            ["audit.risk"] = riskLevel
        };

        return new ProcessedTransactionEvent
        {
            StreamProcessId = $"PROC-{Guid.NewGuid().ToString()[..8].ToUpper()}",
            OriginalEventId = raw.EventId,
            TransactionId = raw.TransactionId,
            OriginAccount = raw.OriginAccount,
            DestinationAccount = raw.DestinationAccount,
            Amount = raw.Amount,
            Currency = raw.Currency ?? "USD",
            TransactionType = raw.TransactionType,
            Channel = raw.Channel,
            RiskLevel = riskLevel,
            FraudScore = score,
            ProcessedStatus = processedStatus,
            OriginalEmittedAt = raw.EmittedAt,
            ProcessedAt = now,
            ProcessingLatencyMs = Math.Round(latencyMs, 2),
            AuditMetadata = audit
        };
    }
}
