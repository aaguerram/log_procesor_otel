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
        var effectiveEmittedAt = raw.EmittedAt != default
            ? raw.EmittedAt
            : (raw.StartTimeUtc ?? now);

        var latencyMs = raw.DurationMs.HasValue && raw.DurationMs.Value > 0
            ? raw.DurationMs.Value
            : Math.Max(0.1, (now - effectiveEmittedAt).TotalMilliseconds);

        var effectiveTxnId = raw.TransactionId ?? raw.TraceId ?? $"TXN-{Guid.NewGuid().ToString()[..8].ToUpper()}";
        var effectiveEventId = raw.EventId ?? raw.SpanId ?? Guid.NewGuid().ToString();
        var effectiveChannel = raw.Channel ?? (raw.Tags != null && raw.Tags.TryGetValue("url.path", out var p) ? p : "OTEL_TRACE");
        var effectiveTxnType = raw.TransactionType ?? raw.Name ?? "OBSERVABILITY_LOG";

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

        // Preservar todas las etiquetas OTel
        if (raw.Tags != null)
        {
            foreach (var (k, v) in raw.Tags)
            {
                audit[$"otel.{k}"] = v;
            }
        }

        string? responsePreview = null;
        string? requestPreview = null;
        if (raw.Tags != null)
        {
            raw.Tags.TryGetValue("http.response.body_preview", out responsePreview);
            raw.Tags.TryGetValue("http.request.body_preview", out requestPreview);
        }

        return new ProcessedTransactionEvent
        {
            StreamProcessId = $"PROC-{Guid.NewGuid().ToString()[..8].ToUpper()}",
            OriginalEventId = effectiveEventId,
            TransactionId = effectiveTxnId,
            TraceId = raw.TraceId,
            SpanId = raw.SpanId,
            ParentSpanId = raw.ParentSpanId,
            Name = raw.Name,
            Kind = raw.Kind,
            OriginAccount = raw.OriginAccount ?? (raw.TraceId != null ? $"TRACE-{raw.TraceId[..8]}" : null),
            DestinationAccount = raw.DestinationAccount,
            Amount = raw.Amount,
            Currency = raw.Currency ?? "USD",
            TransactionType = effectiveTxnType,
            Channel = effectiveChannel,
            RiskLevel = riskLevel,
            FraudScore = score,
            ProcessedStatus = processedStatus,
            OriginalEmittedAt = effectiveEmittedAt,
            ProcessedAt = now,
            ProcessingLatencyMs = Math.Round(latencyMs, 2),
            Tags = raw.Tags,
            ResponseBodyPreview = responsePreview,
            RequestBodyPreview = requestPreview,
            RawPayload = raw.RawPayloadJson,
            AuditMetadata = audit
        };
    }
}
