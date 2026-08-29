using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Ports;

namespace ConsumerStreams.Application.Services;

/// <summary>
/// Implementación de la lógica de dominio para enriquecimiento y análisis de riesgo en streaming.
/// El reloj se inyecta como <see cref="TimeProvider"/> para poder verificar el cálculo de latencia
/// y las marcas de tiempo de forma determinista.
/// </summary>
public class TransactionEnricher(TimeProvider timeProvider) : ITransactionTransformerPort
{
    public ProcessedTransactionEvent TransformAndEnrich(RawTransactionEvent rawEvent)
    {
        var now = timeProvider.GetUtcNow().UtcDateTime;

        var effectiveEmittedAt = rawEvent.EmittedAt != default
            ? rawEvent.EmittedAt
            : (rawEvent.StartTimeUtc ?? now);

        var latencyMs = rawEvent.DurationMs is > 0
            ? rawEvent.DurationMs.Value
            : Math.Max(0.1, (now - effectiveEmittedAt).TotalMilliseconds);

        var effectiveTxnId = rawEvent.TransactionId ?? rawEvent.TraceId ?? $"TXN-{Guid.NewGuid().ToString()[..8].ToUpper()}";
        var effectiveEventId = rawEvent.EventId ?? rawEvent.SpanId ?? Guid.NewGuid().ToString();
        var effectiveChannel = rawEvent.Channel ?? ResolveChannelFromTags(rawEvent.Tags);
        var effectiveTxnType = rawEvent.TransactionType ?? rawEvent.Name ?? "OBSERVABILITY_LOG";

        // Reglas de negocio: Cálculo de Riesgo y Scoring de Fraude
        int score = CalculateFraudScore(rawEvent);
        string riskLevel = ResolveRiskLevel(score);
        string processedStatus = riskLevel == "HIGH" ? "FLAGGED_FOR_AUDIT" : "VERIFIED_AND_AUDITED";

        var audit = BuildAuditMetadata(rawEvent, score, riskLevel);
        ExtractBodyPreviews(rawEvent.Tags, out var responsePreview, out var requestPreview);

        return new ProcessedTransactionEvent
        {
            StreamProcessId = $"PROC-{Guid.NewGuid().ToString()[..8].ToUpper()}",
            OriginalEventId = effectiveEventId,
            TransactionId = effectiveTxnId,
            TraceId = rawEvent.TraceId,
            SpanId = rawEvent.SpanId,
            ParentSpanId = rawEvent.ParentSpanId,
            Name = rawEvent.Name,
            Kind = rawEvent.Kind,
            OriginAccount = rawEvent.OriginAccount ?? (rawEvent.TraceId != null ? $"TRACE-{rawEvent.TraceId[..8]}" : null),
            DestinationAccount = rawEvent.DestinationAccount,
            Amount = rawEvent.Amount,
            Currency = rawEvent.Currency ?? "USD",
            TransactionType = effectiveTxnType,
            Channel = effectiveChannel,
            RiskLevel = riskLevel,
            FraudScore = score,
            ProcessedStatus = processedStatus,
            OriginalEmittedAt = effectiveEmittedAt,
            ProcessedAt = now,
            ProcessingLatencyMs = Math.Round(latencyMs, 2),
            Tags = rawEvent.Tags,
            ResponseBodyPreview = responsePreview,
            RequestBodyPreview = requestPreview,
            RawPayload = rawEvent.RawPayloadJson,
            AuditMetadata = audit
        };
    }

    private static string ResolveChannelFromTags(Dictionary<string, string>? tags)
        => tags != null && tags.TryGetValue("url.path", out var path) ? path : "OTEL_TRACE";

    /// <summary>Motor de scoring de fraude por monto, canal y tipo de transacción (0-100).</summary>
    private static int CalculateFraudScore(RawTransactionEvent e)
    {
        int score = 10;

        if (e.Amount > 1500) score += 50;
        else if (e.Amount > 500) score += 25;

        if (e.Channel is "ATM" or "MOBILE_APP") score += 10;
        if (e.TransactionType is "WITHDRAWAL" or "QR_PAYMENT") score += 15;

        return Math.Clamp(score, 0, 100);
    }

    private static string ResolveRiskLevel(int score) => score switch
    {
        >= 60 => "HIGH",
        >= 30 => "MEDIUM",
        _ => "LOW"
    };

    private static Dictionary<string, string> BuildAuditMetadata(RawTransactionEvent e, int score, string riskLevel)
    {
        var audit = new Dictionary<string, string>
        {
            ["processor.engine"] = "KafkaStreaming-AOT",
            ["processor.runtime"] = ".NET 10 Native AOT",
            ["processor.node"] = Environment.MachineName,
            ["audit.score"] = score.ToString(),
            ["audit.risk"] = riskLevel
        };

        // Preservar todas las etiquetas OTel
        if (e.Tags != null)
        {
            foreach (var (k, v) in e.Tags)
            {
                audit[$"otel.{k}"] = v;
            }
        }

        return audit;
    }

    private static void ExtractBodyPreviews(Dictionary<string, string>? tags, out string? responsePreview, out string? requestPreview)
    {
        responsePreview = null;
        requestPreview = null;
        if (tags == null) return;

        tags.TryGetValue("http.response.body_preview", out responsePreview);
        tags.TryGetValue("http.request.body_preview", out requestPreview);
    }
}
