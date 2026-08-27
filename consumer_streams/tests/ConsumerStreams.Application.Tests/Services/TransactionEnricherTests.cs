using ConsumerStreams.Application.Services;
using ConsumerStreams.Domain.Models;
using Microsoft.Extensions.Time.Testing;

namespace ConsumerStreams.Application.Tests.Services;

public class TransactionEnricherTests
{
    private static readonly DateTimeOffset Now = new(2026, 8, 27, 12, 0, 0, TimeSpan.Zero);
    private readonly TransactionEnricher _enricher = new(new FakeTimeProvider(Now));

    private static RawTransactionEvent Raw(decimal amount = 0, string? channel = null, string? type = null, double? durationMs = null)
        => new()
        {
            TransactionId = "TXN-1",
            EventId = "EVT-1",
            Amount = amount,
            Channel = channel,
            TransactionType = type,
            DurationMs = durationMs,
            EmittedAt = Now.UtcDateTime.AddMilliseconds(-250)
        };

    [Theory]
    [InlineData(100, 10, "LOW")]
    [InlineData(600, 35, "MEDIUM")]
    [InlineData(2000, 60, "HIGH")]
    public void TransformAndEnrich_ScoresRiskByAmountThresholds(decimal amount, int expectedScore, string expectedLevel)
    {
        var result = _enricher.TransformAndEnrich(Raw(amount));

        Assert.Equal(expectedScore, result.FraudScore);
        Assert.Equal(expectedLevel, result.RiskLevel);
    }

    [Fact]
    public void TransformAndEnrich_AddsChannelAndTransactionTypeRisk()
    {
        var result = _enricher.TransformAndEnrich(Raw(amount: 600, channel: "ATM", type: "WITHDRAWAL"));

        Assert.Equal(60, result.FraudScore); // 10 + 25 + 10 + 15
        Assert.Equal("HIGH", result.RiskLevel);
        Assert.Equal("FLAGGED_FOR_AUDIT", result.ProcessedStatus);
    }

    [Fact]
    public void TransformAndEnrich_LowRisk_IsVerifiedAndAudited()
    {
        Assert.Equal("VERIFIED_AND_AUDITED", _enricher.TransformAndEnrich(Raw(50)).ProcessedStatus);
    }

    [Fact]
    public void TransformAndEnrich_ScoreIsClampedTo100()
    {
        var result = _enricher.TransformAndEnrich(Raw(amount: 5000, channel: "ATM", type: "QR_PAYMENT"));

        Assert.Equal(85, result.FraudScore); // 10 + 50 + 10 + 15, sin superar 100
    }

    [Fact]
    public void TransformAndEnrich_UsesDurationMsForLatencyWhenPresent()
    {
        var result = _enricher.TransformAndEnrich(Raw(durationMs: 12.34));

        Assert.Equal(12.34, result.ProcessingLatencyMs);
    }

    [Fact]
    public void TransformAndEnrich_FallsBackToWallClockLatencyFromEmittedAt()
    {
        var result = _enricher.TransformAndEnrich(Raw());

        Assert.Equal(250, result.ProcessingLatencyMs);
    }

    [Fact]
    public void TransformAndEnrich_StampsProcessedAtFromInjectedClock()
    {
        var result = _enricher.TransformAndEnrich(Raw());

        Assert.Equal(Now.UtcDateTime, result.ProcessedAt);
    }

    [Fact]
    public void TransformAndEnrich_PreservesOpenTelemetryTraceFields()
    {
        var raw = new RawTransactionEvent
        {
            TraceId = "trace-abc",
            SpanId = "span-1",
            ParentSpanId = "span-0",
            Name = "GET /contacts",
            Kind = "SERVER",
            Tags = new Dictionary<string, string> { ["http.route"] = "/contacts" }
        };

        var result = _enricher.TransformAndEnrich(raw);

        Assert.Equal("trace-abc", result.TraceId);
        Assert.Equal("span-1", result.SpanId);
        Assert.Equal("GET /contacts", result.Name);
        Assert.Equal("/contacts", result.Tags!["http.route"]);
        Assert.Equal("/contacts", result.AuditMetadata["otel.http.route"]);
    }

    [Fact]
    public void TransformAndEnrich_DefaultsCurrencyToUsd()
    {
        Assert.Equal("USD", _enricher.TransformAndEnrich(Raw()).Currency);
    }
}
