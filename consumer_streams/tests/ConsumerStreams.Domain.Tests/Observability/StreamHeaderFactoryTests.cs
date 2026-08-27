using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Observability;

namespace ConsumerStreams.Domain.Tests.Observability;

public class StreamHeaderFactoryTests
{
    private static ProcessedTransactionEvent Processed() => new()
    {
        StreamProcessId = "PROC-1",
        ProcessedStatus = "VERIFIED_AND_AUDITED",
        RiskLevel = "MEDIUM",
        FraudScore = 35,
        ProcessingLatencyMs = 12.345
    };

    [Fact]
    public void ForProcessedEvent_CopiesInboundHeaders_AndAddsProcessingMetadata()
    {
        var inbound = new Dictionary<string, string> { ["correlation-id"] = "abc" };

        var headers = StreamHeaderFactory.ForProcessedEvent(
            inbound, "TKN-1", "Transfer.Mspx", "Trace", "Transfer_Mspx_Trace", Processed());

        Assert.Equal("abc", headers["correlation-id"]);
        Assert.Equal("ConsumerStreams.NativeAOT", headers[StreamHeaders.StreamProcessor]);
        Assert.Equal("AES-256-GCM", headers[StreamHeaders.DecryptionAlgorithm]);
        Assert.Equal("TKN-1", headers[StreamHeaders.VaultToken]);
        Assert.Equal("Transfer.Mspx", headers[StreamHeaders.ServiceName]);
        Assert.Equal("Trace", headers[StreamHeaders.TelemetryType]);
        Assert.Equal("Transfer_Mspx_Trace", headers[StreamHeaders.TargetCollection]);
        Assert.Equal("VERIFIED_AND_AUDITED", headers[StreamHeaders.ProcessedStatus]);
        Assert.Equal("MEDIUM", headers[StreamHeaders.RiskLevel]);
        Assert.Equal("12.35", headers[StreamHeaders.LatencyMs]);
    }

    [Fact]
    public void ForProcessedEvent_NullVaultToken_WritesNone()
    {
        var headers = StreamHeaderFactory.ForProcessedEvent(null, "", "Svc", "Log", "Svc_Log", Processed());

        Assert.Equal("NONE", headers[StreamHeaders.VaultToken]);
    }

    [Fact]
    public void ForProcessedEvent_NullStatusAndRisk_UsesDefaults()
    {
        var processed = new ProcessedTransactionEvent { StreamProcessId = "P" };

        var headers = StreamHeaderFactory.ForProcessedEvent(null, "T", "Svc", "Trace", "Svc_Trace", processed);

        Assert.Equal("UNKNOWN", headers[StreamHeaders.ProcessedStatus]);
        Assert.Equal("LOW", headers[StreamHeaders.RiskLevel]);
    }

    [Fact]
    public void ForError_AddsErrorMetadata_WithIso8601Timestamp()
    {
        var occurredAt = new DateTimeOffset(2026, 8, 27, 10, 0, 0, TimeSpan.Zero);

        var headers = StreamHeaderFactory.ForError(
            new Dictionary<string, string> { ["k"] = "v" },
            new InvalidOperationException("boom"),
            "tp.observability.application-log.emitted.v1",
            occurredAt);

        Assert.Equal("v", headers["k"]);
        Assert.Equal("InvalidOperationException", headers[StreamHeaders.ErrorType]);
        Assert.Equal("boom", headers[StreamHeaders.ErrorMessage]);
        Assert.Equal("tp.observability.application-log.emitted.v1", headers[StreamHeaders.SourceTopic]);
        Assert.Equal("ConsumerStreams.DLQ", headers[StreamHeaders.ErrorHandler]);
        Assert.Equal(occurredAt.ToString("O"), headers[StreamHeaders.ErrorTimestamp]);
    }
}
