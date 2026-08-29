using LogSink.Domain;
using LogSink.Domain.Services;

namespace LogSink.Domain.Tests.Services;

public class TargetCollectionResolverTests
{
    [Fact]
    public void Resolve_WhenExplicitTargetCollectionHeaderPresent_ReturnsItVerbatim()
    {
        var headers = new Dictionary<string, string>
        {
            [ObservabilityHeaders.TargetCollection] = "Transfer_Mspx_Prometeus_Management_Trace",
            [ObservabilityHeaders.ServiceName] = "Ignored.Service",
            [ObservabilityHeaders.TelemetryType] = "Metric"
        };

        Assert.Equal("Transfer_Mspx_Prometeus_Management_Trace", TargetCollectionResolver.Resolve(headers));
    }

    [Fact]
    public void Resolve_WhenOnlyServiceAndTelemetryPresent_ComposesSanitizedName()
    {
        var headers = new Dictionary<string, string>
        {
            [ObservabilityHeaders.ServiceName] = "Transfer.Mspx.Prometeus.Management",
            [ObservabilityHeaders.TelemetryType] = "Trace"
        };

        Assert.Equal("Transfer_Mspx_Prometeus_Management_Trace", TargetCollectionResolver.Resolve(headers));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Resolve_WhenExplicitHeaderBlank_FallsBackToComposition(string blank)
    {
        var headers = new Dictionary<string, string>
        {
            [ObservabilityHeaders.TargetCollection] = blank,
            [ObservabilityHeaders.ServiceName] = "Svc.A",
            [ObservabilityHeaders.TelemetryType] = "Log"
        };

        Assert.Equal("Svc_A_Log", TargetCollectionResolver.Resolve(headers));
    }

    [Fact]
    public void Resolve_WhenHeadersInsufficient_ReturnsNull()
    {
        var headers = new Dictionary<string, string>
        {
            [ObservabilityHeaders.ServiceName] = "Svc.A"
            // sin x-telemetry-type
        };

        Assert.Null(TargetCollectionResolver.Resolve(headers));
    }

    [Fact]
    public void Resolve_WhenNoHeaders_ReturnsNull()
    {
        Assert.Null(TargetCollectionResolver.Resolve(new Dictionary<string, string>()));
    }

    [Fact]
    public void Resolve_WhenHeadersNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => TargetCollectionResolver.Resolve(null!));
    }
}
