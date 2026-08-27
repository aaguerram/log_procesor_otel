using ConsumerStreams.Domain.Observability;

namespace ConsumerStreams.Domain.Tests.Observability;

public class TargetCollectionResolverTests
{
    [Fact]
    public void Resolve_ReplacesDotsWithUnderscores_AndAppendsTelemetryLabel()
    {
        Assert.Equal(
            "Transfer_Mspx_Prometeus_Management_Trace",
            TargetCollectionResolver.Resolve("Transfer.Mspx.Prometeus.Management", "Trace"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void Resolve_BlankServiceName_Throws(string? serviceName)
    {
        Assert.ThrowsAny<ArgumentException>(() => TargetCollectionResolver.Resolve(serviceName!, "Trace"));
    }

    [Fact]
    public void Resolve_BlankLabel_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() => TargetCollectionResolver.Resolve("Svc.A", ""));
    }
}
