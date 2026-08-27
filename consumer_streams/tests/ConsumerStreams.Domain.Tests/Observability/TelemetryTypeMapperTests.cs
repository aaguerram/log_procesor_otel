using ConsumerStreams.Domain.Observability;
using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Tests.Observability;

public class TelemetryTypeMapperTests
{
    [Theory]
    [InlineData(TelemetryType.Trace, "Trace")]
    [InlineData(TelemetryType.Metric, "Metric")]
    [InlineData(TelemetryType.Log, "Log")]
    [InlineData(TelemetryType.Unspecified, "Trace")]
    public void ToLabel_MapsEnumToCanonicalLabel(TelemetryType type, string expected)
    {
        Assert.Equal(expected, TelemetryTypeMapper.ToLabel(type));
    }
}
