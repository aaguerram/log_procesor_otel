using System.Text;
using Confluent.Kafka;
using LogSink.Infrastructure.Messaging;

namespace LogSink.Infrastructure.Tests.Messaging;

public class KafkaHeaderMapperTests
{
    [Fact]
    public void ToDictionary_WhenNull_ReturnsEmpty()
    {
        Assert.Empty(KafkaHeaderMapper.ToDictionary(null));
    }

    [Fact]
    public void ToDictionary_DecodesUtf8Values()
    {
        var headers = new Headers { { "x-service-name", Encoding.UTF8.GetBytes("Transfer.Mspx") } };

        var dict = KafkaHeaderMapper.ToDictionary(headers);

        Assert.Equal("Transfer.Mspx", dict["x-service-name"]);
    }

    [Fact]
    public void ToKafkaHeaders_SkipsNullValues_AndEncodesUtf8()
    {
        var source = new Dictionary<string, string?>
        {
            ["a"] = "1",
            ["b"] = null
        };

        var headers = KafkaHeaderMapper.ToKafkaHeaders(source!);

        Assert.True(headers.TryGetLastBytes("a", out var bytes));
        Assert.Equal("1", Encoding.UTF8.GetString(bytes));
        Assert.False(headers.TryGetLastBytes("b", out _));
    }

    [Fact]
    public void RoundTrip_PreservesEntries()
    {
        var original = new Dictionary<string, string> { ["k1"] = "v1", ["k2"] = "v2" };

        var roundTripped = KafkaHeaderMapper.ToDictionary(KafkaHeaderMapper.ToKafkaHeaders(original));

        Assert.Equal(original, roundTripped);
    }
}
