using System.Text;
using Confluent.Kafka;
using ConsumerStreams.Infrastructure.Messaging;

namespace ConsumerStreams.Infrastructure.Tests.Messaging;

public class KafkaHeaderMapperTests
{
    [Fact]
    public void ToDictionary_Null_ReturnsEmpty() => Assert.Empty(KafkaHeaderMapper.ToDictionary(null));

    [Fact]
    public void ToDictionary_DecodesUtf8()
    {
        var headers = new Headers { { "x-service-name", Encoding.UTF8.GetBytes("Transfer.Mspx") } };

        Assert.Equal("Transfer.Mspx", KafkaHeaderMapper.ToDictionary(headers)["x-service-name"]);
    }

    [Fact]
    public void ToKafkaHeaders_SkipsNulls_AndRoundTrips()
    {
        var source = new Dictionary<string, string?> { ["a"] = "1", ["b"] = null };

        var headers = KafkaHeaderMapper.ToKafkaHeaders(source!);

        Assert.True(headers.TryGetLastBytes("a", out _));
        Assert.False(headers.TryGetLastBytes("b", out _));
    }

    [Fact]
    public void RoundTrip_PreservesEntries()
    {
        var original = new Dictionary<string, string> { ["k1"] = "v1", ["k2"] = "v2" };

        Assert.Equal(original, KafkaHeaderMapper.ToDictionary(KafkaHeaderMapper.ToKafkaHeaders(original)));
    }
}
