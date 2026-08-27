using ConsumerStreams.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;

namespace ConsumerStreams.Infrastructure.Tests.Configuration;

public class ConfigReaderTests
{
    private static IConfiguration Config(params (string Key, string Value)[] pairs)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(pairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)))
            .Build();

    [Fact]
    public void FirstValue_ReturnsFirstNonBlankInPriorityOrder()
        => Assert.Equal("v", Config(("A", "  "), ("B", "v")).FirstValue("A", "B", "C"));

    [Fact]
    public void Required_Missing_ThrowsConfigError()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Config().Required("KafkaStream:BootstrapServers", "A"));
        Assert.Contains("KafkaStream:BootstrapServers", ex.Message);
    }

    [Theory]
    [InlineData("1000", 1000)]
    [InlineData("x", 42)]
    public void IntOrDefault_ParsesOrFallsBack(string raw, int expected)
        => Assert.Equal(expected, Config(("k", raw)).IntOrDefault(42, "k"));

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    [InlineData("x", false)]
    public void BoolOrDefault_ParsesOrFallsBack(string raw, bool expected)
        => Assert.Equal(expected, Config(("k", raw)).BoolOrDefault(false, "k"));

    [Theory]
    [InlineData("false", false)]
    [InlineData("true", true)]
    [InlineData("garbage", true)]
    [InlineData(null, true)]
    public void FlagEnabledByDefault_OnlyDisabledByExplicitFalse(string? raw, bool expected)
    {
        var config = raw is null ? Config() : Config(("k", raw));
        Assert.Equal(expected, config.FlagEnabledByDefault("k"));
    }
}
