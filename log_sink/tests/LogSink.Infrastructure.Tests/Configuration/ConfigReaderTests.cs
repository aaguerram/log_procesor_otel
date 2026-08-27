using LogSink.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;

namespace LogSink.Infrastructure.Tests.Configuration;

public class ConfigReaderTests
{
    private static IConfiguration Config(params (string Key, string Value)[] pairs)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(pairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)))
            .Build();

    [Fact]
    public void FirstValue_ReturnsFirstNonBlankInPriorityOrder()
    {
        var config = Config(("B", "  "), ("C", "third"));

        Assert.Equal("third", config.FirstValue("A", "B", "C"));
    }

    [Fact]
    public void FirstValue_WhenNoneSet_ReturnsNull()
    {
        Assert.Null(Config().FirstValue("A", "B"));
    }

    [Fact]
    public void Required_WhenMissing_ThrowsConfigError()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Config().Required("LogSink:BootstrapServers", "A", "B"));
        Assert.Contains("LogSink:BootstrapServers", ex.Message);
        Assert.Contains("CONFIG ERROR", ex.Message);
    }

    [Fact]
    public void Required_WhenPresent_ReturnsValue()
    {
        var config = Config(("TECH_INT_MSG_KAFKA_BROKERS", "kafka:29092"));

        Assert.Equal("kafka:29092", config.Required("brokers", "LogSink:BootstrapServers", "TECH_INT_MSG_KAFKA_BROKERS"));
    }

    [Theory]
    [InlineData("42", 42)]
    [InlineData("not-a-number", 7)]
    [InlineData(null, 7)]
    public void IntOrDefault_ParsesOrFallsBack(string? raw, int expected)
    {
        var config = raw is null ? Config() : Config(("k", raw));

        Assert.Equal(expected, config.IntOrDefault(7, "k"));
    }

    [Theory]
    [InlineData("0.75", 0.75)]
    [InlineData("bad", 0.5)]
    public void DoubleOrDefault_ParsesOrFallsBack(string raw, double expected)
    {
        Assert.Equal(expected, Config(("k", raw)).DoubleOrDefault(0.5, "k"));
    }

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
