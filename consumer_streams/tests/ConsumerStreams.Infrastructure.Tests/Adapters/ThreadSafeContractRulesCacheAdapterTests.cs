using ConsumerStreams.Domain.Contracts;
using ConsumerStreams.Domain.Models;
using ConsumerStreams.Infrastructure.Adapters;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace ConsumerStreams.Infrastructure.Tests.Adapters;

public class ThreadSafeContractRulesCacheAdapterTests
{
    private const string YamlA = "openapi: 3.0.0\ninfo:\n  title: A\n  version: 1.0.0\n";
    private const string YamlB = "openapi: 3.0.0\ninfo:\n  title: B\n  version: 2.0.0\n";

    private readonly Mock<IContractCompiler> _compiler = new();
    private readonly FakeTimeProvider _clock = new();

    public ThreadSafeContractRulesCacheAdapterTests()
    {
        _compiler.Setup(c => c.Compile(It.IsAny<string>()))
            .Returns<string>(yaml => new CompiledContractRules
            {
                ServiceName = yaml.Contains("title: B") ? "B" : "A",
                Version = "1.0",
                ContractKey = yaml.GetHashCode().ToString(),
                Operations = System.Collections.Frozen.FrozenDictionary<string, System.Collections.Frozen.FrozenDictionary<string, DataProtectionRuleType>>.Empty
            });
    }

    private ThreadSafeContractRulesCacheAdapter Build()
        => new(_compiler.Object, _clock, NullLogger<ThreadSafeContractRulesCacheAdapter>.Instance);

    [Fact]
    public void GetOrCompile_SameContract_CompilesOnlyOnce()
    {
        using var cache = Build();

        cache.GetOrCompile(YamlA);
        cache.GetOrCompile(YamlA);
        cache.GetOrCompile(YamlA);

        _compiler.Verify(c => c.Compile(YamlA), Times.Once);
        Assert.Equal(1, cache.ActiveContractsCount);
    }

    [Fact]
    public void GetOrCompile_DifferentContracts_AreCachedSeparately()
    {
        using var cache = Build();

        cache.GetOrCompile(YamlA);
        cache.GetOrCompile(YamlB);

        Assert.Equal(2, cache.ActiveContractsCount);
    }

    [Fact]
    public void GetOrCompile_BlankYaml_CompilesEmptyWithoutCaching()
    {
        using var cache = Build();

        cache.GetOrCompile("   ");

        Assert.Equal(0, cache.ActiveContractsCount);
        _compiler.Verify(c => c.Compile(string.Empty), Times.Once);
    }

    [Fact]
    public void EvictionTimer_RemovesContractsIdleBeyondSlidingTtl()
    {
        using var cache = Build();
        cache.GetOrCompile(YamlA);

        _clock.Advance(TimeSpan.FromMinutes(11)); // supera el TTL de 10 min y dispara varios ticks de 1 min

        Assert.Equal(0, cache.ActiveContractsCount);
    }

    [Fact]
    public void EvictionTimer_KeepsContractsThatWereRecentlyAccessed()
    {
        using var cache = Build();
        cache.GetOrCompile(YamlA);

        for (var i = 0; i < 15; i++)
        {
            _clock.Advance(TimeSpan.FromMinutes(1));
            cache.GetOrCompile(YamlA); // "touch" cada minuto
        }

        Assert.Equal(1, cache.ActiveContractsCount);
    }
}
