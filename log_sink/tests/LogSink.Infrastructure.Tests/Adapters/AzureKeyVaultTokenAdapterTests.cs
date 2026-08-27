using LogSink.Infrastructure.Adapters;
using LogSink.Infrastructure.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace LogSink.Infrastructure.Tests.Adapters;

public class AzureKeyVaultTokenAdapterTests
{
    private static readonly SinkSettings Settings = new()
    {
        CosmosEndpoint = "http://azure-documentdb:8081",
        CosmosPrimaryKey = "primary-key",
        DatabaseName = "ProdubancoObservability",
        ContainerName = "audit_logs",
        PartitionKeyPath = "/partitionKey",
        VaultTokenId = "TKN-COSMOS-PRODUBANCO-V1"
    };

    private static AzureKeyVaultTokenAdapter Build(FakeTimeProvider clock)
        => new(Options.Create(Settings), clock, NullLogger<AzureKeyVaultTokenAdapter>.Instance);

    [Fact]
    public async Task ResolveCosmosCredentialsAsync_MapsFromSettings()
    {
        var creds = await Build(new FakeTimeProvider()).ResolveCosmosCredentialsAsync("TKN-COSMOS-PRODUBANCO-V1");

        Assert.Equal(Settings.CosmosEndpoint, creds.Endpoint);
        Assert.Equal(Settings.CosmosPrimaryKey, creds.PrimaryKey);
        Assert.Equal(Settings.DatabaseName, creds.DatabaseName);
        Assert.Equal(Settings.ContainerName, creds.ContainerName);
    }

    [Fact]
    public async Task ResolveCosmosCredentialsAsync_ReturnsCachedInstanceWithinTtl()
    {
        var clock = new FakeTimeProvider();
        var adapter = Build(clock);

        var first = await adapter.ResolveCosmosCredentialsAsync("token");
        clock.Advance(TimeSpan.FromMinutes(59));
        var second = await adapter.ResolveCosmosCredentialsAsync("token");

        Assert.Same(first, second);
    }

    [Fact]
    public async Task ResolveCosmosCredentialsAsync_RefreshesAfterTtlExpires()
    {
        var clock = new FakeTimeProvider();
        var adapter = Build(clock);

        var first = await adapter.ResolveCosmosCredentialsAsync("token");
        clock.Advance(TimeSpan.FromMinutes(61));
        var second = await adapter.ResolveCosmosCredentialsAsync("token");

        Assert.NotSame(first, second);
        Assert.Equal(first, second); // mismo contenido, instancia nueva
    }
}
