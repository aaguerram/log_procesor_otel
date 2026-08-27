using ConsumerStreams.Infrastructure.Adapters;
using ConsumerStreams.Infrastructure.Security;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace ConsumerStreams.Infrastructure.Tests.Adapters;

public class AzureKeyVaultTokenAdapterTests
{
    private readonly Mock<IAesKeyMaterialFactory> _factory = new();
    private readonly FakeTimeProvider _clock = new();

    public AzureKeyVaultTokenAdapterTests()
    {
        _factory.Setup(f => f.Create(It.IsAny<string>(), It.IsAny<string>()))
            .Returns(() => new Domain.Ports.VaultKeyMaterial
            {
                VaultTokenId = "TKN", CertThumbprint = "TH", KeyVersion = "2026.1", AesKey256 = new byte[32]
            });
    }

    private AzureKeyVaultTokenAdapter Build()
        => new(_factory.Object, _clock, NullLogger<AzureKeyVaultTokenAdapter>.Instance);

    [Fact]
    public async Task ResolveKeyByTokenAsync_FirstCall_DelegatesToFactory()
    {
        await Build().ResolveKeyByTokenAsync("TKN", "TH");

        _factory.Verify(f => f.Create("TKN", "TH"), Times.Once);
    }

    [Fact]
    public async Task ResolveKeyByTokenAsync_WithinTtl_ServesFromCache()
    {
        var adapter = Build();

        await adapter.ResolveKeyByTokenAsync("TKN", "TH");
        _clock.Advance(TimeSpan.FromMinutes(59));
        await adapter.ResolveKeyByTokenAsync("TKN", "TH");

        _factory.Verify(f => f.Create(It.IsAny<string>(), It.IsAny<string>()), Times.Once);
    }

    [Fact]
    public async Task ResolveKeyByTokenAsync_AfterTtl_RecomputesKey()
    {
        var adapter = Build();

        await adapter.ResolveKeyByTokenAsync("TKN", "TH");
        _clock.Advance(TimeSpan.FromMinutes(61));
        await adapter.ResolveKeyByTokenAsync("TKN", "TH");

        _factory.Verify(f => f.Create(It.IsAny<string>(), It.IsAny<string>()), Times.Exactly(2));
    }
}
