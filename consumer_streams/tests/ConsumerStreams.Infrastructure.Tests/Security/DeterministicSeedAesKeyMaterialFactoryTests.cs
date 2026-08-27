using System.Security.Cryptography;
using System.Text;
using ConsumerStreams.Infrastructure.Security;

namespace ConsumerStreams.Infrastructure.Tests.Security;

public class DeterministicSeedAesKeyMaterialFactoryTests
{
    private readonly DeterministicSeedAesKeyMaterialFactory _factory = new();

    [Fact]
    public void Create_DerivesKeyFromTheSharedSeed()
    {
        var expected = SHA256.HashData(Encoding.UTF8.GetBytes(
            "PRODUBANCO-SECRET-KEY-SEED-produbanco-encryption-cert-2026"));

        var material = _factory.Create("TKN-1", "AA11");

        Assert.Equal(expected, material.AesKey256);
        Assert.Equal(32, material.AesKey256.Length);
    }

    [Fact]
    public void Create_PropagatesTokenAndThumbprint()
    {
        var material = _factory.Create("TKN-42", "THUMB-99");

        Assert.Equal("TKN-42", material.VaultTokenId);
        Assert.Equal("THUMB-99", material.CertThumbprint);
        Assert.Equal("2026.1", material.KeyVersion);
    }

    [Fact]
    public void Create_IsDeterministicAcrossCalls()
    {
        Assert.Equal(_factory.Create("a", "b").AesKey256, _factory.Create("c", "d").AesKey256);
    }
}
