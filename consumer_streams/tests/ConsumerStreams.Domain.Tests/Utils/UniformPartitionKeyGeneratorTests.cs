using System.Globalization;
using ConsumerStreams.Domain.Utils;

namespace ConsumerStreams.Domain.Tests.Utils;

public class UniformPartitionKeyGeneratorTests
{
    [Fact]
    public void GenerateDispersedKey_WithoutBusinessId_HasPkPrefixAnd16HexChars()
    {
        var key = UniformPartitionKeyGenerator.GenerateDispersedKey();

        Assert.StartsWith("PK-", key);
        var hex = key["PK-".Length..];
        Assert.Equal(16, hex.Length);
        Assert.True(ulong.TryParse(hex, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out _));
    }

    [Fact]
    public void GenerateDispersedKey_WithBusinessId_AppendsIdAsSuffix()
    {
        var key = UniformPartitionKeyGenerator.GenerateDispersedKey("ACCT-12345678");

        Assert.StartsWith("PK-", key);
        Assert.EndsWith("-ACCT-12345678", key);
    }

    [Fact]
    public void GenerateDispersedKey_ProducesUniqueKeysAcrossManyCalls()
    {
        var keys = Enumerable.Range(0, 5_000)
            .Select(_ => UniformPartitionKeyGenerator.GenerateDispersedKey())
            .ToHashSet();

        Assert.Equal(5_000, keys.Count);
    }

    [Fact]
    public void GenerateDispersedKey_DistributesAcrossAll40PartitionsWithNoEmptyBucket()
    {
        var buckets = new int[40];

        for (var i = 0; i < 8_000; i++)
        {
            var key = UniformPartitionKeyGenerator.GenerateDispersedKey($"ACCT-{i}");
            var hex = key.Split('-')[1];
            var value = ulong.Parse(hex, NumberStyles.HexNumber, CultureInfo.InvariantCulture);
            buckets[value % 40]++;
        }

        Assert.DoesNotContain(0, buckets);
        var average = 8_000 / 40.0;
        Assert.All(buckets, count => Assert.InRange(count, average * 0.5, average * 1.5));
    }
}
