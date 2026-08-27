using System.Security.Cryptography;
using System.Text;
using LogSink.Infrastructure.Cosmos;

namespace LogSink.Infrastructure.Tests.Cosmos;

public class CosmosResourceTokenFactoryTests
{
    // Clave bien conocida del emulador local de Cosmos DB.
    private const string EmulatorKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    private readonly CosmosResourceTokenFactory _factory = new();

    [Fact]
    public void Create_ProducesUrlEncodedMasterTokenWithMatchingHmacSignature()
    {
        const string verb = "POST";
        const string resourceType = "docs";
        const string resourceLink = "dbs/ProdubancoObservability/colls/audit_logs";
        const string date = "Tue, 01 Nov 2026 12:00:00 GMT";

        var token = Uri.UnescapeDataString(_factory.Create(verb, resourceType, resourceLink, date, EmulatorKey));

        Assert.StartsWith("type=master&ver=1.0&sig=", token);

        var expectedPayload = $"post\ndocs\n{resourceLink}\n{date.ToLowerInvariant()}\n\n";
        using var hmac = new HMACSHA256(Convert.FromBase64String(EmulatorKey));
        var expectedSig = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(expectedPayload)));

        Assert.Equal($"type=master&ver=1.0&sig={expectedSig}", token);
    }

    [Fact]
    public void Create_IsDeterministicForSameInputs()
    {
        var a = _factory.Create("POST", "docs", "dbs/d/colls/c", "date", EmulatorKey);
        var b = _factory.Create("POST", "docs", "dbs/d/colls/c", "date", EmulatorKey);

        Assert.Equal(a, b);
    }

    [Fact]
    public void Create_WithInvalidBase64Key_Throws()
    {
        Assert.ThrowsAny<FormatException>(() => _factory.Create("POST", "docs", "dbs/d/colls/c", "date", "not-base64!!!"));
    }

    [Fact]
    public void Create_WithBlankKey_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() => _factory.Create("POST", "docs", "link", "date", "   "));
    }
}
