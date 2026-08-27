using System.Security.Cryptography;
using System.Text;

namespace LogSink.Infrastructure.Cosmos;

/// <summary>
/// Implementación del esquema de firma HMAC-SHA256 del token maestro de Azure Cosmos DB
/// (<see href="https://learn.microsoft.com/rest/api/cosmos-db/access-control-on-cosmosdb-resources"/>).
/// </summary>
public sealed class CosmosResourceTokenFactory : ICosmosResourceTokenFactory
{
    private const string TokenVersion = "1.0";
    private const string TokenType = "master";

    public string Create(string verb, string resourceType, string resourceLink, string utcDate, string primaryKeyBase64)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(primaryKeyBase64);

        var keyBytes = Convert.FromBase64String(primaryKeyBase64);
        var payload = $"{verb.ToLowerInvariant()}\n{resourceType.ToLowerInvariant()}\n{resourceLink}\n{utcDate.ToLowerInvariant()}\n\n";

        using var hmac = new HMACSHA256(keyBytes);
        var signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(payload)));

        return Uri.EscapeDataString($"type={TokenType}&ver={TokenVersion}&sig={signature}");
    }
}
