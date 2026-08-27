using System.Net;
using System.Net.Http.Headers;
using System.Text;
using LogSink.Domain.Ports;

namespace LogSink.Infrastructure.Cosmos;

/// <summary>
/// Implementa el upsert de un documento vía el endpoint REST de Cosmos DB
/// (<c>POST /dbs/{db}/colls/{coll}/docs</c> con cabecera <c>x-ms-documentdb-is-upsert</c>).
/// Recibe el <see cref="HttpClient"/> por inyección para poder sustituirlo en pruebas.
/// </summary>
public sealed class CosmosDocumentClient(
    HttpClient httpClient,
    ICosmosResourceTokenFactory tokenFactory,
    TimeProvider timeProvider) : ICosmosDocumentClient
{
    private const string ApiVersion = "2018-12-31";

    public async Task<double> UpsertDocumentAsync(
        CosmosDbCredentials credentials,
        string? targetCollection,
        string partitionKey,
        string rawJson,
        CancellationToken cancellationToken)
    {
        var collection = !string.IsNullOrWhiteSpace(targetCollection)
            ? targetCollection
            : credentials.ContainerName;

        var resourceLink = $"dbs/{credentials.DatabaseName}/colls/{collection}";
        var resourceUri = $"{credentials.Endpoint.TrimEnd('/')}/{resourceLink}/docs";
        var utcDate = timeProvider.GetUtcNow().UtcDateTime.ToString("r");

        using var request = new HttpRequestMessage(HttpMethod.Post, resourceUri)
        {
            Content = new ByteArrayContent(Encoding.UTF8.GetBytes(rawJson))
            {
                Headers = { ContentType = new MediaTypeHeaderValue("application/json") }
            }
        };

        request.Headers.Add("x-ms-date", utcDate);
        request.Headers.Add("x-ms-version", ApiVersion);
        request.Headers.Add("x-ms-documentdb-is-upsert", "True");
        request.Headers.Add("x-ms-documentdb-partitionkey", $"[\"{partitionKey}\"]");
        request.Headers.Add("authorization", tokenFactory.Create("POST", "docs", resourceLink, utcDate, credentials.PrimaryKey));

        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

        var requestCharge = ReadRequestCharge(response);

        if (response.IsSuccessStatusCode)
        {
            return requestCharge;
        }

        if (response.StatusCode == HttpStatusCode.TooManyRequests || (int)response.StatusCode >= 500)
        {
            throw new CosmosTransientException(
                $"Cosmos DB devolvió código transitorio {(int)response.StatusCode} ({response.ReasonPhrase})",
                response.StatusCode);
        }

        throw new InvalidOperationException(
            $"Fallo en inserción Cosmos DB con código {(int)response.StatusCode}: {response.ReasonPhrase}");
    }

    private static double ReadRequestCharge(HttpResponseMessage response)
    {
        if (response.Headers.TryGetValues("x-ms-request-charge", out var values)
            && double.TryParse(values.FirstOrDefault(), out var parsed))
        {
            return parsed;
        }

        return 1.0;
    }
}
