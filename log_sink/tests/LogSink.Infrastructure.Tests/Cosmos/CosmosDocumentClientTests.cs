using System.Net;
using LogSink.Domain.Ports;
using LogSink.Infrastructure.Cosmos;
using LogSink.Infrastructure.Tests.TestSupport;
using Microsoft.Extensions.Time.Testing;

namespace LogSink.Infrastructure.Tests.Cosmos;

public class CosmosDocumentClientTests
{
    private static readonly CosmosDbCredentials Credentials = new(
        Endpoint: "http://localhost:8081",
        PrimaryKey: "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
        DatabaseName: "ProdubancoObservability",
        ContainerName: "audit_logs",
        PartitionKeyPath: "/partitionKey");

    private static CosmosDocumentClient Build(StubHttpMessageHandler handler)
        => new(new HttpClient(handler), new CosmosResourceTokenFactory(), new FakeTimeProvider());

    [Fact]
    public async Task UpsertDocumentAsync_On201_ReturnsRequestChargeFromHeader()
    {
        var handler = new StubHttpMessageHandler().RespondWith(HttpStatusCode.Created, [("x-ms-request-charge", "7.23")]);

        var ru = await Build(handler).UpsertDocumentAsync(Credentials, null, "pk-1", "{\"id\":\"a\"}", CancellationToken.None);

        Assert.Equal(7.23, ru);
        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("http://localhost:8081/dbs/ProdubancoObservability/colls/audit_logs/docs", request.RequestUri!.ToString());
        Assert.Equal("True", request.Headers.GetValues("x-ms-documentdb-is-upsert").Single());
        Assert.Equal("[\"pk-1\"]", request.Headers.GetValues("x-ms-documentdb-partitionkey").Single());
    }

    [Fact]
    public async Task UpsertDocumentAsync_UsesTargetCollectionOverrideInResourcePath()
    {
        var handler = new StubHttpMessageHandler().RespondWith(HttpStatusCode.Created);

        await Build(handler).UpsertDocumentAsync(Credentials, "Svc_A_Trace", "pk", "{}", CancellationToken.None);

        Assert.EndsWith("/colls/Svc_A_Trace/docs", handler.Requests[0].RequestUri!.AbsolutePath);
    }

    [Fact]
    public async Task UpsertDocumentAsync_MissingChargeHeader_DefaultsToOne()
    {
        var handler = new StubHttpMessageHandler().RespondWith(HttpStatusCode.OK);

        var ru = await Build(handler).UpsertDocumentAsync(Credentials, null, "pk", "{}", CancellationToken.None);

        Assert.Equal(1.0, ru);
    }

    [Theory]
    [InlineData(HttpStatusCode.TooManyRequests)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.GatewayTimeout)]
    public async Task UpsertDocumentAsync_OnThrottleOrServerError_ThrowsCosmosTransientException(HttpStatusCode status)
    {
        var handler = new StubHttpMessageHandler().RespondWith(status);

        var ex = await Assert.ThrowsAsync<CosmosTransientException>(
            () => Build(handler).UpsertDocumentAsync(Credentials, null, "pk", "{}", CancellationToken.None));

        Assert.Equal(status, ex.StatusCode);
    }

    [Theory]
    [InlineData(HttpStatusCode.BadRequest)]
    [InlineData(HttpStatusCode.Forbidden)]
    [InlineData(HttpStatusCode.NotFound)]
    public async Task UpsertDocumentAsync_OnNonRetriableClientError_ThrowsInvalidOperation(HttpStatusCode status)
    {
        var handler = new StubHttpMessageHandler().RespondWith(status);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => Build(handler).UpsertDocumentAsync(Credentials, null, "pk", "{}", CancellationToken.None));
    }
}
