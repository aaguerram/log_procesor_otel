using LogSink.Domain.Ports;
using LogSink.Infrastructure.Adapters;
using LogSink.Infrastructure.Configuration;
using LogSink.Infrastructure.Cosmos;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace LogSink.Infrastructure.Tests.Adapters;

public class CosmosDbBulkSinkAdapterTests
{
    private readonly Mock<ICosmosDocumentClient> _documentClient = new();
    private readonly Mock<IVaultTokenProviderPort> _vault = new();
    private readonly Mock<IDlqProducerPort> _dlq = new();

    private static readonly CosmosDbCredentials Credentials = new(
        "http://localhost:8081", "key", "db", "audit_logs", "/partitionKey");

    public CosmosDbBulkSinkAdapterTests()
    {
        _vault.Setup(v => v.ResolveCosmosCredentialsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Credentials);
        _dlq.Setup(d => d.SendToDlqAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<IDictionary<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
    }

    private CosmosDbBulkSinkAdapter Build(int retryAttempts = 0)
    {
        var settings = new SinkSettings
        {
            DlqTopic = "tp.observability.application-log.processed.dlq.v1",
            VaultTokenId = "TKN",
            Resilience = new ResilienceSettings { Retry = new RetrySettings { MaxRetryAttempts = retryAttempts, DelaySeconds = 1 } }
        };

        return new CosmosDbBulkSinkAdapter(
            _documentClient.Object,
            _vault.Object,
            _dlq.Object,
            Options.Create(settings),
            new FakeTimeProvider(),
            NullLogger<CosmosDbBulkSinkAdapter>.Instance);
    }

    private static IReadOnlyList<LogSinkItem> Items(int count)
        => Enumerable.Range(0, count).Select(i => new LogSinkItem($"{{\"i\":{i}}}", $"pk-{i}")).ToList();

    [Fact]
    public async Task BulkInsertRawJsonLogsAsync_WhenEmpty_ReturnsZeroedResult()
    {
        var result = await Build().BulkInsertRawJsonLogsAsync([], CancellationToken.None);

        Assert.Equal(0, result.TotalProcessed);
        _vault.Verify(v => v.ResolveCosmosCredentialsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task BulkInsertRawJsonLogsAsync_AllSucceed_AggregatesSuccessAndRequestUnits()
    {
        _documentClient
            .Setup(c => c.UpsertDocumentAsync(It.IsAny<CosmosDbCredentials>(), It.IsAny<string?>(),
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(2.5);

        var result = await Build().BulkInsertRawJsonLogsAsync(Items(4), CancellationToken.None);

        Assert.Equal(4, result.TotalProcessed);
        Assert.Equal(4, result.TotalSuccessful);
        Assert.Equal(0, result.TotalFailed);
        Assert.Equal(10.0, result.RequestUnitsConsumed);
        _dlq.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task BulkInsertRawJsonLogsAsync_NonRetriableFailure_RoutesItemToDlq()
    {
        _documentClient
            .Setup(c => c.UpsertDocumentAsync(It.IsAny<CosmosDbCredentials>(), It.IsAny<string?>(),
                "pk-0", It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("400 Bad Request"));
        _documentClient
            .Setup(c => c.UpsertDocumentAsync(It.IsAny<CosmosDbCredentials>(), It.IsAny<string?>(),
                "pk-1", It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(1.0);

        var result = await Build().BulkInsertRawJsonLogsAsync(Items(2), CancellationToken.None);

        Assert.Equal(1, result.TotalSuccessful);
        Assert.Equal(1, result.TotalFailed);
        Assert.Equal(1, result.TotalDlqSent);
        _dlq.Verify(d => d.SendToDlqAsync(
            "tp.observability.application-log.processed.dlq.v1", "pk-0", It.IsAny<string>(),
            It.IsAny<IDictionary<string, string>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task BulkInsertRawJsonLogsAsync_WhenDlqAlsoFails_CountsFailureButNotDlqSent()
    {
        _documentClient
            .Setup(c => c.UpsertDocumentAsync(It.IsAny<CosmosDbCredentials>(), It.IsAny<string?>(),
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("boom"));
        _dlq.Setup(d => d.SendToDlqAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<IDictionary<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var result = await Build().BulkInsertRawJsonLogsAsync(Items(1), CancellationToken.None);

        Assert.Equal(1, result.TotalFailed);
        Assert.Equal(0, result.TotalDlqSent);
    }

    [Fact]
    public async Task BulkInsertRawJsonLogsAsync_TransientFailure_RetriesThenRoutesToDlq()
    {
        _documentClient
            .Setup(c => c.UpsertDocumentAsync(It.IsAny<CosmosDbCredentials>(), It.IsAny<string?>(),
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new CosmosTransientException("429"));

        var result = await Build(retryAttempts: 1).BulkInsertRawJsonLogsAsync(Items(1), CancellationToken.None);

        Assert.Equal(1, result.TotalFailed);
        Assert.Equal(1, result.TotalDlqSent);
        // 1 intento inicial + 1 reintento
        _documentClient.Verify(c => c.UpsertDocumentAsync(It.IsAny<CosmosDbCredentials>(), It.IsAny<string?>(),
            It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }
}
