using LogSink.Application.UseCases;
using LogSink.Domain;
using LogSink.Domain.Models;
using LogSink.Domain.Ports;
using LogSink.Domain.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace LogSink.Application.Tests.UseCases;

public class BulkSinkPipelineUseCaseTests
{
    private readonly Mock<IBatchConsumerPort> _consumer = new();
    private readonly Mock<IDocumentDbBulkSinkPort> _sink = new();
    private readonly BulkSinkPipelineUseCase _useCase;

    /// <summary>Callback capturado que el caso de uso registra en el puerto de consumo.</summary>
    private Func<IReadOnlyList<KafkaBatchItem>, CancellationToken, Task<bool>>? _batchHandler;

    public BulkSinkPipelineUseCaseTests()
    {
        _consumer
            .Setup(c => c.StartBatchConsumerAsync(
                It.IsAny<string>(), It.IsAny<int>(), It.IsAny<TimeSpan>(),
                It.IsAny<Func<IReadOnlyList<KafkaBatchItem>, CancellationToken, Task<bool>>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, int, TimeSpan, Func<IReadOnlyList<KafkaBatchItem>, CancellationToken, Task<bool>>, CancellationToken>(
                (_, _, _, handler, _) => _batchHandler = handler)
            .Returns(Task.CompletedTask);

        _sink
            .Setup(s => s.BulkInsertRawJsonLogsAsync(It.IsAny<IReadOnlyList<LogSinkItem>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((IReadOnlyList<LogSinkItem> items, CancellationToken _) =>
                new BulkSinkResult(items.Count, items.Count, 0, 0, 1.0, items.Count));

        _useCase = new BulkSinkPipelineUseCase(
            _consumer.Object,
            _sink.Object,
            new TargetCollectionResolver(),
            NullLogger<BulkSinkPipelineUseCase>.Instance);
    }

    private async Task<Func<IReadOnlyList<KafkaBatchItem>, CancellationToken, Task<bool>>> StartAndCaptureHandlerAsync()
    {
        await _useCase.ExecuteBulkSinkPipelineAsync("src.topic", 500, TimeSpan.FromMilliseconds(250), CancellationToken.None);
        Assert.NotNull(_batchHandler);
        return _batchHandler!;
    }

    private static KafkaBatchItem Item(string key, string json, params (string Key, string Value)[] headers)
        => new(key, [], json, 0, 0, headers.ToDictionary(h => h.Key, h => h.Value));

    [Fact]
    public async Task ProcessBatch_WhenEmpty_DoesNotCallSink_AndCommits()
    {
        var handler = await StartAndCaptureHandlerAsync();

        var committed = await handler([], CancellationToken.None);

        Assert.True(committed);
        _sink.Verify(s => s.BulkInsertRawJsonLogsAsync(It.IsAny<IReadOnlyList<LogSinkItem>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ProcessBatch_SkipsItemsWithBlankJson()
    {
        var handler = await StartAndCaptureHandlerAsync();
        IReadOnlyList<LogSinkItem>? sent = null;
        _sink.Setup(s => s.BulkInsertRawJsonLogsAsync(It.IsAny<IReadOnlyList<LogSinkItem>>(), It.IsAny<CancellationToken>()))
            .Callback<IReadOnlyList<LogSinkItem>, CancellationToken>((i, _) => sent = i)
            .ReturnsAsync(new BulkSinkResult(1, 1, 0, 0, 1, 1));

        await handler(
        [
            Item("k1", "{\"a\":1}"),
            Item("k2", "   "),
            Item("k3", "")
        ], CancellationToken.None);

        Assert.NotNull(sent);
        Assert.Single(sent!);
        Assert.Equal("k1", sent![0].PartitionKey);
    }

    [Fact]
    public async Task ProcessBatch_ResolvesTargetCollectionFromHeaders()
    {
        var handler = await StartAndCaptureHandlerAsync();
        IReadOnlyList<LogSinkItem>? sent = null;
        _sink.Setup(s => s.BulkInsertRawJsonLogsAsync(It.IsAny<IReadOnlyList<LogSinkItem>>(), It.IsAny<CancellationToken>()))
            .Callback<IReadOnlyList<LogSinkItem>, CancellationToken>((i, _) => sent = i)
            .ReturnsAsync(new BulkSinkResult(1, 1, 0, 0, 1, 1));

        await handler(
        [
            Item("k1", "{\"a\":1}",
                (ObservabilityHeaders.ServiceName, "Transfer.Mspx.Prometeus.Management"),
                (ObservabilityHeaders.TelemetryType, "Trace"))
        ], CancellationToken.None);

        Assert.Equal("Transfer_Mspx_Prometeus_Management_Trace", sent![0].TargetCollection);
    }

    [Fact]
    public async Task ProcessBatch_DefaultsPartitionKeyWhenMissing()
    {
        var handler = await StartAndCaptureHandlerAsync();
        IReadOnlyList<LogSinkItem>? sent = null;
        _sink.Setup(s => s.BulkInsertRawJsonLogsAsync(It.IsAny<IReadOnlyList<LogSinkItem>>(), It.IsAny<CancellationToken>()))
            .Callback<IReadOnlyList<LogSinkItem>, CancellationToken>((i, _) => sent = i)
            .ReturnsAsync(new BulkSinkResult(1, 1, 0, 0, 1, 1));

        await handler([new KafkaBatchItem(null!, [], "{\"a\":1}", 0, 0, new Dictionary<string, string>())], CancellationToken.None);

        Assert.Equal("default", sent![0].PartitionKey);
    }

    [Fact]
    public async Task ProcessBatch_AlwaysReturnsTrueToCommitOffsets_EvenWithDlqRoutedFailures()
    {
        var handler = await StartAndCaptureHandlerAsync();
        _sink.Setup(s => s.BulkInsertRawJsonLogsAsync(It.IsAny<IReadOnlyList<LogSinkItem>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new BulkSinkResult(2, 1, 1, 1, 5, 1));

        var committed = await handler([Item("k1", "{}"), Item("k2", "{}")], CancellationToken.None);

        Assert.True(committed);
    }
}
