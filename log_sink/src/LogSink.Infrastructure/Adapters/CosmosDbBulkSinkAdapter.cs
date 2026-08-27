using System.Diagnostics;
using LogSink.Domain;
using LogSink.Domain.Models;
using LogSink.Domain.Ports;
using LogSink.Infrastructure.Configuration;
using LogSink.Infrastructure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.CircuitBreaker;

namespace LogSink.Infrastructure.Adapters;

/// <summary>
/// Adaptador de inserción masiva (Bulk Sink) hacia Azure Cosmos DB / DocumentDB.
/// Orquesta el fan-out paralelo controlado por semáforo, aplica la política de resiliencia
/// (<see cref="CosmosDbResiliencePipelineFactory"/>), agrega métricas y deriva cada documento
/// fallido a la DLQ de forma independiente. La E/S HTTP concreta vive en <see cref="ICosmosDocumentClient"/>.
/// </summary>
public sealed class CosmosDbBulkSinkAdapter : IDocumentDbBulkSinkPort
{
    private const int MaxParallelInserts = 100;

    private readonly ICosmosDocumentClient _documentClient;
    private readonly IVaultTokenProviderPort _vaultTokenPort;
    private readonly IDlqProducerPort _dlqPort;
    private readonly SinkSettings _settings;
    private readonly ILogger<CosmosDbBulkSinkAdapter> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly ResiliencePipeline _resiliencePipeline;

    public CosmosDbBulkSinkAdapter(
        ICosmosDocumentClient documentClient,
        IVaultTokenProviderPort vaultTokenPort,
        IDlqProducerPort dlqPort,
        IOptions<SinkSettings> settings,
        TimeProvider timeProvider,
        ILogger<CosmosDbBulkSinkAdapter> logger)
    {
        _documentClient = documentClient;
        _vaultTokenPort = vaultTokenPort;
        _dlqPort = dlqPort;
        _settings = settings.Value;
        _timeProvider = timeProvider;
        _logger = logger;
        _resiliencePipeline = CosmosDbResiliencePipelineFactory.Create(_settings.Resilience, logger);
    }

    public async Task<BulkSinkResult> BulkInsertRawJsonLogsAsync(
        IReadOnlyList<LogSinkItem> items,
        CancellationToken cancellationToken = default)
    {
        if (items.Count == 0)
        {
            return new BulkSinkResult(0, 0, 0, 0, 0);
        }

        var stopwatch = Stopwatch.StartNew();
        var credentials = await _vaultTokenPort.ResolveCosmosCredentialsAsync(_settings.VaultTokenId, cancellationToken);

        var metrics = new BatchMetrics();
        using var gate = new SemaphoreSlim(MaxParallelInserts);

        var insertions = items.Select(item => InsertOneAsync(item, credentials, metrics, gate, cancellationToken));
        await Task.WhenAll(insertions);

        stopwatch.Stop();

        return new BulkSinkResult(
            TotalProcessed: items.Count,
            TotalSuccessful: metrics.Successful,
            TotalFailed: metrics.Failed,
            TotalDlqSent: metrics.DlqSent,
            ElapsedMilliseconds: stopwatch.Elapsed.TotalMilliseconds,
            RequestUnitsConsumed: metrics.RequestUnits);
    }

    private async Task InsertOneAsync(
        LogSinkItem item,
        CosmosDbCredentials credentials,
        BatchMetrics metrics,
        SemaphoreSlim gate,
        CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken);
        try
        {
            var requestUnits = await _resiliencePipeline.ExecuteAsync(
                async token => await _documentClient.UpsertDocumentAsync(
                    credentials, item.TargetCollection, item.PartitionKey, item.RawJson, token),
                cancellationToken);

            metrics.RecordSuccess(requestUnits);
        }
        catch (Exception ex)
        {
            metrics.RecordFailure();
            _logger.LogError(ex, "❌ Fallo en inserción para PartitionKey '{Key}' hacia colección '{Col}'. Redirigiendo a DLQ...",
                item.PartitionKey, item.TargetCollection ?? credentials.ContainerName);

            if (await SendFailedItemToDlqAsync(item, ex, credentials, cancellationToken))
            {
                metrics.RecordDlqSent();
            }
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task<bool> SendFailedItemToDlqAsync(
        LogSinkItem item,
        Exception failure,
        CosmosDbCredentials credentials,
        CancellationToken cancellationToken)
    {
        try
        {
            var headers = new Dictionary<string, string>
            {
                [ObservabilityHeaders.ErrorType] = failure.GetType().Name,
                [ObservabilityHeaders.ErrorMessage] = failure.Message,
                [ObservabilityHeaders.ErrorTimestamp] = _timeProvider.GetUtcNow().ToString("O"),
                [ObservabilityHeaders.RetryAttempts] = _settings.Resilience.Retry.MaxRetryAttempts.ToString(),
                [ObservabilityHeaders.TargetCollection] = item.TargetCollection ?? credentials.ContainerName,
                [ObservabilityHeaders.CircuitState] = failure is BrokenCircuitException ? "OPEN" : "ACTIVE",
                [ObservabilityHeaders.DlqOrigin] = "LogSink.CosmosDbBulkSinkAdapter"
            };

            return await _dlqPort.SendToDlqAsync(_settings.DlqTopic, item.PartitionKey, item.RawJson, headers, cancellationToken);
        }
        catch (Exception dlqFailure)
        {
            _logger.LogCritical(dlqFailure, "❌ [FATAL DLQ ERROR] Error al enviar documento a la DLQ '{Topic}'", _settings.DlqTopic);
            return false;
        }
    }

    /// <summary>Contadores thread-safe del resultado de un lote.</summary>
    private sealed class BatchMetrics
    {
        private int _successful;
        private int _failed;
        private int _dlqSent;
        private double _requestUnits;
        private readonly Lock _sync = new();

        public int Successful => Volatile.Read(ref _successful);
        public int Failed => Volatile.Read(ref _failed);
        public int DlqSent => Volatile.Read(ref _dlqSent);
        public double RequestUnits { get { lock (_sync) { return _requestUnits; } } }

        public void RecordSuccess(double requestUnits)
        {
            Interlocked.Increment(ref _successful);
            lock (_sync) { _requestUnits += requestUnits; }
        }

        public void RecordFailure() => Interlocked.Increment(ref _failed);

        public void RecordDlqSent() => Interlocked.Increment(ref _dlqSent);
    }
}
