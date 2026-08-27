using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using LogSink.Application.Serialization;
using LogSink.Domain.Models;
using LogSink.Domain.Ports;
using LogSink.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace LogSink.Infrastructure.Adapters;

/// <summary>
/// Excepción para fallos transitorios en Cosmos DB que deben activar reintentos o el Circuit Breaker.
/// </summary>
public class CosmosTransientException(string message, HttpStatusCode? statusCode = null) : Exception(message)
{
    public HttpStatusCode? StatusCode { get; } = statusCode;
}

/// <summary>
/// Adaptador de inserción masiva (Bulk Sink) de alto rendimiento para Azure Cosmos DB / DocumentDB.
/// Incorpora Resiliencia oficial con Polly.Core (.NET 10 Native AOT):
/// - Reintentos configurables (2 reintentos, 1s de espera por defecto).
/// - Circuit Breaker configurable (apertura de circuito para evitar sobrecarga).
/// - Enrutamiento individual a DLQ para cada documento fallido del lote.
/// - Timeout estricto de conexión a Cosmos DB (3 segundos por defecto).
/// </summary>
public class CosmosDbBulkSinkAdapter : IDocumentDbBulkSinkPort
{
    private readonly HttpClient _httpClient;
    private readonly IVaultTokenProviderPort _vaultTokenPort;
    private readonly IDlqProducerPort _dlqPort;
    private readonly SinkSettings _settings;
    private readonly ILogger<CosmosDbBulkSinkAdapter> _logger;
    private readonly SemaphoreSlim _concurrencySemaphore = new(100);
    private readonly object _metricsLock = new();
    private readonly ResiliencePipeline _resiliencePipeline;

    public CosmosDbBulkSinkAdapter(
        IVaultTokenProviderPort vaultTokenPort,
        IDlqProducerPort dlqPort,
        IOptions<SinkSettings> settings,
        ILogger<CosmosDbBulkSinkAdapter> logger)
    {
        _vaultTokenPort = vaultTokenPort;
        _dlqPort = dlqPort;
        _settings = settings.Value;
        _logger = logger;

        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(15),
            MaxConnectionsPerServer = 200,
            ConnectTimeout = TimeSpan.FromSeconds(_settings.CosmosTimeoutSeconds > 0 ? _settings.CosmosTimeoutSeconds : 3),
            SslOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = delegate { return true; } // Soporte SSL para emulador local Cosmos DB
            }
        };

        // Timeout estricto de 3 segundos para operaciones hacia Cosmos DB
        var timeoutSeconds = _settings.CosmosTimeoutSeconds > 0 ? _settings.CosmosTimeoutSeconds : 3;
        _httpClient = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(timeoutSeconds),
            DefaultRequestVersion = HttpVersion.Version11
        };

        // Construcción del Pipeline de Resiliencia Polly.Core (Native AOT Compatible)
        var retryOpts = _settings.Resilience.Retry;
        var cbOpts = _settings.Resilience.CircuitBreaker;

        var pipelineBuilder = new ResiliencePipelineBuilder();

        // 1. Estrategia de Reintentos: 2 intentos, 1s de espera cada uno
        pipelineBuilder.AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = retryOpts.MaxRetryAttempts > 0 ? retryOpts.MaxRetryAttempts : 2,
            Delay = TimeSpan.FromSeconds(retryOpts.DelaySeconds > 0 ? retryOpts.DelaySeconds : 1),
            BackoffType = DelayBackoffType.Constant,
            ShouldHandle = new PredicateBuilder()
                .Handle<HttpRequestException>()
                .Handle<TimeoutException>()
                .Handle<CosmosTransientException>()
                .Handle<TaskCanceledException>(ex => !ex.CancellationToken.IsCancellationRequested),
            OnRetry = args =>
            {
                _logger.LogWarning("⚠️ [RETRY #{Attempt}] Reintentando inserción en Cosmos DB tras fallo transitorio. Espera: {Delay}s. Causa: {Error}",
                    args.AttemptNumber + 1, args.RetryDelay.TotalSeconds, args.Outcome.Exception?.Message ?? "Error no tipificado");
                return default;
            }
        });

        // 2. Estrategia de Circuit Breaker: Apertura ante tasa de fallos repetidos hacia Cosmos DB
        pipelineBuilder.AddCircuitBreaker(new CircuitBreakerStrategyOptions
        {
            FailureRatio = cbOpts.FailureRatio > 0 ? cbOpts.FailureRatio : 0.5,
            SamplingDuration = TimeSpan.FromSeconds(cbOpts.SamplingDurationSeconds > 0 ? cbOpts.SamplingDurationSeconds : 10),
            MinimumThroughput = cbOpts.MinimumThroughput > 0 ? cbOpts.MinimumThroughput : 4,
            BreakDuration = TimeSpan.FromSeconds(cbOpts.BreakDurationSeconds > 0 ? cbOpts.BreakDurationSeconds : 15),
            ShouldHandle = new PredicateBuilder()
                .Handle<HttpRequestException>()
                .Handle<TimeoutException>()
                .Handle<CosmosTransientException>()
                .Handle<TaskCanceledException>(ex => !ex.CancellationToken.IsCancellationRequested),
            OnOpened = args =>
            {
                _logger.LogCritical("🔴 [CIRCUIT BREAKER OPEN] El circuito hacia Azure Cosmos DB se ha ABIERTO por {BreakDuration}s tras tasa de fallos excesiva. Los mensajes serán derivados directamente a DLQ.",
                    args.BreakDuration.TotalSeconds);
                return default;
            },
            OnClosed = _ =>
            {
                _logger.LogInformation("🟢 [CIRCUIT BREAKER CLOSED] El circuito hacia Azure Cosmos DB se ha RESTABLECIDO y está CERRADO. Inserciones normales reanudadas.");
                return default;
            },
            OnHalfOpened = _ =>
            {
                _logger.LogWarning("🟡 [CIRCUIT BREAKER HALF-OPEN] El circuito hacia Azure Cosmos DB está en prueba (HALF-OPEN). Evaluando recuperación de Cosmos...");
                return default;
            }
        });

        _resiliencePipeline = pipelineBuilder.Build();
        _logger.LogInformation("Pipeline de Resiliencia inicializado: Timeout={Timeout}s, Reintentos={Retries}x{Delay}s, CircuitBreaker(Break={Break}s, Ratio={Ratio})",
            timeoutSeconds, retryOpts.MaxRetryAttempts, retryOpts.DelaySeconds, cbOpts.BreakDurationSeconds, cbOpts.FailureRatio);
    }

    public async Task<BulkSinkResult> BulkInsertLogsAsync(
        IReadOnlyList<LogDocument> documents,
        CancellationToken cancellationToken = default)
    {
        if (documents.Count == 0)
        {
            return new BulkSinkResult(0, 0, 0, 0, 0);
        }

        var items = documents.Select(d => new LogSinkItem(
            RawJson: JsonSerializer.Serialize(d, SinkJsonContext.Default.LogDocument),
            PartitionKey: d.PartitionKey,
            TargetCollection: _settings.ContainerName)).ToList();

        return await BulkInsertRawJsonLogsAsync(items, cancellationToken);
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

        int successfulCount = 0;
        int failedCount = 0;
        int dlqSentCount = 0;
        double totalRUs = 0;

        var parallelTasks = new Task[items.Count];

        for (int i = 0; i < items.Count; i++)
        {
            var item = items[i];
            parallelTasks[i] = Task.Run(async () =>
            {
                await _concurrencySemaphore.WaitAsync(cancellationToken);
                try
                {
                    // Ejecutar inserción protegida por la Resiliencia (Reintentos + Circuit Breaker)
                    var ru = await _resiliencePipeline.ExecuteAsync(
                        async state => await ExecuteCosmosInsertAsync(item, credentials, state),
                        cancellationToken);

                    Interlocked.Increment(ref successfulCount);
                    lock (_metricsLock)
                    {
                        totalRUs += ru;
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref failedCount);
                    _logger.LogError(ex, "❌ Fallo en inserción para PartitionKey '{Key}' hacia colección '{Col}'. Redirigiendo a DLQ...",
                        item.PartitionKey, item.TargetCollection ?? credentials.ContainerName);

                    // Envío individual e independiente a la cola DLQ
                    bool sentToDlq = await SendFailedItemToDlqAsync(item, ex, credentials, cancellationToken);
                    if (sentToDlq)
                    {
                        Interlocked.Increment(ref dlqSentCount);
                    }
                }
                finally
                {
                    _concurrencySemaphore.Release();
                }
            }, cancellationToken);
        }

        await Task.WhenAll(parallelTasks);
        stopwatch.Stop();

        return new BulkSinkResult(
            TotalProcessed: items.Count,
            TotalSuccessful: successfulCount,
            TotalFailed: failedCount,
            TotalDlqSent: dlqSentCount,
            ElapsedMilliseconds: stopwatch.Elapsed.TotalMilliseconds,
            RequestUnitsConsumed: totalRUs);
    }

    private async Task<double> ExecuteCosmosInsertAsync(
        LogSinkItem item,
        CosmosDbCredentials credentials,
        CancellationToken cancellationToken)
    {
        var effectiveCollection = !string.IsNullOrWhiteSpace(item.TargetCollection)
            ? item.TargetCollection
            : credentials.ContainerName;

        var jsonBytes = Encoding.UTF8.GetBytes(item.RawJson);
        var resourceLink = $"dbs/{credentials.DatabaseName}/colls/{effectiveCollection}";
        var resourceUri = $"{credentials.Endpoint.TrimEnd('/')}/{resourceLink}/docs";

        using var request = new HttpRequestMessage(HttpMethod.Post, resourceUri);
        request.Content = new ByteArrayContent(jsonBytes);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

        var dateHeader = DateTime.UtcNow.ToString("r");
        request.Headers.Add("x-ms-date", dateHeader);
        request.Headers.Add("x-ms-version", "2018-12-31");
        request.Headers.Add("x-ms-documentdb-is-upsert", "True");
        request.Headers.Add("x-ms-documentdb-partitionkey", $"[\"{item.PartitionKey}\"]");

        var authHeader = GenerateCosmosAuthToken("POST", "docs", resourceLink, dateHeader, credentials.PrimaryKey);
        request.Headers.Add("authorization", authHeader);

        using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

        double ruCharge = 1.0;
        if (response.Headers.TryGetValues("x-ms-request-charge", out var ruValues) &&
            double.TryParse(ruValues.FirstOrDefault(), out var parsedRu))
        {
            ruCharge = parsedRu;
        }

        if (response.IsSuccessStatusCode)
        {
            return ruCharge;
        }

        // Manejo de Throttling 429 o Errores de Servidor (500, 502, 503, 504) -> Disparan Reintento
        if (response.StatusCode == (HttpStatusCode)429 || (int)response.StatusCode >= 500)
        {
            throw new CosmosTransientException(
                $"Cosmos DB devolvió código transitorio {(int)response.StatusCode} ({response.ReasonPhrase})",
                response.StatusCode);
        }

        // Cualquier otro error HTTP no exitoso (400, 403, 404, etc.)
        throw new InvalidOperationException($"Fallo en inserción Cosmos DB con código {(int)response.StatusCode}: {response.ReasonPhrase}");
    }

    private async Task<bool> SendFailedItemToDlqAsync(
        LogSinkItem item,
        Exception ex,
        CosmosDbCredentials credentials,
        CancellationToken cancellationToken)
    {
        try
        {
            var effectiveDlqTopic = !string.IsNullOrWhiteSpace(_settings.DlqTopic)
                ? _settings.DlqTopic
                : "tp.observability.application-log.processed.dlq.v1";

            var headers = new Dictionary<string, string>
            {
                ["x-error-type"] = ex.GetType().Name,
                ["x-error-message"] = ex.Message,
                ["x-error-timestamp"] = DateTime.UtcNow.ToString("O"),
                ["x-retry-attempts"] = _settings.Resilience.Retry.MaxRetryAttempts.ToString(),
                ["x-target-collection"] = item.TargetCollection ?? credentials.ContainerName,
                ["x-circuit-state"] = ex is BrokenCircuitException ? "OPEN" : "ACTIVE",
                ["x-dlq-origin"] = "LogSink.CosmosDbBulkSinkAdapter"
            };

            return await _dlqPort.SendToDlqAsync(
                effectiveDlqTopic,
                item.PartitionKey,
                item.RawJson,
                headers,
                cancellationToken);
        }
        catch (Exception dlqEx)
        {
            _logger.LogCritical(dlqEx, "❌ [FATAL DLQ ERROR] Error al enviar documento a la DLQ '{Topic}'", _settings.DlqTopic);
            return false;
        }
    }

    private static string GenerateCosmosAuthToken(string verb, string resourceType, string resourceLink, string date, string key)
    {
        try
        {
            var keyBytes = Convert.FromBase64String(key);
            var payload = $"{verb.ToLowerInvariant()}\n{resourceType.ToLowerInvariant()}\n{resourceLink}\n{date.ToLowerInvariant()}\n\n";
            using var hmac = new HMACSHA256(keyBytes);
            var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(payload));
            var signature = Convert.ToBase64String(hash);
            return Uri.EscapeDataString($"type=master&ver=1.0&sig={signature}");
        }
        catch
        {
            return "type=master&ver=1.0&sig=simulated";
        }
    }
}
