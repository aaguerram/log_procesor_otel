using LogSink.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace LogSink.Infrastructure.Cosmos;

/// <summary>
/// Construye el <see cref="ResiliencePipeline"/> (Reintentos + Circuit Breaker) para las
/// operaciones contra Cosmos DB. Aislado del adaptador para respetar responsabilidad única
/// y poder ajustar la política de resiliencia sin tocar la lógica de inserción.
/// </summary>
public static class CosmosDbResiliencePipelineFactory
{
    public static ResiliencePipeline Create(ResilienceSettings settings, ILogger logger)
    {
        var retry = settings.Retry;
        var breaker = settings.CircuitBreaker;

        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = retry.MaxRetryAttempts > 0 ? retry.MaxRetryAttempts : 2,
                Delay = TimeSpan.FromSeconds(retry.DelaySeconds > 0 ? retry.DelaySeconds : 1),
                BackoffType = DelayBackoffType.Constant,
                ShouldHandle = new PredicateBuilder()
                    .Handle<HttpRequestException>()
                    .Handle<TimeoutException>()
                    .Handle<CosmosTransientException>()
                    .Handle<TaskCanceledException>(ex => !ex.CancellationToken.IsCancellationRequested),
                OnRetry = args =>
                {
                    logger.LogWarning("⚠️ [RETRY #{Attempt}] Reintentando inserción en Cosmos DB tras fallo transitorio. Espera: {Delay}s. Causa: {Error}",
                        args.AttemptNumber + 1, args.RetryDelay.TotalSeconds, args.Outcome.Exception?.Message ?? "Error no tipificado");
                    return default;
                }
            })
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = breaker.FailureRatio > 0 ? breaker.FailureRatio : 0.5,
                SamplingDuration = TimeSpan.FromSeconds(breaker.SamplingDurationSeconds > 0 ? breaker.SamplingDurationSeconds : 10),
                MinimumThroughput = breaker.MinimumThroughput > 0 ? breaker.MinimumThroughput : 4,
                BreakDuration = TimeSpan.FromSeconds(breaker.BreakDurationSeconds > 0 ? breaker.BreakDurationSeconds : 15),
                ShouldHandle = new PredicateBuilder()
                    .Handle<HttpRequestException>()
                    .Handle<TimeoutException>()
                    .Handle<CosmosTransientException>()
                    .Handle<TaskCanceledException>(ex => !ex.CancellationToken.IsCancellationRequested),
                OnOpened = args =>
                {
                    logger.LogCritical("🔴 [CIRCUIT BREAKER OPEN] El circuito hacia Azure Cosmos DB se ha ABIERTO por {BreakDuration}s. Los mensajes serán derivados directamente a DLQ.",
                        args.BreakDuration.TotalSeconds);
                    return default;
                },
                OnClosed = _ =>
                {
                    logger.LogInformation("🟢 [CIRCUIT BREAKER CLOSED] El circuito hacia Azure Cosmos DB se ha RESTABLECIDO. Inserciones normales reanudadas.");
                    return default;
                },
                OnHalfOpened = _ =>
                {
                    logger.LogWarning("🟡 [CIRCUIT BREAKER HALF-OPEN] Evaluando recuperación de Cosmos DB...");
                    return default;
                }
            })
            .Build();
    }
}
