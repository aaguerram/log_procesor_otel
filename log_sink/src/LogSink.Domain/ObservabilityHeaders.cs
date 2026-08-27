namespace LogSink.Domain;

/// <summary>
/// Nombres canónicos de las cabeceras Kafka de observabilidad que produce <c>consumer_streams</c>
/// y consume este servicio. Centralizarlas evita literales mágicos repartidos por el código.
/// </summary>
public static class ObservabilityHeaders
{
    public const string TargetCollection = "x-target-collection";
    public const string ServiceName = "x-service-name";
    public const string TelemetryType = "x-telemetry-type";
    public const string ErrorType = "x-error-type";
    public const string ErrorMessage = "x-error-message";
    public const string ErrorTimestamp = "x-error-timestamp";
    public const string RetryAttempts = "x-retry-attempts";
    public const string CircuitState = "x-circuit-state";
    public const string DlqOrigin = "x-dlq-origin";
}
