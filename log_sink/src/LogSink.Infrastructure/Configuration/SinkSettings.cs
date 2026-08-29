namespace LogSink.Infrastructure.Configuration;

/// <summary>
/// Opciones de configuración para el microservicio Bulk Sink.
/// Las propiedades son desacopladas y no contienen valores quemados;
/// se pueblan dinámicamente desde IConfiguration (appsettings.json / variables de entorno).
/// </summary>
public class SinkSettings
{
    public const string SectionName = "LogSink";

    // Kafka Settings
    public string BootstrapServers { get; set; } = string.Empty;
    public string SourceTopic { get; set; } = string.Empty;
    public string DlqTopic { get; set; } = "tp.observability.application-log.processed.dlq.v1";
    public string GroupId { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 500;
    public int BatchTimeoutMs { get; set; } = 250;

    // Cosmos DB Settings (Timeout 3s por conexión)
    public string CosmosEndpoint { get; set; } = string.Empty;
    public string CosmosPrimaryKey { get; set; } = string.Empty;
    public string DatabaseName { get; set; } = string.Empty;
    public string ContainerName { get; set; } = string.Empty;
    public string PartitionKeyPath { get; set; } = string.Empty;
    public int CosmosTimeoutSeconds { get; set; } = 3;

    /// <summary>
    /// Cuando es <c>true</c>, tolera el certificado autofirmado del emulador local de Cosmos DB
    /// sobre HTTPS. Debe permanecer en <c>false</c> (el valor por defecto) fuera de entornos de
    /// desarrollo: en ese caso se aplica la validación estándar de certificado de servidor.
    /// </summary>
    public bool AllowUntrustedCertificates { get; set; }

    // Azure Key Vault Settings
    public string KeyVaultEndpoint { get; set; } = string.Empty;
    public string VaultTokenId { get; set; } = string.Empty;

    // Resilience Settings (Polly.Core v8 Native AOT)
    public ResilienceSettings Resilience { get; set; } = new();
}

public class ResilienceSettings
{
    public RetrySettings Retry { get; set; } = new();
    public CircuitBreakerSettings CircuitBreaker { get; set; } = new();
}

public class RetrySettings
{
    public int MaxRetryAttempts { get; set; } = 2;
    public int DelaySeconds { get; set; } = 1;
}

public class CircuitBreakerSettings
{
    public double FailureRatio { get; set; } = 0.5;
    public int SamplingDurationSeconds { get; set; } = 10;
    public int MinimumThroughput { get; set; } = 4;
    public int BreakDurationSeconds { get; set; } = 15;
}
