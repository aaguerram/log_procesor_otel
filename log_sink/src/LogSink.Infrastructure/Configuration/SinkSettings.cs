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
    public string GroupId { get; set; } = string.Empty;
    public int BatchSize { get; set; }
    public int BatchTimeoutMs { get; set; }

    // Cosmos DB Settings
    public string CosmosEndpoint { get; set; } = string.Empty;
    public string CosmosPrimaryKey { get; set; } = string.Empty;
    public string DatabaseName { get; set; } = string.Empty;
    public string ContainerName { get; set; } = string.Empty;
    public string PartitionKeyPath { get; set; } = string.Empty;

    // Azure Key Vault Settings
    public string KeyVaultEndpoint { get; set; } = string.Empty;
    public string VaultTokenId { get; set; } = string.Empty;
}
