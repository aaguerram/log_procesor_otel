namespace LogSink.Infrastructure.Configuration;

public class SinkSettings
{
    public const string SectionName = "LogSink";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string SourceTopic { get; set; } = "produbanco-transactions-processed-v1";
    public string GroupId { get; set; } = "log-sink-cosmosdb-group-v1";
    public int BatchSize { get; set; } = 500;
    public int BatchTimeoutMs { get; set; } = 250;

    // Cosmos DB Settings
    public string CosmosEndpoint { get; set; } = "https://azure-documentdb:8081";
    public string CosmosPrimaryKey { get; set; } = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    public string DatabaseName { get; set; } = "ProdubancoObservability";
    public string ContainerName { get; set; } = "audit_logs";
    public string PartitionKeyPath { get; set; } = "/partitionKey";

    // Azure Key Vault
    public string KeyVaultEndpoint { get; set; } = "https://azure-keyvault:8443";
    public string VaultTokenId { get; set; } = "TKN-COSMOS-PRODUBANCO-V1";
}
