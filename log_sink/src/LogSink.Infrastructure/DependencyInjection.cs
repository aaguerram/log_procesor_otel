using LogSink.Application.UseCases;
using LogSink.Domain.Ports;
using LogSink.Infrastructure.Adapters;
using LogSink.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace LogSink.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddLogSinkInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        // Mapeo dinámico y seguro desde appsettings.json y Variables de Entorno (sin reflexión para Native AOT)
        var sinkSettings = new SinkSettings
        {
            BootstrapServers = configuration["LogSink:BootstrapServers"] 
                ?? configuration["TECH-INT-MSG-KAFKA_BROKERS"] 
                ?? configuration["TECH_INT_MSG_KAFKA_BROKERS"] 
                ?? "kafka:29092",

            SourceTopic = configuration["LogSink:SourceTopic"] 
                ?? configuration["TECH-INT-MSG-LOGS_TOPIC"] 
                ?? configuration["TECH_INT_MSG_LOGS_TOPIC"] 
                ?? "tp.observability.application-log.processed.v1",

            GroupId = configuration["LogSink:GroupId"] 
                ?? configuration["TECH-INT-MSG-LOGS_GROUP"] 
                ?? configuration["TECH_INT_MSG_LOGS_GROUP"] 
                ?? "log-sink-cosmosdb-group-v1",

            BatchSize = int.TryParse(configuration["LogSink:BatchSize"] ?? configuration["TECH-INT-DB-BATCH_SIZE"], out var bs) ? bs : 500,
            BatchTimeoutMs = int.TryParse(configuration["LogSink:BatchTimeoutMs"] ?? configuration["TECH-INT-DB-BATCH_TIMEOUT_MS"], out var bt) ? bt : 250,

            CosmosEndpoint = configuration["LogSink:CosmosEndpoint"] 
                ?? configuration["TECH-INT-DB-AUDI_URL"] 
                ?? configuration["TECH_INT_DB_AUDI_URL"] 
                ?? "http://azure-documentdb:8081",

            CosmosPrimaryKey = configuration["LogSink:CosmosPrimaryKey"] 
                ?? configuration["TECH-INT-DB-AUDI_KEY"] 
                ?? configuration["TECH_INT_DB_AUDI_KEY"] 
                ?? string.Empty,

            DatabaseName = configuration["LogSink:DatabaseName"] 
                ?? configuration["TECH-INT-DB-AUDI_NAME"] 
                ?? configuration["TECH_INT_DB_AUDI_NAME"] 
                ?? "ProdubancoObservability",

            ContainerName = configuration["LogSink:ContainerName"] 
                ?? configuration["TECH-INT-DB-AUDI_COLL"] 
                ?? configuration["TECH_INT_DB_AUDI_COLL"] 
                ?? "audit_logs",

            PartitionKeyPath = configuration["LogSink:PartitionKeyPath"] 
                ?? configuration["TECH-INT-DB-AUDI_PK_PATH"] 
                ?? configuration["TECH_INT_DB_AUDI_PK_PATH"] 
                ?? "/partitionKey",

            KeyVaultEndpoint = configuration["LogSink:KeyVaultEndpoint"] 
                ?? configuration["TECH-INT-SECU-VAULT_URL"] 
                ?? configuration["TECH_INT_SECU_VAULT_URL"] 
                ?? "https://azure-keyvault:8443",

            VaultTokenId = configuration["LogSink:VaultTokenId"] 
                ?? configuration["TECH-INT-SECU-TOKEN_ID"] 
                ?? configuration["TECH_INT_SECU_TOKEN_ID"] 
                ?? "TKN-COSMOS-PRODUBANCO-V1"
        };

        services.AddSingleton(Options.Create(sinkSettings));

        // Registrar Adaptadores y Puertos
        services.AddSingleton<IVaultTokenProviderPort, AzureKeyVaultTokenAdapter>();
        services.AddSingleton<IDocumentDbBulkSinkPort, CosmosDbBulkSinkAdapter>();
        services.AddSingleton<IBatchConsumerPort, KafkaBatchConsumerAdapter>();

        // Registrar Casos de Uso
        services.AddSingleton<BulkSinkPipelineUseCase>();

        return services;
    }
}
