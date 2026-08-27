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
        // 1. Mapeo dinámico y estricto desde IConfiguration (appsettings.json / Variables de Entorno)
        var bootstrapServers = configuration["LogSink:BootstrapServers"] 
            ?? configuration["TECH-INT-MSG-KAFKA_BROKERS"] 
            ?? configuration["TECH_INT_MSG_KAFKA_BROKERS"];

        var sourceTopic = configuration["LogSink:SourceTopic"] 
            ?? configuration["TECH-INT-MSG-LOGS_TOPIC"] 
            ?? configuration["TECH_INT_MSG_LOGS_TOPIC"];

        var groupId = configuration["LogSink:GroupId"] 
            ?? configuration["TECH-INT-MSG-LOGS_GROUP"] 
            ?? configuration["TECH_INT_MSG_LOGS_GROUP"];

        var cosmosEndpoint = configuration["LogSink:CosmosEndpoint"] 
            ?? configuration["TECH-INT-DB-AUDI_URL"] 
            ?? configuration["TECH_INT_DB_AUDI_URL"];

        var databaseName = configuration["LogSink:DatabaseName"] 
            ?? configuration["TECH-INT-DB-AUDI_NAME"] 
            ?? configuration["TECH_INT_DB_AUDI_NAME"];

        var containerName = configuration["LogSink:ContainerName"] 
            ?? configuration["TECH-INT-DB-AUDI_COLL"] 
            ?? configuration["TECH_INT_DB_AUDI_COLL"];

        var keyVaultEndpoint = configuration["LogSink:KeyVaultEndpoint"] 
            ?? configuration["TECH-INT-SECU-VAULT_URL"] 
            ?? configuration["TECH_INT_SECU_VAULT_URL"];

        var vaultTokenId = configuration["LogSink:VaultTokenId"] 
            ?? configuration["TECH-INT-SECU-TOKEN_ID"] 
            ?? configuration["TECH_INT_SECU_TOKEN_ID"];

        // Validación Fail-Fast: Si falta alguna configuración esencial, la aplicación falla al iniciar
        if (string.IsNullOrWhiteSpace(bootstrapServers))
            throw new InvalidOperationException("[CONFIG ERROR] 'LogSink:BootstrapServers' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(sourceTopic))
            throw new InvalidOperationException("[CONFIG ERROR] 'LogSink:SourceTopic' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(groupId))
            throw new InvalidOperationException("[CONFIG ERROR] 'LogSink:GroupId' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(cosmosEndpoint))
            throw new InvalidOperationException("[CONFIG ERROR] 'LogSink:CosmosEndpoint' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(databaseName))
            throw new InvalidOperationException("[CONFIG ERROR] 'LogSink:DatabaseName' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(containerName))
            throw new InvalidOperationException("[CONFIG ERROR] 'LogSink:ContainerName' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(keyVaultEndpoint))
            throw new InvalidOperationException("[CONFIG ERROR] 'LogSink:KeyVaultEndpoint' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(vaultTokenId))
            throw new InvalidOperationException("[CONFIG ERROR] 'LogSink:VaultTokenId' no está configurado en appsettings.json ni en las variables de entorno.");

        var dlqTopic = configuration["LogSink:DlqTopic"]
            ?? configuration["TECH-INT-MSG-DLQ_TOPIC"]
            ?? "tp.observability.application-log.processed.dlq.v1";

        var sinkSettings = new SinkSettings
        {
            BootstrapServers = bootstrapServers,
            SourceTopic = sourceTopic,
            DlqTopic = dlqTopic,
            GroupId = groupId,
            BatchSize = int.TryParse(configuration["LogSink:BatchSize"] ?? configuration["TECH-INT-DB-BATCH_SIZE"], out var bs) ? bs : 500,
            BatchTimeoutMs = int.TryParse(configuration["LogSink:BatchTimeoutMs"] ?? configuration["TECH-INT-DB-BATCH_TIMEOUT_MS"], out var bt) ? bt : 250,
            CosmosEndpoint = cosmosEndpoint,
            CosmosPrimaryKey = configuration["LogSink:CosmosPrimaryKey"] ?? configuration["TECH-INT-DB-AUDI_KEY"] ?? string.Empty,
            DatabaseName = databaseName,
            ContainerName = containerName,
            PartitionKeyPath = configuration["LogSink:PartitionKeyPath"] ?? configuration["TECH-INT-DB-AUDI_PK_PATH"] ?? "/partitionKey",
            CosmosTimeoutSeconds = int.TryParse(configuration["LogSink:CosmosTimeoutSeconds"] ?? configuration["COSMOS_TIMEOUT_SECONDS"], out var cts) ? cts : 3,
            KeyVaultEndpoint = keyVaultEndpoint,
            VaultTokenId = vaultTokenId,
            Resilience = new ResilienceSettings
            {
                Retry = new RetrySettings
                {
                    MaxRetryAttempts = int.TryParse(configuration["LogSink:Resilience:Retry:MaxRetryAttempts"], out var mra) ? mra : 2,
                    DelaySeconds = int.TryParse(configuration["LogSink:Resilience:Retry:DelaySeconds"], out var ds) ? ds : 1
                },
                CircuitBreaker = new CircuitBreakerSettings
                {
                    FailureRatio = double.TryParse(configuration["LogSink:Resilience:CircuitBreaker:FailureRatio"], out var fr) ? fr : 0.5,
                    SamplingDurationSeconds = int.TryParse(configuration["LogSink:Resilience:CircuitBreaker:SamplingDurationSeconds"], out var sds) ? sds : 10,
                    MinimumThroughput = int.TryParse(configuration["LogSink:Resilience:CircuitBreaker:MinimumThroughput"], out var mt) ? mt : 4,
                    BreakDurationSeconds = int.TryParse(configuration["LogSink:Resilience:CircuitBreaker:BreakDurationSeconds"], out var bds) ? bds : 15
                }
            }
        };

        services.AddSingleton(Options.Create(sinkSettings));

        // Registrar Adaptadores y Puertos
        services.AddSingleton<IVaultTokenProviderPort, AzureKeyVaultTokenAdapter>();
        services.AddSingleton<IDlqProducerPort, KafkaDlqProducerAdapter>();
        services.AddSingleton<IDocumentDbBulkSinkPort, CosmosDbBulkSinkAdapter>();
        services.AddSingleton<IBatchConsumerPort, KafkaBatchConsumerAdapter>();

        // Registrar Casos de Uso
        services.AddSingleton<BulkSinkPipelineUseCase>();

        return services;
    }
}
