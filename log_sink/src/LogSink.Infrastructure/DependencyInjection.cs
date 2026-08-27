using System.Net;
using System.Net.Security;
using LogSink.Application.UseCases;
using LogSink.Domain.Ports;
using LogSink.Domain.Services;
using LogSink.Infrastructure.Adapters;
using LogSink.Infrastructure.Configuration;
using LogSink.Infrastructure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace LogSink.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddLogSinkInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        var sinkSettings = BuildSinkSettings(configuration);
        services.AddSingleton(Options.Create(sinkSettings));

        services.AddSingleton(TimeProvider.System);

        // Servicios de dominio (lógica pura)
        services.AddSingleton<TargetCollectionResolver>();

        // Cosmos DB: firma de token, cliente HTTP de bajo nivel y adaptador de bulk sink
        services.AddSingleton<ICosmosResourceTokenFactory, CosmosResourceTokenFactory>();
        services.AddSingleton<ICosmosDocumentClient>(sp => new CosmosDocumentClient(
            CreateCosmosHttpClient(sinkSettings),
            sp.GetRequiredService<ICosmosResourceTokenFactory>(),
            sp.GetRequiredService<TimeProvider>()));

        // Puertos y adaptadores (Hexagonal)
        services.AddSingleton<IVaultTokenProviderPort, AzureKeyVaultTokenAdapter>();
        services.AddSingleton<IDlqProducerPort, KafkaDlqProducerAdapter>();
        services.AddSingleton<IDocumentDbBulkSinkPort, CosmosDbBulkSinkAdapter>();
        services.AddSingleton<IBatchConsumerPort, KafkaBatchConsumerAdapter>();

        // Casos de uso
        services.AddSingleton<BulkSinkPipelineUseCase>();

        return services;
    }

    private static SinkSettings BuildSinkSettings(IConfiguration configuration)
    {
        return new SinkSettings
        {
            BootstrapServers = configuration.Required("LogSink:BootstrapServers",
                "LogSink:BootstrapServers", "TECH-INT-MSG-KAFKA_BROKERS", "TECH_INT_MSG_KAFKA_BROKERS"),
            SourceTopic = configuration.Required("LogSink:SourceTopic",
                "LogSink:SourceTopic", "TECH-INT-MSG-LOGS_TOPIC", "TECH_INT_MSG_LOGS_TOPIC"),
            GroupId = configuration.Required("LogSink:GroupId",
                "LogSink:GroupId", "TECH-INT-MSG-LOGS_GROUP", "TECH_INT_MSG_LOGS_GROUP"),
            DlqTopic = configuration.ValueOrDefault("tp.observability.application-log.processed.dlq.v1",
                "LogSink:DlqTopic", "TECH-INT-MSG-DLQ_TOPIC"),
            BatchSize = configuration.IntOrDefault(500, "LogSink:BatchSize", "TECH-INT-DB-BATCH_SIZE"),
            BatchTimeoutMs = configuration.IntOrDefault(250, "LogSink:BatchTimeoutMs", "TECH-INT-DB-BATCH_TIMEOUT_MS"),
            CosmosEndpoint = configuration.Required("LogSink:CosmosEndpoint",
                "LogSink:CosmosEndpoint", "TECH-INT-DB-AUDI_URL", "TECH_INT_DB_AUDI_URL"),
            CosmosPrimaryKey = configuration.ValueOrDefault(string.Empty,
                "LogSink:CosmosPrimaryKey", "TECH-INT-DB-AUDI_KEY"),
            DatabaseName = configuration.Required("LogSink:DatabaseName",
                "LogSink:DatabaseName", "TECH-INT-DB-AUDI_NAME", "TECH_INT_DB_AUDI_NAME"),
            ContainerName = configuration.Required("LogSink:ContainerName",
                "LogSink:ContainerName", "TECH-INT-DB-AUDI_COLL", "TECH_INT_DB_AUDI_COLL"),
            PartitionKeyPath = configuration.ValueOrDefault("/partitionKey",
                "LogSink:PartitionKeyPath", "TECH-INT-DB-AUDI_PK_PATH"),
            CosmosTimeoutSeconds = configuration.IntOrDefault(3, "LogSink:CosmosTimeoutSeconds", "COSMOS_TIMEOUT_SECONDS"),
            KeyVaultEndpoint = configuration.Required("LogSink:KeyVaultEndpoint",
                "LogSink:KeyVaultEndpoint", "TECH-INT-SECU-VAULT_URL", "TECH_INT_SECU_VAULT_URL"),
            VaultTokenId = configuration.Required("LogSink:VaultTokenId",
                "LogSink:VaultTokenId", "TECH-INT-SECU-TOKEN_ID", "TECH_INT_SECU_TOKEN_ID"),
            Resilience = new ResilienceSettings
            {
                Retry = new RetrySettings
                {
                    MaxRetryAttempts = configuration.IntOrDefault(2, "LogSink:Resilience:Retry:MaxRetryAttempts"),
                    DelaySeconds = configuration.IntOrDefault(1, "LogSink:Resilience:Retry:DelaySeconds")
                },
                CircuitBreaker = new CircuitBreakerSettings
                {
                    FailureRatio = configuration.DoubleOrDefault(0.5, "LogSink:Resilience:CircuitBreaker:FailureRatio"),
                    SamplingDurationSeconds = configuration.IntOrDefault(10, "LogSink:Resilience:CircuitBreaker:SamplingDurationSeconds"),
                    MinimumThroughput = configuration.IntOrDefault(4, "LogSink:Resilience:CircuitBreaker:MinimumThroughput"),
                    BreakDurationSeconds = configuration.IntOrDefault(15, "LogSink:Resilience:CircuitBreaker:BreakDurationSeconds")
                }
            }
        };
    }

    private static HttpClient CreateCosmosHttpClient(SinkSettings settings)
    {
        var timeoutSeconds = settings.CosmosTimeoutSeconds > 0 ? settings.CosmosTimeoutSeconds : 3;

        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(15),
            MaxConnectionsPerServer = 200,
            ConnectTimeout = TimeSpan.FromSeconds(timeoutSeconds),
            SslOptions = new SslClientAuthenticationOptions
            {
                // El emulador local de Cosmos DB usa un certificado autofirmado.
                RemoteCertificateValidationCallback = (_, _, _, _) => true
            }
        };

        return new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(timeoutSeconds),
            DefaultRequestVersion = HttpVersion.Version11
        };
    }
}
