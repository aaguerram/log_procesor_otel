using KafkaDemo.Application.UseCases;
using KafkaDemo.Domain.Configuration;
using KafkaDemo.Domain.Ports;
using KafkaDemo.Infrastructure.Adapters;
using KafkaDemo.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddKafkaInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        // 1. Configuración de Opciones dinámica y estricta desde IConfiguration y Variables de Entorno
        var bootstrapServers = configuration["Kafka:BootstrapServers"] 
            ?? configuration["TECH-INT-MSG-KAFKA_BROKERS"] 
            ?? configuration["TECH_INT_MSG_KAFKA_BROKERS"];

        var clientId = configuration["Kafka:ClientId"] 
            ?? configuration["TECH-INT-MSG-CLIENT_ID"] 
            ?? configuration["TECH_INT_MSG_CLIENT_ID"];

        if (string.IsNullOrWhiteSpace(bootstrapServers))
            throw new InvalidOperationException("[CONFIG ERROR] 'Kafka:BootstrapServers' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(clientId))
            throw new InvalidOperationException("[CONFIG ERROR] 'Kafka:ClientId' no está configurado en appsettings.json ni en las variables de entorno.");

        var kafkaSettings = new KafkaSettings
        {
            BootstrapServers = bootstrapServers,
            ClientId = clientId,
            Acks = configuration["Kafka:Acks"] ?? "all",
            EnableIdempotence = !bool.TryParse(configuration["Kafka:EnableIdempotence"], out var idemp) || idemp,
            MessageTimeoutMs = int.TryParse(configuration["Kafka:MessageTimeoutMs"], out var mt) ? mt : 10000,
            RequestTimeoutMs = int.TryParse(configuration["Kafka:RequestTimeoutMs"], out var rt) ? rt : 5000,
            SocketTimeoutMs = int.TryParse(configuration["Kafka:SocketTimeoutMs"], out var st) ? st : 10000,
            RetryBackoffMs = int.TryParse(configuration["Kafka:RetryBackoffMs"], out var rb) ? rb : 500,
            MessageSendMaxRetries = int.TryParse(configuration["Kafka:MessageSendMaxRetries"], out var mr) ? mr : 3
        };

        services.AddSingleton(Options.Create(kafkaSettings));

        // Configuración de Podado de Trazas OTel (MaxDepth y MaxArrayItems configurables)
        var pruningSettings = new TracePruningSettings
        {
            Enabled = !bool.TryParse(configuration["TracePruning:Enabled"] ?? configuration["TRACE_PRUNING_ENABLED"], out var enabled) || enabled,
            MaxArrayItems = int.TryParse(configuration["TracePruning:MaxArrayItems"] ?? configuration["TRACE_PRUNING_MAX_ARRAY_ITEMS"], out var maxItems) ? maxItems : 10,
            MaxDepth = int.TryParse(configuration["TracePruning:MaxDepth"] ?? configuration["TRACE_PRUNING_MAX_DEPTH"], out var maxDepth) ? maxDepth : 5
        };
        services.AddSingleton(Options.Create(pruningSettings));
        services.AddSingleton(pruningSettings);

        // 2. Puertos y Adaptadores (Hexagonal)
        services.AddSingleton<IMessageProducerPort, KafkaProducerAdapter>();
        services.AddSingleton<ITopicManagementPort, KafkaAdminAdapter>();
        services.AddSingleton<IVaultTokenProviderPort, AzureKeyVaultTokenAdapter>();
        services.AddSingleton<IPayloadCryptoPort, AesGcmPayloadCryptoAdapter>();

        // 3. Casos de Uso de Aplicación
        services.AddScoped<SendMessagesUseCase>();
        services.AddScoped<ManageTopicsUseCase>();

        return services;
    }
}
