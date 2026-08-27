using ConsumerStreams.Application.Services;
using ConsumerStreams.Application.UseCases;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Adapters;
using ConsumerStreams.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ConsumerStreams.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddConsumerStreamsInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        // 1. Mapeo dinámico y estricto desde IConfiguration (appsettings.json / Variables de Entorno)
        var bootstrapServers = configuration["KafkaStream:BootstrapServers"] 
            ?? configuration["TECH-INT-MSG-KAFKA_BROKERS"] 
            ?? configuration["TECH_INT_MSG_KAFKA_BROKERS"];

        var groupId = configuration["KafkaStream:GroupId"] 
            ?? configuration["TECH-INT-MSG-STREAM_GROUP"] 
            ?? configuration["TECH_INT_MSG_STREAM_GROUP"];

        var sourceTopic = configuration["KafkaStream:SourceTopic"] 
            ?? configuration["TECH-INT-MSG-SOURCE_TOPIC"] 
            ?? configuration["TECH_INT_MSG_SOURCE_TOPIC"];

        var targetTopic = configuration["KafkaStream:TargetTopic"] 
            ?? configuration["TECH-INT-MSG-TARGET_TOPIC"] 
            ?? configuration["TECH_INT_MSG_TARGET_TOPIC"];

        if (string.IsNullOrWhiteSpace(bootstrapServers))
            throw new InvalidOperationException("[CONFIG ERROR] 'KafkaStream:BootstrapServers' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(groupId))
            throw new InvalidOperationException("[CONFIG ERROR] 'KafkaStream:GroupId' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(sourceTopic))
            throw new InvalidOperationException("[CONFIG ERROR] 'KafkaStream:SourceTopic' no está configurado en appsettings.json ni en las variables de entorno.");

        if (string.IsNullOrWhiteSpace(targetTopic))
            throw new InvalidOperationException("[CONFIG ERROR] 'KafkaStream:TargetTopic' no está configurado en appsettings.json ni en las variables de entorno.");

        var streamSettings = new KafkaStreamSettings
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            SourceTopic = sourceTopic,
            TargetTopic = targetTopic,
            ErrorTopic = configuration["KafkaStream:ErrorTopic"] ?? "tp.observability.application-log.error.v1",
            AutoOffsetReset = configuration["KafkaStream:AutoOffsetReset"] ?? "Earliest",
            EnableAutoCommit = bool.TryParse(configuration["KafkaStream:EnableAutoCommit"], out var ec) && ec,
            PollTimeoutMs = int.TryParse(configuration["KafkaStream:PollTimeoutMs"], out var pt) ? pt : 1000
        };

        services.AddSingleton(Options.Create(streamSettings));

        // 2. Configuración de Reglas de Protección de Datos (x-log-data-protection)
        var protectionSettings = new ConsumerStreams.Domain.Configuration.DataProtectionRulesSettings
        {
            Enabled = !bool.TryParse(configuration["DataProtectionRules:Enabled"] ?? configuration["DATA_PROTECTION_ENABLED"], out var enabled) || enabled,
            HashSha256 = !bool.TryParse(configuration["DataProtectionRules:HashSha256"] ?? configuration["DATA_PROTECTION_HASH_SHA256"], out var hash) || hash,
            PartialLast4 = !bool.TryParse(configuration["DataProtectionRules:PartialLast4"] ?? configuration["DATA_PROTECTION_PARTIAL_LAST4"], out var part) || part,
            Remove = !bool.TryParse(configuration["DataProtectionRules:Remove"] ?? configuration["DATA_PROTECTION_REMOVE"], out var rem) || rem,
            Full = !bool.TryParse(configuration["DataProtectionRules:Full"] ?? configuration["DATA_PROTECTION_FULL"], out var full) || full,
            MaskUrlPathAndQuery = !bool.TryParse(configuration["DataProtectionRules:MaskUrlPathAndQuery"] ?? configuration["DATA_PROTECTION_MASK_URL"], out var maskUrl) || maskUrl
        };
        services.AddSingleton(protectionSettings);
        services.AddSingleton(Options.Create(protectionSettings));

        // 3. Puertos y Adaptadores (Hexagonal)
        services.AddSingleton<IStreamConsumerPort, KafkaStreamConsumerAdapter>();
        services.AddSingleton<IStreamProducerPort, KafkaStreamProducerAdapter>();
        services.AddSingleton<ITransactionTransformerPort, TransactionEnricher>();
        services.AddSingleton<IVaultTokenProviderPort, AzureKeyVaultTokenAdapter>();
        services.AddSingleton<IPayloadCryptoPort, AesGcmPayloadCryptoAdapter>();
        services.AddSingleton<IContractRulesCachePort, ThreadSafeContractRulesCacheAdapter>();

        // 3. Casos de Uso
        services.AddSingleton<StreamProcessingPipelineUseCase>();

        return services;
    }
}
