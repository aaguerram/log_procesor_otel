using ConsumerStreams.Application.Services;
using ConsumerStreams.Application.UseCases;
using ConsumerStreams.Domain.Configuration;
using ConsumerStreams.Domain.Contracts;
using ConsumerStreams.Domain.DataProtection;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Adapters;
using ConsumerStreams.Infrastructure.Configuration;
using ConsumerStreams.Infrastructure.Security;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ConsumerStreams.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddConsumerStreamsInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton(Options.Create(BuildStreamSettings(configuration)));

        var protectionSettings = BuildDataProtectionSettings(configuration);
        services.AddSingleton(protectionSettings);
        services.AddSingleton(Options.Create(protectionSettings));

        services.AddSingleton(TimeProvider.System);

        // Servicios de dominio (lógica pura / política)
        services.AddSingleton<IContractCompiler, OpenApiContractCompilerAdapter>();
        services.AddSingleton<PayloadMaskingService>();
        services.AddSingleton<EnvelopeDecryptionService>();

        // Puertos y adaptadores (Hexagonal)
        services.AddSingleton<IStreamConsumerPort, KafkaStreamConsumerAdapter>();
        services.AddSingleton<IStreamProducerPort, KafkaStreamProducerAdapter>();
        services.AddSingleton<IDlqProducerPort, KafkaDlqProducerAdapter>();
        services.AddSingleton<ITransactionTransformerPort, TransactionEnricher>();
        services.AddSingleton<IAesKeyMaterialFactory, DeterministicSeedAesKeyMaterialFactory>();
        services.AddSingleton<IVaultTokenProviderPort, AzureKeyVaultTokenAdapter>();
        services.AddSingleton<IPayloadCryptoPort, AesGcmPayloadCryptoAdapter>();
        services.AddSingleton<IContractRulesCachePort, ThreadSafeContractRulesCacheAdapter>();

        // Casos de uso
        services.AddSingleton<StreamProcessingPipelineUseCase>();

        return services;
    }

    private static KafkaStreamSettings BuildStreamSettings(IConfiguration configuration) => new()
    {
        BootstrapServers = configuration.Required("KafkaStream:BootstrapServers",
            "KafkaStream:BootstrapServers", "TECH-INT-MSG-KAFKA_BROKERS", "TECH_INT_MSG_KAFKA_BROKERS"),
        GroupId = configuration.Required("KafkaStream:GroupId",
            "KafkaStream:GroupId", "TECH-INT-MSG-STREAM_GROUP", "TECH_INT_MSG_STREAM_GROUP"),
        SourceTopic = configuration.Required("KafkaStream:SourceTopic",
            "KafkaStream:SourceTopic", "TECH-INT-MSG-SOURCE_TOPIC", "TECH_INT_MSG_SOURCE_TOPIC"),
        TargetTopic = configuration.Required("KafkaStream:TargetTopic",
            "KafkaStream:TargetTopic", "TECH-INT-MSG-TARGET_TOPIC", "TECH_INT_MSG_TARGET_TOPIC"),
        ErrorTopic = configuration.ValueOrDefault("tp.observability.application-log.error.v1", "KafkaStream:ErrorTopic"),
        AutoOffsetReset = configuration.ValueOrDefault("Earliest", "KafkaStream:AutoOffsetReset"),
        EnableAutoCommit = configuration.BoolOrDefault(false, "KafkaStream:EnableAutoCommit"),
        PollTimeoutMs = configuration.IntOrDefault(1000, "KafkaStream:PollTimeoutMs")
    };

    private static DataProtectionRulesSettings BuildDataProtectionSettings(IConfiguration configuration) => new()
    {
        Enabled = configuration.FlagEnabledByDefault("DataProtectionRules:Enabled", "DATA_PROTECTION_ENABLED"),
        HashSha256 = configuration.FlagEnabledByDefault("DataProtectionRules:HashSha256", "DATA_PROTECTION_HASH_SHA256"),
        PartialLast4 = configuration.FlagEnabledByDefault("DataProtectionRules:PartialLast4", "DATA_PROTECTION_PARTIAL_LAST4"),
        Remove = configuration.FlagEnabledByDefault("DataProtectionRules:Remove", "DATA_PROTECTION_REMOVE"),
        Full = configuration.FlagEnabledByDefault("DataProtectionRules:Full", "DATA_PROTECTION_FULL"),
        MaskUrlPathAndQuery = configuration.FlagEnabledByDefault("DataProtectionRules:MaskUrlPathAndQuery", "DATA_PROTECTION_MASK_URL")
    };
}
