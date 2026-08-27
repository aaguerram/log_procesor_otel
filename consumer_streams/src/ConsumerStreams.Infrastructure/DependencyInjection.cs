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
        // 1. Mapeo dinámico y seguro de configuración para Native AOT (.NET 10)
        var streamSettings = new KafkaStreamSettings
        {
            BootstrapServers = configuration["KafkaStream:BootstrapServers"] 
                ?? configuration["TECH-INT-MSG-KAFKA_BROKERS"] 
                ?? configuration["TECH_INT_MSG_KAFKA_BROKERS"] 
                ?? "kafka:29092",

            GroupId = configuration["KafkaStream:GroupId"] 
                ?? configuration["TECH-INT-MSG-STREAM_GROUP"] 
                ?? configuration["TECH_INT_MSG_STREAM_GROUP"] 
                ?? "consumer-streams-produbanco-v1",

            SourceTopic = configuration["KafkaStream:SourceTopic"] 
                ?? configuration["TECH-INT-MSG-SOURCE_TOPIC"] 
                ?? configuration["TECH_INT_MSG_SOURCE_TOPIC"] 
                ?? "tp.observability.application-log.emitted.v1",

            TargetTopic = configuration["KafkaStream:TargetTopic"] 
                ?? configuration["TECH-INT-MSG-TARGET_TOPIC"] 
                ?? configuration["TECH_INT_MSG_TARGET_TOPIC"] 
                ?? "tp.observability.application-log.processed.v1",

            AutoOffsetReset = configuration["KafkaStream:AutoOffsetReset"] ?? "Earliest",
            EnableAutoCommit = bool.TryParse(configuration["KafkaStream:EnableAutoCommit"], out var ec) && ec,
            PollTimeoutMs = int.TryParse(configuration["KafkaStream:PollTimeoutMs"], out var pt) ? pt : 1000
        };

        services.AddSingleton(Options.Create(streamSettings));

        // 2. Puertos y Adaptadores (Hexagonal)
        services.AddSingleton<IStreamConsumerPort, KafkaStreamConsumerAdapter>();
        services.AddSingleton<IStreamProducerPort, KafkaStreamProducerAdapter>();
        services.AddSingleton<ITransactionTransformerPort, TransactionEnricher>();
        services.AddSingleton<IVaultTokenProviderPort, AzureKeyVaultTokenAdapter>();
        services.AddSingleton<IPayloadCryptoPort, AesGcmPayloadCryptoAdapter>();

        // 3. Casos de Uso
        services.AddSingleton<StreamProcessingPipelineUseCase>();

        return services;
    }
}
