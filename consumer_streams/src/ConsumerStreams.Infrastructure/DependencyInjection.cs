using ConsumerStreams.Application.Services;
using ConsumerStreams.Application.UseCases;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Adapters;
using ConsumerStreams.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ConsumerStreams.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddConsumerStreamsInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        // 1. Configuración de Opciones
        services.Configure<KafkaStreamSettings>(configuration.GetSection(KafkaStreamSettings.SectionName));

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
