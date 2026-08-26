using KafkaDemo.Application.UseCases;
using KafkaDemo.Domain.Ports;
using KafkaDemo.Infrastructure.Adapters;
using KafkaDemo.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaDemo.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddKafkaInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        // 1. Configuración de Opciones
        services.Configure<KafkaSettings>(configuration.GetSection(KafkaSettings.SectionName));

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
