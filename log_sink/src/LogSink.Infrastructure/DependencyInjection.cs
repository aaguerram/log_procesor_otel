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
        // Mapeo seguro y libre de reflexión para Native AOT (.NET 10)
        var sinkSettings = new SinkSettings();
        var section = configuration.GetSection(SinkSettings.SectionName);
        if (section.Exists())
        {
            sinkSettings.BootstrapServers = section[nameof(SinkSettings.BootstrapServers)] ?? sinkSettings.BootstrapServers;
            sinkSettings.SourceTopic = section[nameof(SinkSettings.SourceTopic)] ?? sinkSettings.SourceTopic;
            sinkSettings.GroupId = section[nameof(SinkSettings.GroupId)] ?? sinkSettings.GroupId;
            if (int.TryParse(section[nameof(SinkSettings.BatchSize)], out var bs)) sinkSettings.BatchSize = bs;
            if (int.TryParse(section[nameof(SinkSettings.BatchTimeoutMs)], out var bt)) sinkSettings.BatchTimeoutMs = bt;
            sinkSettings.CosmosEndpoint = section[nameof(SinkSettings.CosmosEndpoint)] ?? sinkSettings.CosmosEndpoint;
            sinkSettings.CosmosPrimaryKey = section[nameof(SinkSettings.CosmosPrimaryKey)] ?? sinkSettings.CosmosPrimaryKey;
            sinkSettings.DatabaseName = section[nameof(SinkSettings.DatabaseName)] ?? sinkSettings.DatabaseName;
            sinkSettings.ContainerName = section[nameof(SinkSettings.ContainerName)] ?? sinkSettings.ContainerName;
            sinkSettings.PartitionKeyPath = section[nameof(SinkSettings.PartitionKeyPath)] ?? sinkSettings.PartitionKeyPath;
            sinkSettings.KeyVaultEndpoint = section[nameof(SinkSettings.KeyVaultEndpoint)] ?? sinkSettings.KeyVaultEndpoint;
            sinkSettings.VaultTokenId = section[nameof(SinkSettings.VaultTokenId)] ?? sinkSettings.VaultTokenId;
        }

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
