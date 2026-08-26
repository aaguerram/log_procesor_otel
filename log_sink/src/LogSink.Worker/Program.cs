using LogSink.Infrastructure;
using LogSink.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

// Logging de alto rendimiento Native AOT
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.IncludeScopes = false;
    options.SingleLine = true;
    options.TimestampFormat = "[HH:mm:ss] ";
});

// Configuración de Arquitectura Hexagonal
builder.Services.AddLogSinkInfrastructure(builder.Configuration);

// Servicio en segundo plano
builder.Services.AddHostedService<BulkSinkWorkerService>();

var host = builder.Build();

Console.WriteLine("╔═══════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║      COSMOS DB BULK SINK - HEXAGONAL NATIVE AOT (.NET 10)         ║");
Console.WriteLine("║        Micro-Batching de 500 Documentos & Zero Allocation         ║");
Console.WriteLine("╚═══════════════════════════════════════════════════════════════════╝");

await host.RunAsync();
