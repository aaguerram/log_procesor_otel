using ConsumerStreams.Infrastructure;
using ConsumerStreams.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

Console.ForegroundColor = ConsoleColor.Magenta;
Console.WriteLine(@"
╔═══════════════════════════════════════════════════════════════════╗
║     KAFKA STREAMING PROCESSOR - HEXAGONAL NATIVE AOT (.NET 10)    ║
║        Lectura Reactiva, Scoring y Reenvío de Eventos             ║
╚═══════════════════════════════════════════════════════════════════╝
");
Console.ResetColor();

var builder = Host.CreateApplicationBuilder(args);

// Configuración de Logging
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.IncludeScopes = false;
    options.SingleLine = true;
    options.TimestampFormat = "[HH:mm:ss] ";
});

// Inyección Hexagonal de Puertos, Adaptadores y Casos de Uso
builder.Services.AddConsumerStreamsInfrastructure(builder.Configuration);

// Servicio en segundo plano para el ciclo de streaming
builder.Services.AddHostedService<StreamWorkerService>();

var host = builder.Build();
await host.RunAsync();
