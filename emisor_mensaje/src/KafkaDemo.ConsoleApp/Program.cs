using KafkaDemo.Application.UseCases;
using KafkaDemo.Domain.Ports;
using KafkaDemo.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine(@"
╔═══════════════════════════════════════════════════════════════════╗
║       KAFKA DATA PRODUCER - HEXAGONAL ARCHITECTURE (.NET 10)     ║
║               Envío Automatizado de 20 Mensajes                   ║
╚═══════════════════════════════════════════════════════════════════╝
");
Console.ResetColor();

var builder = Host.CreateApplicationBuilder(args);

// Configuración y variables de entorno
builder.Configuration
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
    .AddEnvironmentVariables();

// Inyección de dependencias Hexagonal
builder.Services.AddKafkaInfrastructure(builder.Configuration);

var host = builder.Build();

using var scope = host.Services.CreateScope();
var sendMessagesUseCase = scope.ServiceProvider.GetRequiredService<SendMessagesUseCase>();
var topicManagementPort = scope.ServiceProvider.GetRequiredService<ITopicManagementPort>();
var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();

const string targetTopic = "tp.observability.application-log.emitted.v1";
const int messageCount = 20;

Console.WriteLine($"[1/3] Verificando conectividad con el clúster de Kafka...");

// Espera activa a que Kafka esté listo (útil para Docker Compose)
var isConnected = false;
var maxRetries = 15;
var retryCount = 0;

while (!isConnected && retryCount < maxRetries)
{
    retryCount++;
    isConnected = await topicManagementPort.PingAsync();
    if (!isConnected)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"      Intento {retryCount}/{maxRetries}: Esperando a que el broker de Kafka esté disponible (3s)...");
        Console.ResetColor();
        await Task.Delay(3000);
    }
}

if (!isConnected)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine("❌ Error crítico: No se pudo establecer conexión con Kafka después de múltiples reintentos.");
    Console.ResetColor();
    return 1;
}

Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine("✔ Conectado exitosamente con Kafka!");
Console.ResetColor();

Console.WriteLine($"\n[2/3] Generando y enviando {messageCount} transacciones bancarias al tópico '{targetTopic}'...");

try
{
    var batchResult = await sendMessagesUseCase.GenerateAndSendBatchAsync(targetTopic, messageCount);

    Console.WriteLine("\n[3/3] Detalle de mensajes persistidos:");
    Console.WriteLine(new string('─', 80));
    Console.WriteLine($"{"#",-4} | {"Partición",-10} | {"Offset",-10} | {"Estado",-12} | {"Clave (Cuenta)",-20} | {"Timestamp UTC",-15}");
    Console.WriteLine(new string('─', 80));

    int idx = 1;
    foreach (var item in batchResult.Results)
    {
        Console.WriteLine($"{idx++,-4} | P-{item.Partition,-8} | #{item.Offset,-9} | {item.Status,-12} | {item.Key,-20} | {item.Timestamp:HH:mm:ss.fff}");
    }

    Console.WriteLine(new string('─', 80));
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"\n🎉 Éxito: Se enviaron {batchResult.TotalSent}/{batchResult.TotalRequested} mensajes en {batchResult.ElapsedMilliseconds:F2} ms.");
    Console.WriteLine($"   Tópico de destino: {batchResult.TargetTopic}");
    Console.ResetColor();
    return 0;
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"\n❌ Fallo durante la publicación de mensajes: {ex.Message}");
    Console.ResetColor();
    logger.LogError(ex, "Excepción durante la ejecución del envío por lotes.");
    return 1;
}
