using KafkaDemo.Application.DTOs;
using KafkaDemo.Application.UseCases;
using KafkaDemo.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

// Configuración Hexagonal de Kafka
builder.Services.AddKafkaInfrastructure(builder.Configuration);

// CORS si se requiere acceso distribuido
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
    });
});

var app = builder.Build();

app.UseCors();
app.UseDefaultFiles();
app.UseStaticFiles();

// ===================== REST API ENDPOINTS =====================

// Estado de salud del clúster
app.MapGet("/api/health", async (ManageTopicsUseCase useCase, CancellationToken ct) =>
{
    var health = await useCase.CheckClusterHealthAsync(ct);
    return Results.Ok(health);
});

// Listar todos los tópicos
app.MapGet("/api/topics", async (ManageTopicsUseCase useCase, bool? includeInternal, CancellationToken ct) =>
{
    try
    {
        var topics = await useCase.ListTopicsAsync(includeInternal ?? false, ct);
        return Results.Ok(topics);
    }
    catch (Exception ex)
    {
        return Results.Problem(detail: ex.Message, title: "Error al listar tópicos");
    }
});

// Detalle de un tópico específico
app.MapGet("/api/topics/{topicName}", async (string topicName, ManageTopicsUseCase useCase, CancellationToken ct) =>
{
    var topic = await useCase.GetTopicDetailsAsync(topicName, ct);
    return topic is not null ? Results.Ok(topic) : Results.NotFound(new { message = $"Tópico '{topicName}' no encontrado." });
});

// Crear nuevo tópico
app.MapPost("/api/topics", async (CreateTopicDto dto, ManageTopicsUseCase useCase, CancellationToken ct) =>
{
    try
    {
        var created = await useCase.CreateTopicAsync(dto, ct);
        return Results.Ok(new { success = created, message = $"Tópico '{dto.TopicName}' creado satisfactoriamente." });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, message = ex.Message });
    }
});

// Eliminar un tópico
app.MapDelete("/api/topics/{topicName}", async (string topicName, ManageTopicsUseCase useCase, CancellationToken ct) =>
{
    try
    {
        var deleted = await useCase.DeleteTopicAsync(topicName, ct);
        return Results.Ok(new { success = deleted, message = $"Tópico '{topicName}' eliminado exitosamente." });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, message = ex.Message });
    }
});

// Enviar mensaje individual
app.MapPost("/api/messages/send", async (SendMessageRequestDto request, SendMessagesUseCase useCase, CancellationToken ct) =>
{
    try
    {
        var result = await useCase.SendCustomMessageAsync(request, ct);
        return Results.Ok(new { success = true, result });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, message = ex.Message });
    }
});

// Enviar lote de 20 mensajes (o cantidad personalizada)
app.MapPost("/api/messages/send-batch", async (SendBatchRequest? body, SendMessagesUseCase useCase, CancellationToken ct) =>
{
    try
    {
        var topic = body?.Topic ?? "tp.observability.application-log.emitted.v1";
        var count = body?.Count is > 0 ? body.Count.Value : 20;
        var result = await useCase.GenerateAndSendBatchAsync(topic, count, ct);
        return Results.Ok(new { success = true, result });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, message = ex.Message });
    }
});

// Fallback a index.html
app.MapFallbackToFile("index.html");

app.Run();

public record SendBatchRequest(string? Topic, int? Count);
