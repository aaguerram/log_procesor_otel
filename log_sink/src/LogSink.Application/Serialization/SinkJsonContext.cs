using System.Text.Json.Serialization;
using LogSink.Domain.Models;

namespace LogSink.Application.Serialization;

/// <summary>
/// Contexto de serialización/deserialización JSON por generación de código en tiempo de compilación (Source Generator).
/// Imprescindible para Native AOT en .NET 10 (Zero-Reflection).
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    WriteIndented = false,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(LogDocument))]
[JsonSerializable(typeof(List<LogDocument>))]
[JsonSerializable(typeof(BulkSinkResult))]
[JsonSerializable(typeof(Dictionary<string, string>))]
public partial class SinkJsonContext : JsonSerializerContext
{
}
