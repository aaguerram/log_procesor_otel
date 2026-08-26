using System.Text.Json.Serialization;
using ConsumerStreams.Domain.Models;

namespace ConsumerStreams.Application.Serialization;

/// <summary>
/// Contexto de serialización generado en tiempo de compilación (Source Generator) para compatibilidad 100% Native AOT.
/// </summary>
[JsonSourceGenerationOptions(
    WriteIndented = false,
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(RawTransactionEvent))]
[JsonSerializable(typeof(ProcessedTransactionEvent))]
[JsonSerializable(typeof(Dictionary<string, string>))]
public partial class StreamJsonContext : JsonSerializerContext
{
}
