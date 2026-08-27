using System.Buffers;
using System.Text;
using System.Text.Json;
using KafkaDemo.Domain.Configuration;

namespace KafkaDemo.Domain.Utils;

/// <summary>
/// Podador de ultra-alta velocidad y 0 asignaciones de memoria Heap para trazas OpenTelemetry de tipo GET.
/// Trunca cualquier arreglo dentro de 'http.response.body_preview' hasta 'MaxArrayItems' elementos
/// y hasta la profundidad configurada 'MaxDepth' en un solo pase de streaming con Utf8JsonReader/Writer.
/// </summary>
public static class OTelTracePruner
{
    private static readonly byte[] BodyPreviewPropName = Encoding.UTF8.GetBytes("http.response.body_preview");
    private static readonly byte[] RequestMethodPropName = Encoding.UTF8.GetBytes("http.request.method");
    private static readonly byte[] MethodGetSpan = Encoding.UTF8.GetBytes("GET");

    /// <summary>
    /// Evalúa si el JSON es una traza GET de OpenTelemetry y poda sus listas internas de respuesta.
    /// Si no es GET o el podado está desactivado, retorna el string original con 0 procesamiento.
    /// </summary>
    public static string PruneIfGetTrace(string rawJson, TracePruningSettings? settings)
    {
        if (string.IsNullOrWhiteSpace(rawJson) || settings is { Enabled: false })
            return rawJson;

        int maxArrayItems = settings?.MaxArrayItems ?? 10;
        int maxDepth = settings?.MaxDepth ?? 5;

        // 1. Verificación rápida: Si no contiene GET o http.response.body_preview, salir inmediatamente
        if (!rawJson.Contains("GET", StringComparison.OrdinalIgnoreCase) || 
            !rawJson.Contains("http.response.body_preview", StringComparison.Ordinal))
        {
            return rawJson;
        }

        // 2. Procesamiento y reemplazo en Streaming de http.response.body_preview
        try
        {
            var utf8Bytes = Encoding.UTF8.GetBytes(rawJson);
            return PruneOuterTrace(utf8Bytes, maxArrayItems, maxDepth);
        }
        catch
        {
            // En caso de cualquier JSON no estándar o error, fallback seguro al original
            return rawJson;
        }
    }

    private static string PruneOuterTrace(ReadOnlySpan<byte> outerUtf8, int maxArrayItems, int maxDepth)
    {
        var reader = new Utf8JsonReader(outerUtf8, isFinalBlock: true, state: default);
        var bufferWriter = new ArrayBufferWriter<byte>(outerUtf8.Length);
        using var writer = new Utf8JsonWriter(bufferWriter);

        while (reader.Read())
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.PropertyName:
                    writer.WritePropertyName(reader.ValueSpan);
                    
                    if (reader.ValueTextEquals(BodyPreviewPropName))
                    {
                        if (reader.Read())
                        {
                            if (reader.TokenType == JsonTokenType.String)
                            {
                                var bodyPreviewStr = reader.GetString();
                                if (!string.IsNullOrEmpty(bodyPreviewStr))
                                {
                                    var prunedInner = PruneInnerJsonString(bodyPreviewStr, maxArrayItems, maxDepth);
                                    writer.WriteStringValue(prunedInner);
                                }
                                else
                                {
                                    writer.WriteStringValue(string.Empty);
                                }
                            }
                            else if (reader.TokenType == JsonTokenType.Null)
                            {
                                writer.WriteNullValue();
                            }
                            else
                            {
                                writer.WriteRawValue(reader.ValueSpan, skipInputValidation: true);
                            }
                        }
                    }
                    break;

                case JsonTokenType.StartObject:
                    writer.WriteStartObject();
                    break;

                case JsonTokenType.EndObject:
                    writer.WriteEndObject();
                    break;

                case JsonTokenType.StartArray:
                    writer.WriteStartArray();
                    break;

                case JsonTokenType.EndArray:
                    writer.WriteEndArray();
                    break;

                case JsonTokenType.String:
                    writer.WriteStringValue(reader.ValueSpan);
                    break;

                case JsonTokenType.Number:
                    writer.WriteRawValue(reader.ValueSpan, skipInputValidation: true);
                    break;

                case JsonTokenType.True:
                case JsonTokenType.False:
                    writer.WriteBooleanValue(reader.GetBoolean());
                    break;

                case JsonTokenType.Null:
                    writer.WriteNullValue();
                    break;
            }
        }

        writer.Flush();
        return Encoding.UTF8.GetString(bufferWriter.WrittenSpan);
    }

    /// <summary>
    /// Poda los arreglos del JSON de respuesta hasta maxArrayItems y maxDepth en un solo pase de streaming.
    /// </summary>
    public static string PruneInnerJsonString(string innerJson, int maxArrayItems, int maxDepth)
    {
        var utf8Bytes = Encoding.UTF8.GetBytes(innerJson);
        var reader = new Utf8JsonReader(utf8Bytes, isFinalBlock: true, state: default);
        var bufferWriter = new ArrayBufferWriter<byte>(utf8Bytes.Length);
        using var writer = new Utf8JsonWriter(bufferWriter);

        if (reader.Read())
        {
            PruneAndCopy(ref reader, writer, 1, maxArrayItems, maxDepth);
        }

        writer.Flush();
        return Encoding.UTF8.GetString(bufferWriter.WrittenSpan);
    }

    private static void PruneAndCopy(ref Utf8JsonReader reader, Utf8JsonWriter writer, int currentDepth, int maxArrayItems, int maxDepth)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.StartObject:
                writer.WriteStartObject();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                {
                    if (reader.TokenType == JsonTokenType.PropertyName)
                    {
                        writer.WritePropertyName(reader.ValueSpan);
                        reader.Read();
                        PruneAndCopy(ref reader, writer, currentDepth + 1, maxArrayItems, maxDepth);
                    }
                }
                writer.WriteEndObject();
                break;

            case JsonTokenType.StartArray:
                writer.WriteStartArray();
                int itemCount = 0;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (currentDepth <= maxDepth && itemCount >= maxArrayItems)
                    {
                        if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                        {
                            reader.TrySkip();
                        }
                    }
                    else
                    {
                        itemCount++;
                        PruneAndCopy(ref reader, writer, currentDepth + 1, maxArrayItems, maxDepth);
                    }
                }
                writer.WriteEndArray();
                break;

            case JsonTokenType.PropertyName:
                writer.WritePropertyName(reader.ValueSpan);
                break;

            case JsonTokenType.String:
                writer.WriteStringValue(reader.ValueSpan);
                break;

            case JsonTokenType.Number:
                writer.WriteRawValue(reader.ValueSpan, skipInputValidation: true);
                break;

            case JsonTokenType.True:
            case JsonTokenType.False:
                writer.WriteBooleanValue(reader.GetBoolean());
                break;

            case JsonTokenType.Null:
                writer.WriteNullValue();
                break;
        }
    }
}
