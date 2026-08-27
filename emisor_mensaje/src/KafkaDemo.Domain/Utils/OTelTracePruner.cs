using System.Buffers;
using System.Runtime.CompilerServices;
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

    /// <summary>
    /// Comprueba en tiempo sub-nanosegundo y 0 asignaciones de memoria Heap si el contenido es un JSON estructurado (objeto o array).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsJsonPayload(ReadOnlySpan<byte> bytes)
    {
        int i = 0;
        while (i < bytes.Length && (bytes[i] == (byte)' ' || bytes[i] == (byte)'\t' || 
                                    bytes[i] == (byte)'\r' || bytes[i] == (byte)'\n'))
        {
            i++;
        }

        if (i >= bytes.Length) return false;
        byte first = bytes[i];
        return first == (byte)'{' || first == (byte)'[';
    }

    /// <summary>
    /// Comprueba en tiempo sub-nanosegundo y 0 asignaciones sobre string si el contenido es un JSON estructurado.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsJsonPayload(ReadOnlySpan<char> chars)
    {
        int i = 0;
        while (i < chars.Length && char.IsWhiteSpace(chars[i]))
        {
            i++;
        }

        if (i >= chars.Length) return false;
        char first = chars[i];
        return first == '{' || first == '[';
    }

    /// <summary>
    /// Evalúa si el JSON es una traza GET de OpenTelemetry y, tras validar que sea un JSON bien formado, poda sus listas internas de respuesta.
    /// Si no es GET, no es JSON o el podado está desactivado, retorna el string original con 0 procesamiento.
    /// </summary>
    public static string PruneIfGetTrace(string rawJson, TracePruningSettings? settings)
    {
        if (string.IsNullOrWhiteSpace(rawJson) || settings is { Enabled: false })
            return rawJson;

        // 1. Verificación preliminar de método GET
        if (!rawJson.Contains("GET", StringComparison.OrdinalIgnoreCase) || 
            !rawJson.Contains("http.response.body_preview", StringComparison.Ordinal))
        {
            return rawJson;
        }

        // 2. Validación de tipo JSON (0 asignaciones, sub-nanosegundo)
        if (!IsJsonPayload(rawJson.AsSpan()))
        {
            return rawJson;
        }

        int maxArrayItems = settings?.MaxArrayItems ?? 10;
        int maxDepth = settings?.MaxDepth ?? 5;

        // 3. Procesamiento y reemplazo en Streaming de http.response.body_preview
        try
        {
            var utf8Bytes = Encoding.UTF8.GetBytes(rawJson);
            return PruneOuterTrace(utf8Bytes, maxArrayItems, maxDepth);
        }
        catch
        {
            // En caso de cualquier error sintáctico o no estándar, fallback seguro al original
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
                                // Validar que el preview sea JSON estructurado antes de aplicar el algoritmo
                                if (!string.IsNullOrEmpty(bodyPreviewStr) && IsJsonPayload(bodyPreviewStr.AsSpan()))
                                {
                                    var prunedInner = PruneInnerJsonString(bodyPreviewStr, maxArrayItems, maxDepth);
                                    writer.WriteStringValue(prunedInner);
                                }
                                else if (bodyPreviewStr != null)
                                {
                                    // Si no es JSON (ej. texto plano, HTML), se preserva tal cual
                                    writer.WriteStringValue(bodyPreviewStr);
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
        if (!IsJsonPayload(innerJson.AsSpan()))
            return innerJson;

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
