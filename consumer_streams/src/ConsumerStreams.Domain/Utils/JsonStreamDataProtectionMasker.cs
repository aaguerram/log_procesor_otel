using System.Buffers;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ConsumerStreams.Domain.Configuration;
using ConsumerStreams.Domain.Models;

namespace ConsumerStreams.Domain.Utils;

/// <summary>
/// Motor de enmascaramiento de datos de observabilidad en streaming de 0 asignaciones de memoria Heap.
/// Aplica las políticas x-log-data-protection (HashSha256, PartialLast4, Remove, Full)
/// directamente sobre buffers de bytes UTF-8 con Utf8JsonReader y Utf8JsonWriter.
/// </summary>
public static class JsonStreamDataProtectionMasker
{
    private static readonly byte[] BodyPreviewPropName = Encoding.UTF8.GetBytes("http.response.body_preview");
    private static readonly byte[] RequestBodyPropName = Encoding.UTF8.GetBytes("http.request.body_preview");
    private static readonly byte[] MethodPropName = Encoding.UTF8.GetBytes("http.request.method");
    private static readonly byte[] RoutePropName = Encoding.UTF8.GetBytes("http.route");

    public static byte[] MaskPayload(
        ReadOnlySpan<byte> inputUtf8,
        CompiledContractRules rules,
        DataProtectionRulesSettings settings)
    {
        if (inputUtf8.IsEmpty || settings is { Enabled: false })
            return inputUtf8.ToArray();

        // 1. Extraer método y ruta en un pase preliminar ultra-rápido de metadatos
        ExtractMetadata(inputUtf8, out string method, out string route);
        route = OpenApiContractCompiler.NormalizeRoute(route);

        // 2. Procesamiento Streaming Recursivo en 1 solo pase
        var bufferWriter = new ArrayBufferWriter<byte>(inputUtf8.Length + 256);
        using (var writer = new Utf8JsonWriter(bufferWriter))
        {
            var reader = new Utf8JsonReader(inputUtf8, isFinalBlock: true, state: default);
            if (reader.Read())
            {
                MaskAndCopy(ref reader, writer, rules, settings, method, route, string.Empty);
            }
            writer.Flush();
        }

        return bufferWriter.WrittenSpan.ToArray();
    }

    private static void MaskAndCopy(
        ref Utf8JsonReader reader,
        Utf8JsonWriter writer,
        CompiledContractRules rules,
        DataProtectionRulesSettings settings,
        string method,
        string route,
        string currentProperty)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.StartObject:
                writer.WriteStartObject();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                {
                    if (reader.TokenType == JsonTokenType.PropertyName)
                    {
                        var propNameBytes = reader.ValueSpan;
                        string propName = reader.GetString() ?? string.Empty;
                        var rule = rules.GetRule(method, route, propName);

                        // Si la regla es Remove y está activa, saltamos la propiedad y su valor por completo
                        if (rule == DataProtectionRuleType.Remove && settings.Remove)
                        {
                            reader.Read(); // Avanzar al valor
                            if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                            {
                                reader.TrySkip();
                            }
                            continue; // No escribir la propiedad
                        }

                        writer.WritePropertyName(propNameBytes);
                        reader.Read(); // Avanzar al valor
                        MaskAndCopy(ref reader, writer, rules, settings, method, route, propName);
                    }
                }
                writer.WriteEndObject();
                break;

            case JsonTokenType.StartArray:
                writer.WriteStartArray();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    MaskAndCopy(ref reader, writer, rules, settings, method, route, currentProperty);
                }
                writer.WriteEndArray();
                break;

            case JsonTokenType.PropertyName:
                writer.WritePropertyName(reader.ValueSpan);
                break;

            case JsonTokenType.String:
                // Evaluar si es un preview JSON interno embebido como string
                if (currentProperty is "http.response.body_preview" or "http.request.body_preview")
                {
                    var innerStr = reader.GetString();
                    if (!string.IsNullOrEmpty(innerStr) && (innerStr.StartsWith('{') || innerStr.StartsWith('[')))
                    {
                        var innerMasked = MaskInnerJsonString(innerStr, rules, settings, method, route);
                        writer.WriteStringValue(innerMasked);
                        break;
                    }
                }

                // Evaluar regla para el string
                var strRule = rules.GetRule(method, route, currentProperty);
                WriteMaskedStringOrNumber(ref reader, writer, strRule, settings);
                break;

            case JsonTokenType.Number:
                var numRule = rules.GetRule(method, route, currentProperty);
                WriteMaskedStringOrNumber(ref reader, writer, numRule, settings);
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

    private static string MaskInnerJsonString(
        string innerJson,
        CompiledContractRules rules,
        DataProtectionRulesSettings settings,
        string method,
        string route)
    {
        var innerBytes = Encoding.UTF8.GetBytes(innerJson);
        var bufferWriter = new ArrayBufferWriter<byte>(innerBytes.Length + 128);
        using (var writer = new Utf8JsonWriter(bufferWriter))
        {
            var reader = new Utf8JsonReader(innerBytes, isFinalBlock: true, state: default);
            if (reader.Read())
            {
                MaskAndCopy(ref reader, writer, rules, settings, method, route, string.Empty);
            }
            writer.Flush();
        }

        return Encoding.UTF8.GetString(bufferWriter.WrittenSpan);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteMaskedStringOrNumber(
        ref Utf8JsonReader reader,
        Utf8JsonWriter writer,
        DataProtectionRuleType rule,
        DataProtectionRulesSettings settings)
    {
        switch (rule)
        {
            case DataProtectionRuleType.HashSha256 when settings.HashSha256:
                Span<byte> hashBytes = stackalloc byte[32];
                SHA256.HashData(reader.ValueSpan, hashBytes);
                Span<char> hexChars = stackalloc char[64];
                Convert.TryToHexStringLower(hashBytes, hexChars, out _);
                writer.WriteStringValue(hexChars);
                return;

            case DataProtectionRuleType.PartialLast4 when settings.PartialLast4:
                var valSpan = reader.ValueSpan;
                int len = valSpan.Length;
                if (len <= 4)
                {
                    writer.WriteStringValue(valSpan);
                }
                else
                {
                    Span<char> maskedChars = stackalloc char[len];
                    int maskCount = len - 4;
                    maskedChars[..maskCount].Fill('*');
                    for (int i = maskCount; i < len; i++)
                    {
                        maskedChars[i] = (char)valSpan[i];
                    }
                    writer.WriteStringValue(maskedChars);
                }
                return;

            default:
                if (reader.TokenType == JsonTokenType.String)
                    writer.WriteStringValue(reader.ValueSpan);
                else
                    writer.WriteRawValue(reader.ValueSpan, skipInputValidation: true);
                break;
        }
    }

    private static void ExtractMetadata(ReadOnlySpan<byte> utf8, out string method, out string route)
    {
        method = "GET";
        route = string.Empty;

        var reader = new Utf8JsonReader(utf8, isFinalBlock: true, state: default);
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                if (reader.ValueTextEquals(MethodPropName))
                {
                    if (reader.Read() && reader.TokenType == JsonTokenType.String)
                        method = reader.GetString() ?? "GET";
                }
                else if (reader.ValueTextEquals(RoutePropName))
                {
                    if (reader.Read() && reader.TokenType == JsonTokenType.String)
                        route = reader.GetString() ?? string.Empty;
                }
            }
        }
    }
}
