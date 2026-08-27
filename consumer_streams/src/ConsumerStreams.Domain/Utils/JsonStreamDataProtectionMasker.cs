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
    private static readonly byte[] NamePropName = Encoding.UTF8.GetBytes("Name");

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
                // 1. Evaluar si es un preview JSON interno embebido como string
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

                // 2. Evaluar si es url.path, http.target o url.full para enmascaramiento de Path Parameters y Query
                if (settings.MaskUrlPathAndQuery && currentProperty is "url.path" or "http.target" or "url.full" or "http.url")
                {
                    var rawUrl = reader.GetString();
                    if (!string.IsNullOrEmpty(rawUrl))
                    {
                        int qIdx = rawUrl.IndexOf('?');
                        if (qIdx >= 0)
                        {
                            string pathPart = rawUrl[..qIdx];
                            string queryPart = rawUrl[(qIdx + 1)..];
                            string maskedPath = MaskUrlPath(pathPart, rules, settings, method, route);
                            string maskedQuery = MaskUrlQuery(queryPart, rules, settings, method, route);
                            writer.WriteStringValue($"{maskedPath}?{maskedQuery}");
                            break;
                        }
                        else
                        {
                            string maskedPath = MaskUrlPath(rawUrl, rules, settings, method, route);
                            writer.WriteStringValue(maskedPath);
                            break;
                        }
                    }
                }

                // 3. Evaluar si es url.query
                if (settings.MaskUrlPathAndQuery && currentProperty is "url.query" or "http.query")
                {
                    var rawQuery = reader.GetString();
                    if (!string.IsNullOrEmpty(rawQuery))
                    {
                        string maskedQuery = MaskUrlQuery(rawQuery, rules, settings, method, route);
                        writer.WriteStringValue(maskedQuery);
                        break;
                    }
                }

                // 4. Evaluar regla estándar para la propiedad string
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

    private static string MaskUrlPath(
        string? rawPath,
        CompiledContractRules rules,
        DataProtectionRulesSettings settings,
        string method,
        string routeTemplate)
    {
        if (string.IsNullOrEmpty(rawPath)) return rawPath ?? string.Empty;

        // 1. Obtener información de parámetros de ruta
        var routeInfo = rules.FindRouteInfo(method, routeTemplate);
        if (routeInfo == null || routeInfo.PathParamRules.Length == 0)
        {
            // Fallback: Buscar template coincidente entre las rutas compiladas
            foreach (var info in rules.RouteParameterRules.Values)
            {
                if (info.PathParamRules.Length > 0 && PathMatchesTemplate(rawPath, info.TemplateSegments))
                {
                    routeInfo = info;
                    break;
                }
            }
        }

        if (routeInfo == null || routeInfo.PathParamRules.Length == 0)
            return rawPath;

        // 2. Segmentar la ruta real
        bool hasLeadingSlash = rawPath.StartsWith('/');
        var rawSegments = rawPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var tplSegments = routeInfo.TemplateSegments;

        // 3. Alinear offset de sufijo
        int offset = rawSegments.Length - tplSegments.Length;
        if (offset < 0) return rawPath;

        // Validar coincidencia en segmentos literales
        for (int t = 0; t < tplSegments.Length; t++)
        {
            var tSeg = tplSegments[t];
            if (!tSeg.StartsWith('{') && !tSeg.EndsWith('}'))
            {
                if (!tSeg.Equals(rawSegments[offset + t], StringComparison.OrdinalIgnoreCase))
                    return rawPath;
            }
        }

        // 4. Enmascarar segmentos correspondientes a Path Parameters
        foreach (var (tplIndex, _, rule) in routeInfo.PathParamRules)
        {
            if (tplIndex >= 0 && tplIndex < tplSegments.Length)
            {
                int rawIdx = offset + tplIndex;
                if (rawIdx >= 0 && rawIdx < rawSegments.Length)
                {
                    rawSegments[rawIdx] = MaskSingleValue(rawSegments[rawIdx], rule, settings);
                }
            }
        }

        return (hasLeadingSlash ? "/" : "") + string.Join('/', rawSegments);
    }

    private static bool PathMatchesTemplate(string rawPath, string[] tplSegments)
    {
        var rawSegments = rawPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        int offset = rawSegments.Length - tplSegments.Length;
        if (offset < 0) return false;

        for (int t = 0; t < tplSegments.Length; t++)
        {
            var tSeg = tplSegments[t];
            if (!tSeg.StartsWith('{') && !tSeg.EndsWith('}'))
            {
                if (!tSeg.Equals(rawSegments[offset + t], StringComparison.OrdinalIgnoreCase))
                    return false;
            }
        }
        return true;
    }

    private static string MaskUrlQuery(
        string? rawQuery,
        CompiledContractRules rules,
        DataProtectionRulesSettings settings,
        string method,
        string routeTemplate)
    {
        if (string.IsNullOrEmpty(rawQuery)) return rawQuery ?? string.Empty;

        bool hasLeadingQuestion = rawQuery.StartsWith('?');
        string queryBody = hasLeadingQuestion ? rawQuery[1..] : rawQuery;

        var pairs = queryBody.Split('&', StringSplitOptions.RemoveEmptyEntries);
        var routeInfo = rules.FindRouteInfo(method, routeTemplate);

        var maskedPairs = new List<string>(pairs.Length);
        foreach (var pair in pairs)
        {
            var eqIdx = pair.IndexOf('=');
            if (eqIdx <= 0)
            {
                maskedPairs.Add(pair);
                continue;
            }

            string key = pair[..eqIdx];
            string val = pair[(eqIdx + 1)..];

            var rule = DataProtectionRuleType.Full;
            if (routeInfo != null && routeInfo.QueryParamRules.TryGetValue(key, out var rRule))
            {
                rule = rRule;
            }
            else
            {
                rule = rules.GetRule(method, routeTemplate, key);
            }

            if (rule == DataProtectionRuleType.Remove && settings.Remove)
            {
                continue;
            }

            string maskedVal = MaskSingleValue(val, rule, settings);
            maskedPairs.Add($"{key}={maskedVal}");
        }

        string res = string.Join('&', maskedPairs);
        return hasLeadingQuestion ? $"?{res}" : res;
    }

    private static string MaskSingleValue(string val, DataProtectionRuleType rule, DataProtectionRulesSettings settings)
    {
        switch (rule)
        {
            case DataProtectionRuleType.HashSha256 when settings.HashSha256:
                var bytes = Encoding.UTF8.GetBytes(val);
                Span<byte> hashBytes = stackalloc byte[32];
                SHA256.HashData(bytes, hashBytes);
                Span<char> hexChars = stackalloc char[64];
                Convert.TryToHexStringLower(hashBytes, hexChars, out _);
                return new string(hexChars);

            case DataProtectionRuleType.PartialLast4 when settings.PartialLast4:
                if (val.Length <= 4) return val;
                return new string('*', val.Length - 4) + val[^4..];

            case DataProtectionRuleType.Remove when settings.Remove:
                return "***REDACTED***";

            default:
                return val;
        }
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
                else if (reader.ValueTextEquals(NamePropName))
                {
                    if (reader.Read() && reader.TokenType == JsonTokenType.String)
                    {
                        var nameVal = reader.GetString();
                        if (!string.IsNullOrEmpty(nameVal) && string.IsNullOrEmpty(route))
                        {
                            var parts = nameVal.Split(' ', 2);
                            if (parts.Length == 2 && (parts[0] is "GET" or "POST" or "PUT" or "DELETE" or "PATCH"))
                            {
                                method = parts[0];
                                route = parts[1];
                            }
                            else if (nameVal.StartsWith('/'))
                            {
                                route = nameVal;
                            }
                        }
                    }
                }
            }
        }
    }
}
