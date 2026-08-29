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
    private static readonly byte[] MethodPropName = Encoding.UTF8.GetBytes("http.request.method");
    private static readonly byte[] RoutePropName = Encoding.UTF8.GetBytes("http.route");
    private static readonly byte[] NamePropName = Encoding.UTF8.GetBytes("Name");

    /// <summary>
    /// Contexto inmutable de una operación de enmascaramiento: árbol de reglas, interruptores de
    /// configuración y metadatos de la ruta OpenAPI resuelta. Se propaga por valor durante el
    /// recorrido recursivo para no multiplicar la lista de parámetros de cada método auxiliar.
    /// </summary>
    private readonly record struct MaskingContext(
        CompiledContractRules Rules,
        DataProtectionRulesSettings Settings,
        string Method,
        string Route);

    public static byte[] MaskPayload(
        ReadOnlySpan<byte> inputUtf8,
        CompiledContractRules rules,
        DataProtectionRulesSettings settings)
    {
        if (inputUtf8.IsEmpty || settings is { Enabled: false })
            return inputUtf8.ToArray();

        // 1. Extraer método y ruta en un pase preliminar ultra-rápido de metadatos
        ExtractMetadata(inputUtf8, out string method, out string route);
        var context = new MaskingContext(rules, settings, method, OpenApiContractCompiler.NormalizeRoute(route));

        // 2. Procesamiento Streaming Recursivo en 1 solo pase
        return MaskDocument(inputUtf8, context);
    }

    private static byte[] MaskDocument(ReadOnlySpan<byte> inputUtf8, MaskingContext context)
    {
        var bufferWriter = new ArrayBufferWriter<byte>(inputUtf8.Length + 256);
        using (var writer = new Utf8JsonWriter(bufferWriter))
        {
            var reader = new Utf8JsonReader(inputUtf8, isFinalBlock: true, state: default);
            if (reader.Read())
            {
                MaskAndCopy(ref reader, writer, context, string.Empty, string.Empty);
            }
            writer.Flush();
        }

        return bufferWriter.WrittenSpan.ToArray();
    }

    private static void MaskAndCopy(
        ref Utf8JsonReader reader,
        Utf8JsonWriter writer,
        MaskingContext context,
        string parentPath,
        string simplePropName)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.StartObject:
                CopyObject(ref reader, writer, context, parentPath);
                break;

            case JsonTokenType.StartArray:
                CopyArray(ref reader, writer, context, parentPath, simplePropName);
                break;

            case JsonTokenType.PropertyName:
                writer.WritePropertyName(reader.ValueSpan);
                break;

            case JsonTokenType.String:
                CopyStringValue(ref reader, writer, context, parentPath, simplePropName);
                break;

            case JsonTokenType.Number:
                WriteMaskedStringOrNumber(ref reader, writer,
                    context.Rules.GetRule(context.Method, context.Route, parentPath, simplePropName), context.Settings);
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

    private static void CopyObject(ref Utf8JsonReader reader, Utf8JsonWriter writer, MaskingContext context, string parentPath)
    {
        writer.WriteStartObject();
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            var propNameBytes = reader.ValueSpan;
            string propName = reader.GetString() ?? string.Empty;
            string fullChildPath = string.IsNullOrEmpty(parentPath) ? propName : $"{parentPath}.{propName}";
            var rule = context.Rules.GetRule(context.Method, context.Route, fullChildPath, propName);

            // Si la regla es Remove y está activa, saltamos la propiedad y su valor por completo
            if (rule == DataProtectionRuleType.Remove && context.Settings.Remove)
            {
                SkipPropertyValue(ref reader);
                continue;
            }

            writer.WritePropertyName(propNameBytes);
            reader.Read(); // Avanzar al valor
            MaskAndCopy(ref reader, writer, context, fullChildPath, propName);
        }
        writer.WriteEndObject();
    }

    private static void SkipPropertyValue(ref Utf8JsonReader reader)
    {
        reader.Read(); // Avanzar al valor
        if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
            reader.TrySkip();
    }

    private static void CopyArray(
        ref Utf8JsonReader reader, Utf8JsonWriter writer, MaskingContext context, string parentPath, string simplePropName)
    {
        writer.WriteStartArray();
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            MaskAndCopy(ref reader, writer, context, parentPath, simplePropName);
        }
        writer.WriteEndArray();
    }

    private static void CopyStringValue(
        ref Utf8JsonReader reader,
        Utf8JsonWriter writer,
        MaskingContext context,
        string parentPath,
        string simplePropName)
    {
        // 1. Preview JSON interno embebido como string
        if (TryCopyEmbeddedJsonPreview(ref reader, writer, context, simplePropName))
            return;

        // 2. url.path / http.target / url.full / http.url: enmascarar Path Parameters y Query
        if (context.Settings.MaskUrlPathAndQuery && TryCopyMaskedUrl(ref reader, writer, context, simplePropName))
            return;

        // 3. url.query / http.query
        if (context.Settings.MaskUrlPathAndQuery && TryCopyMaskedQuery(ref reader, writer, context, simplePropName))
            return;

        // 4. Regla estándar jerárquica para la propiedad string
        var strRule = context.Rules.GetRule(context.Method, context.Route, parentPath, simplePropName);
        WriteMaskedStringOrNumber(ref reader, writer, strRule, context.Settings);
    }

    private static bool TryCopyEmbeddedJsonPreview(
        ref Utf8JsonReader reader, Utf8JsonWriter writer, MaskingContext context, string simplePropName)
    {
        if (simplePropName is not ("http.response.body_preview" or "http.request.body_preview" or "body_preview"))
            return false;

        var innerStr = reader.GetString();
        if (string.IsNullOrEmpty(innerStr) || (!innerStr.StartsWith('{') && !innerStr.StartsWith('[')))
            return false;

        writer.WriteStringValue(MaskInnerJsonString(innerStr, context));
        return true;
    }

    private static bool TryCopyMaskedUrl(
        ref Utf8JsonReader reader, Utf8JsonWriter writer, MaskingContext context, string simplePropName)
    {
        if (simplePropName is not ("url.path" or "http.target" or "url.full" or "http.url"))
            return false;

        var rawUrl = reader.GetString();
        if (string.IsNullOrEmpty(rawUrl))
            return false;

        int qIdx = rawUrl.IndexOf('?');
        if (qIdx >= 0)
        {
            string maskedPath = MaskUrlPath(rawUrl[..qIdx], context);
            string maskedQuery = MaskUrlQuery(rawUrl[(qIdx + 1)..], context);
            writer.WriteStringValue($"{maskedPath}?{maskedQuery}");
        }
        else
        {
            writer.WriteStringValue(MaskUrlPath(rawUrl, context));
        }

        return true;
    }

    private static bool TryCopyMaskedQuery(
        ref Utf8JsonReader reader, Utf8JsonWriter writer, MaskingContext context, string simplePropName)
    {
        if (simplePropName is not ("url.query" or "http.query"))
            return false;

        var rawQuery = reader.GetString();
        if (string.IsNullOrEmpty(rawQuery))
            return false;

        writer.WriteStringValue(MaskUrlQuery(rawQuery, context));
        return true;
    }

    private static string MaskInnerJsonString(string innerJson, MaskingContext context)
    {
        var innerBytes = Encoding.UTF8.GetBytes(innerJson);
        var bufferWriter = new ArrayBufferWriter<byte>(innerBytes.Length + 128);
        using (var writer = new Utf8JsonWriter(bufferWriter))
        {
            var reader = new Utf8JsonReader(innerBytes, isFinalBlock: true, state: default);
            if (reader.Read())
            {
                MaskAndCopy(ref reader, writer, context, string.Empty, string.Empty);
            }
            writer.Flush();
        }

        return Encoding.UTF8.GetString(bufferWriter.WrittenSpan);
    }

    private static string MaskUrlPath(string? rawPath, MaskingContext context)
    {
        if (string.IsNullOrEmpty(rawPath)) return rawPath ?? string.Empty;

        var routeInfo = ResolvePathRouteInfo(context.Rules, context.Method, context.Route, rawPath);
        if (routeInfo == null || routeInfo.PathParamRules.Length == 0)
            return rawPath;

        bool hasLeadingSlash = rawPath.StartsWith('/');
        var rawSegments = rawPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var tplSegments = routeInfo.TemplateSegments;

        // Alinear el offset de sufijo entre la ruta real y la plantilla
        int offset = rawSegments.Length - tplSegments.Length;
        if (offset < 0) return rawPath;

        if (!LiteralSegmentsMatch(tplSegments, rawSegments, offset))
            return rawPath;

        ApplyPathParamMasks(routeInfo, rawSegments, tplSegments.Length, offset, context.Settings);

        return (hasLeadingSlash ? "/" : string.Empty) + string.Join('/', rawSegments);
    }

    private static CompiledRouteParameterInfo? ResolvePathRouteInfo(
        CompiledContractRules rules, string method, string routeTemplate, string rawPath)
    {
        var routeInfo = rules.FindRouteInfo(method, routeTemplate);
        if (routeInfo is { PathParamRules.Length: > 0 })
            return routeInfo;

        // Fallback: buscar una plantilla coincidente entre las rutas compiladas
        foreach (var info in rules.RouteParameterRules.Values)
        {
            if (info.PathParamRules.Length > 0 && PathMatchesTemplate(rawPath, info.TemplateSegments))
                return info;
        }

        return routeInfo;
    }

    private static bool LiteralSegmentsMatch(string[] tplSegments, string[] rawSegments, int offset)
    {
        for (int t = 0; t < tplSegments.Length; t++)
        {
            var tSeg = tplSegments[t];
            if (!tSeg.StartsWith('{') && !tSeg.EndsWith('}')
                && !tSeg.Equals(rawSegments[offset + t], StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }
        return true;
    }

    private static void ApplyPathParamMasks(
        CompiledRouteParameterInfo routeInfo, string[] rawSegments, int tplLength, int offset, DataProtectionRulesSettings settings)
    {
        foreach (var (tplIndex, _, rule) in routeInfo.PathParamRules)
        {
            if (tplIndex < 0 || tplIndex >= tplLength)
                continue;

            int rawIdx = offset + tplIndex;
            if (rawIdx >= 0 && rawIdx < rawSegments.Length)
                rawSegments[rawIdx] = MaskSingleValue(rawSegments[rawIdx], rule, settings);
        }
    }

    private static bool PathMatchesTemplate(string rawPath, string[] tplSegments)
    {
        var rawSegments = rawPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        int offset = rawSegments.Length - tplSegments.Length;
        if (offset < 0) return false;

        return LiteralSegmentsMatch(tplSegments, rawSegments, offset);
    }

    private static string MaskUrlQuery(string? rawQuery, MaskingContext context)
    {
        if (string.IsNullOrEmpty(rawQuery)) return rawQuery ?? string.Empty;

        bool hasLeadingQuestion = rawQuery.StartsWith('?');
        string queryBody = hasLeadingQuestion ? rawQuery[1..] : rawQuery;

        var pairs = queryBody.Split('&', StringSplitOptions.RemoveEmptyEntries);
        var routeInfo = context.Rules.FindRouteInfo(context.Method, context.Route);

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

            var rule = ResolveQueryRule(context, routeInfo, key);
            if (rule == DataProtectionRuleType.Remove && context.Settings.Remove)
                continue;

            maskedPairs.Add($"{key}={MaskSingleValue(val, rule, context.Settings)}");
        }

        string res = string.Join('&', maskedPairs);
        return hasLeadingQuestion ? $"?{res}" : res;
    }

    private static DataProtectionRuleType ResolveQueryRule(
        MaskingContext context, CompiledRouteParameterInfo? routeInfo, string key)
    {
        if (routeInfo != null && routeInfo.QueryParamRules.TryGetValue(key, out var rule))
            return rule;

        return context.Rules.GetRule(context.Method, context.Route, key);
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
            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            if (reader.ValueTextEquals(MethodPropName))
                method = ReadStringToken(ref reader) ?? method;
            else if (reader.ValueTextEquals(RoutePropName))
                route = ReadStringToken(ref reader) ?? route;
            else if (reader.ValueTextEquals(NamePropName))
                ApplyNameHeuristic(ReadStringToken(ref reader), ref method, ref route);
        }
    }

    private static string? ReadStringToken(ref Utf8JsonReader reader)
        => reader.Read() && reader.TokenType == JsonTokenType.String ? reader.GetString() : null;

    private static void ApplyNameHeuristic(string? nameValue, ref string method, ref string route)
    {
        if (string.IsNullOrEmpty(nameValue) || !string.IsNullOrEmpty(route))
            return;

        var parts = nameValue.Split(' ', 2);
        if (parts.Length == 2 && parts[0] is "GET" or "POST" or "PUT" or "DELETE" or "PATCH")
        {
            method = parts[0];
            route = parts[1];
        }
        else if (nameValue.StartsWith('/'))
        {
            route = nameValue;
        }
    }
}
