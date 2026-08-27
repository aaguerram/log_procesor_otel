using System.Collections.Frozen;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using ConsumerStreams.Domain.Models;

namespace ConsumerStreams.Domain.Utils;

/// <summary>
/// Compilador ultra-rápido de contratos OpenAPI YAML para extraer árboles compactos de reglas x-log-data-protection.
/// </summary>
public static partial class OpenApiContractCompiler
{
    [GeneratedRegex(@"^\s+title:\s*(.+)$", RegexOptions.Multiline)]
    private static partial Regex TitleRegex();

    [GeneratedRegex(@"^\s+version:\s*['""]?([^'""]+)['""]?$", RegexOptions.Multiline)]
    private static partial Regex VersionRegex();

    [GeneratedRegex(@"\{(\w+)(?::[^\}]+)?\}")]
    private static partial Regex RouteParamNormalizer();

    public static CompiledContractRules Compile(string swaggerYaml)
    {
        if (string.IsNullOrWhiteSpace(swaggerYaml))
        {
            return new CompiledContractRules
            {
                ServiceName = "Unknown",
                Version = "1.0.0",
                ContractKey = "empty",
                Operations = FrozenDictionary<string, FrozenDictionary<string, DataProtectionRuleType>>.Empty,
                GlobalPropertyRules = FrozenDictionary<string, DataProtectionRuleType>.Empty
            };
        }

        // 1. Extraer Metadatos Básicos (Title, Version)
        string title = "UnknownService";
        string version = "1.0.0";

        var titleMatch = TitleRegex().Match(swaggerYaml);
        if (titleMatch.Success)
            title = titleMatch.Groups[1].Value.Trim();

        var versionMatch = VersionRegex().Match(swaggerYaml);
        if (versionMatch.Success)
            version = versionMatch.Groups[1].Value.Trim();

        // 2. Calcular Key Criptográfica Única del Contrato
        byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(swaggerYaml));
        string hashHex = Convert.ToHexStringLower(hashBytes.AsSpan(0, 16)); // 32 caracteres hex
        string contractKey = $"{title}:{version}:{hashHex}";

        // 3. Extracción de Operaciones, Parámetros y Esquemas en un solo pase
        var operations = new Dictionary<string, Dictionary<string, DataProtectionRuleType>>(StringComparer.OrdinalIgnoreCase);
        var globalRules = new Dictionary<string, DataProtectionRuleType>(StringComparer.OrdinalIgnoreCase);

        var lines = swaggerYaml.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
        string currentPath = string.Empty;
        string currentMethod = string.Empty;
        string pendingProperty = string.Empty;

        for (int i = 0; i < lines.Length; i++)
        {
            string line = lines[i];
            string trimmed = line.Trim();

            // Detectar Path: "  /contacts/..."
            if (line.StartsWith("  /") && trimmed.EndsWith(':'))
            {
                currentPath = NormalizeRoute(trimmed.TrimEnd(':'));
                currentMethod = string.Empty;
                pendingProperty = string.Empty;
                continue;
            }

            // Detectar Método HTTP: "    get:" o "    post:"
            if (line.StartsWith("    ") && !line.StartsWith("      ") && trimmed.EndsWith(':'))
            {
                string maybeMethod = trimmed.TrimEnd(':').ToUpperInvariant();
                if (maybeMethod is "GET" or "POST" or "PUT" or "DELETE" or "PATCH")
                {
                    currentMethod = maybeMethod;
                    pendingProperty = string.Empty;
                    continue;
                }
            }

            // Detectar nombres de parámetros o propiedades
            if (trimmed.StartsWith("- name:") || trimmed.StartsWith("name:"))
            {
                var parts = trimmed.Split(':', 2);
                if (parts.Length > 1)
                    pendingProperty = parts[1].Trim().Trim('\'', '"');
            }
            else if (trimmed.EndsWith(':') && 
                     !trimmed.StartsWith("properties:") && 
                     !trimmed.StartsWith("schema:") && 
                     !trimmed.StartsWith("tags:") &&
                     !trimmed.StartsWith("responses:") &&
                     !trimmed.StartsWith("content:") &&
                     !trimmed.StartsWith("parameters:") &&
                     !trimmed.StartsWith("components:"))
            {
                pendingProperty = trimmed.TrimEnd(':');
            }

            // Detectar directiva x-log-data-protection
            if (trimmed.Contains("x-log-data-protection:"))
            {
                var parts = trimmed.Split(':', 2);
                if (parts.Length > 1)
                {
                    string ruleText = parts[1].Trim().Trim('\'', '"');
                    var ruleType = ParseRuleType(ruleText);

                    if (!string.IsNullOrEmpty(pendingProperty))
                    {
                        // 1. Asignar al Endpoint actual si estamos dentro de un path/método
                        if (!string.IsNullOrEmpty(currentPath) && !string.IsNullOrEmpty(currentMethod))
                        {
                            string opKey = $"{currentMethod} {currentPath}";
                            if (!operations.TryGetValue(opKey, out var opDict))
                            {
                                opDict = new Dictionary<string, DataProtectionRuleType>(StringComparer.OrdinalIgnoreCase);
                                operations[opKey] = opDict;
                            }
                            opDict[pendingProperty] = ruleType;
                        }

                        // 2. Asignar al diccionario global fallback de propiedades
                        globalRules[pendingProperty] = ruleType;
                    }
                }
            }
        }

        // 4. Convertir a FrozenDictionaries inmutables de alto rendimiento
        var frozenOps = operations.ToFrozenDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
            StringComparer.OrdinalIgnoreCase);

        var frozenGlobals = globalRules.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

        return new CompiledContractRules
        {
            ServiceName = title,
            Version = version,
            ContractKey = contractKey,
            Operations = frozenOps,
            GlobalPropertyRules = frozenGlobals
        };
    }

    public static string NormalizeRoute(string route)
    {
        if (string.IsNullOrEmpty(route)) return string.Empty;
        // Convierte "/contacts/contacts-by-idClient/{idClient:int}/{channel}" a "/contacts/contacts-by-idClient/{idClient}/{channel}"
        return RouteParamNormalizer().Replace(route, "{$1}").Trim();
    }

    private static DataProtectionRuleType ParseRuleType(string ruleText)
    {
        if (ruleText.Contains("Hash", StringComparison.OrdinalIgnoreCase))
            return DataProtectionRuleType.HashSha256;

        if (ruleText.Contains("Partial", StringComparison.OrdinalIgnoreCase))
            return DataProtectionRuleType.PartialLast4;

        if (ruleText.Contains("Remove", StringComparison.OrdinalIgnoreCase))
            return DataProtectionRuleType.Remove;

        return DataProtectionRuleType.Full;
    }
}
