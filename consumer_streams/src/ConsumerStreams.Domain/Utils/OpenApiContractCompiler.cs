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
    [GeneratedRegex(@"^[ \t]+title:[ \t]*(.+?)[ \t]*$", RegexOptions.Multiline)]
    private static partial Regex TitleRegex();

    // El grupo excluye CR/LF para que la captura no se derrame a las líneas siguientes
    // cuando el valor no viene entre comillas (bug: [^'""]+ es codicioso y consume saltos de línea).
    [GeneratedRegex(@"^[ \t]+version:[ \t]*['""]?([^'""\r\n]+?)['""]?[ \t]*$", RegexOptions.Multiline)]
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
                Operations = FrozenDictionary<string, FrozenDictionary<string, DataProtectionRuleType>>.Empty
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
        var routeParamBuilders = new Dictionary<string, (List<(int SegmentIndex, string ParamName, DataProtectionRuleType Rule)> PathRules, Dictionary<string, DataProtectionRuleType> QueryRules)>(StringComparer.OrdinalIgnoreCase);

        var opSchemaRefs = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        var schemas = new Dictionary<string, (Dictionary<string, DataProtectionRuleType> Props, HashSet<string> SubRefs)>(StringComparer.OrdinalIgnoreCase);

        var lines = swaggerYaml.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
        bool inComponents = false;
        string currentPath = string.Empty;
        string currentMethod = string.Empty;
        string currentSchema = string.Empty;
        string pendingProperty = string.Empty;
        string currentParamIn = string.Empty;

        for (int i = 0; i < lines.Length; i++)
        {
            string line = lines[i];
            string trimmed = line.Trim();

            if (line.StartsWith("components:"))
            {
                inComponents = true;
                currentPath = string.Empty;
                currentMethod = string.Empty;
                pendingProperty = string.Empty;
                continue;
            }

            if (!inComponents)
            {
                // Detectar Path: "  /contacts/..."
                if (line.StartsWith("  /") && trimmed.EndsWith(':'))
                {
                    currentPath = NormalizeRoute(trimmed.TrimEnd(':'));
                    currentMethod = string.Empty;
                    pendingProperty = string.Empty;
                    currentParamIn = string.Empty;
                    continue;
                }

                // Detectar Método HTTP: "    get:" o "    post:" o "    put:" o "    delete:"
                if (line.StartsWith("    ") && !line.StartsWith("      ") && trimmed.EndsWith(':'))
                {
                    string maybeMethod = trimmed.TrimEnd(':').ToUpperInvariant();
                    if (maybeMethod is "GET" or "POST" or "PUT" or "DELETE" or "PATCH")
                    {
                        currentMethod = maybeMethod;
                        pendingProperty = string.Empty;
                        currentParamIn = string.Empty;

                        string opKey = $"{currentMethod} {currentPath}";
                        if (!opSchemaRefs.ContainsKey(opKey))
                            opSchemaRefs[opKey] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        continue;
                    }
                }

                // Detectar "in: path" o "in: query"
                if (trimmed.StartsWith("in:"))
                {
                    var inParts = trimmed.Split(':', 2);
                    if (inParts.Length > 1)
                        currentParamIn = inParts[1].Trim().Trim('\'', '"').ToLowerInvariant();
                }

                // Detectar $ref en operación
                if (trimmed.Contains("$ref:"))
                {
                    var refParts = trimmed.Split("$ref:", 2);
                    if (refParts.Length > 1)
                    {
                        string refTarget = refParts[1].Trim().Trim('\'', '"');
                        string schemaName = refTarget.Substring(refTarget.LastIndexOf('/') + 1);
                        if (!string.IsNullOrEmpty(currentPath) && !string.IsNullOrEmpty(currentMethod))
                        {
                            string opKey = $"{currentMethod} {currentPath}";
                            if (!opSchemaRefs.TryGetValue(opKey, out var refSet))
                            {
                                refSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                opSchemaRefs[opKey] = refSet;
                            }
                            refSet.Add(schemaName);
                        }
                    }
                }

                // Detectar nombres de parámetros
                if (trimmed.StartsWith("- name:") || trimmed.StartsWith("name:"))
                {
                    var parts = trimmed.Split(':', 2);
                    if (parts.Length > 1)
                    {
                        pendingProperty = parts[1].Trim().Trim('\'', '"');
                        if (trimmed.StartsWith("- name:"))
                            currentParamIn = string.Empty;
                    }
                }
                else if (trimmed.EndsWith(':'))
                {
                    string key = trimmed.TrimEnd(':');
                    if (!IsReservedYamlKeyword(key))
                    {
                        pendingProperty = key;
                    }
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
                            if (!string.IsNullOrEmpty(currentPath) && !string.IsNullOrEmpty(currentMethod))
                            {
                                string opKey = $"{currentMethod} {currentPath}";
                                if (!operations.TryGetValue(opKey, out var opDict))
                                {
                                    opDict = new Dictionary<string, DataProtectionRuleType>(StringComparer.OrdinalIgnoreCase);
                                    operations[opKey] = opDict;
                                }
                                opDict[pendingProperty] = ruleType;

                                if (!routeParamBuilders.TryGetValue(opKey, out var paramBuilder))
                                {
                                    paramBuilder = (new List<(int, string, DataProtectionRuleType)>(), new Dictionary<string, DataProtectionRuleType>(StringComparer.OrdinalIgnoreCase));
                                    routeParamBuilders[opKey] = paramBuilder;
                                }

                                var segments = currentPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
                                int segmentIdx = -1;
                                for (int s = 0; s < segments.Length; s++)
                                {
                                    var seg = segments[s].Trim('{', '}');
                                    if (seg.Equals(pendingProperty, StringComparison.OrdinalIgnoreCase))
                                    {
                                        segmentIdx = s;
                                        break;
                                    }
                                }

                                if (segmentIdx >= 0 || currentParamIn == "path")
                                {
                                    paramBuilder.PathRules.Add((segmentIdx, pendingProperty, ruleType));
                                }
                                else
                                {
                                    paramBuilder.QueryRules[pendingProperty] = ruleType;
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                // Sección Components -> Schemas
                if (line.StartsWith("    ") && !line.StartsWith("      ") && trimmed.EndsWith(':'))
                {
                    currentSchema = trimmed.TrimEnd(':');
                    if (!schemas.ContainsKey(currentSchema))
                    {
                        schemas[currentSchema] = (new Dictionary<string, DataProtectionRuleType>(StringComparer.OrdinalIgnoreCase), new HashSet<string>(StringComparer.OrdinalIgnoreCase));
                    }
                    pendingProperty = string.Empty;
                    continue;
                }

                if (!string.IsNullOrEmpty(currentSchema))
                {
                    if (trimmed.Contains("$ref:"))
                    {
                        var refParts = trimmed.Split("$ref:", 2);
                        if (refParts.Length > 1)
                        {
                            string refTarget = refParts[1].Trim().Trim('\'', '"');
                            string subSchema = refTarget.Substring(refTarget.LastIndexOf('/') + 1);
                            schemas[currentSchema].SubRefs.Add(subSchema);
                        }
                    }
                    else if (trimmed.EndsWith(':'))
                    {
                        string key = trimmed.TrimEnd(':');
                        if (!IsReservedYamlKeyword(key))
                        {
                            pendingProperty = key;
                        }
                    }

                    if (trimmed.Contains("x-log-data-protection:"))
                    {
                        var parts = trimmed.Split(':', 2);
                        if (parts.Length > 1)
                        {
                            string ruleText = parts[1].Trim().Trim('\'', '"');
                            var ruleType = ParseRuleType(ruleText);
                            if (!string.IsNullOrEmpty(pendingProperty))
                            {
                                schemas[currentSchema].Props[pendingProperty] = ruleType;
                            }
                        }
                    }
                }
            }
        }

        // 4. Resolver y Vincular Propiedades de Esquemas referenciados a cada Operación
        void ResolveSchemaProps(string schemaName, Dictionary<string, DataProtectionRuleType> targetDict, HashSet<string> visited)
        {
            if (visited.Contains(schemaName) || !schemas.TryGetValue(schemaName, out var sEntry))
                return;

            visited.Add(schemaName);
            foreach (var (p, r) in sEntry.Props)
            {
                targetDict[p] = r;
            }
            foreach (var subRef in sEntry.SubRefs)
            {
                ResolveSchemaProps(subRef, targetDict, visited);
            }
        }

        foreach (var (opKey, refSet) in opSchemaRefs)
        {
            if (!operations.TryGetValue(opKey, out var opDict))
            {
                opDict = new Dictionary<string, DataProtectionRuleType>(StringComparer.OrdinalIgnoreCase);
                operations[opKey] = opDict;
            }

            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var sRef in refSet)
            {
                ResolveSchemaProps(sRef, opDict, visited);
            }
        }

        // 5. Convertir a FrozenDictionaries inmutables de alto rendimiento
        var frozenOps = operations.ToFrozenDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
            StringComparer.OrdinalIgnoreCase);

        var routeParameterRules = new Dictionary<string, CompiledRouteParameterInfo>(StringComparer.OrdinalIgnoreCase);
        foreach (var (opKey, (pathRules, queryRules)) in routeParamBuilders)
        {
            var parts = opKey.Split(' ', 2);
            string rPath = parts.Length > 1 ? parts[1] : opKey;
            var segments = rPath.Split('/', StringSplitOptions.RemoveEmptyEntries);

            var info = new CompiledRouteParameterInfo
            {
                NormalizedRoute = rPath,
                TemplateSegments = segments,
                PathParamRules = pathRules.ToArray(),
                QueryParamRules = queryRules.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase)
            };

            routeParameterRules[opKey] = info;
            routeParameterRules[rPath] = info;
        }

        return new CompiledContractRules
        {
            ServiceName = title,
            Version = version,
            ContractKey = contractKey,
            Operations = frozenOps,
            RouteParameterRules = routeParameterRules.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase)
        };
    }

    private static bool IsReservedYamlKeyword(string key) =>
        key is "properties" or "schema" or "tags" or "responses" or "content" or 
               "parameters" or "components" or "type" or "format" or "description" or 
               "required" or "example" or "examples" or "default" or "items" or 
               "application/json" or "requestBody" or "summary" or "operationId" or
               "minimum" or "maximum" or "minLength" or "maxLength" or "pattern" or
               "200" or "201" or "400" or "401" or "403" or "404" or "500";

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
