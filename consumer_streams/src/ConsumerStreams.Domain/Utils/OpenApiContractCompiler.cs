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
    private const string RefToken = "$ref:";
    private const string ProtectionDirective = "x-log-data-protection:";

    // Separadores de línea declarados como campo para no reasignar el arreglo en cada compilación.
    private static readonly string[] LineSeparators = ["\r\n", "\n"];

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

        var (title, version) = ExtractContractMetadata(swaggerYaml);
        string contractKey = BuildContractKey(title, version, swaggerYaml);

        var parser = new YamlContractParser();
        foreach (var line in swaggerYaml.Split(LineSeparators, StringSplitOptions.None))
        {
            parser.ConsumeLine(line);
        }
        parser.ResolveSchemaReferences();

        return parser.Build(title, version, contractKey);
    }

    /// <summary>Extrae los metadatos básicos (title, version) mediante expresiones regulares compiladas.</summary>
    private static (string Title, string Version) ExtractContractMetadata(string swaggerYaml)
    {
        string title = "UnknownService";
        string version = "1.0.0";

        var titleMatch = TitleRegex().Match(swaggerYaml);
        if (titleMatch.Success)
            title = titleMatch.Groups[1].Value.Trim();

        var versionMatch = VersionRegex().Match(swaggerYaml);
        if (versionMatch.Success)
            version = versionMatch.Groups[1].Value.Trim();

        return (title, version);
    }

    /// <summary>Calcula la clave criptográfica única del contrato (title:version:sha256[0..16]).</summary>
    private static string BuildContractKey(string title, string version, string swaggerYaml)
    {
        byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(swaggerYaml));
        string hashHex = Convert.ToHexStringLower(hashBytes.AsSpan(0, 16)); // 32 caracteres hex
        return $"{title}:{version}:{hashHex}";
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

    /// <summary>Extrae el nombre del esquema de un valor <c>$ref</c> ('#/components/schemas/Foo' -> 'Foo').</summary>
    private static string ExtractRefSchemaName(string rawRefValue)
    {
        string target = rawRefValue.Trim().Trim('\'', '"');
        return target[(target.LastIndexOf('/') + 1)..];
    }

    private static int FindTemplateSegmentIndex(string route, string paramName)
    {
        var segments = route.Split('/', StringSplitOptions.RemoveEmptyEntries);
        for (int s = 0; s < segments.Length; s++)
        {
            if (segments[s].Trim('{', '}').Equals(paramName, StringComparison.OrdinalIgnoreCase))
                return s;
        }
        return -1;
    }

    /// <summary>
    /// Analizador incremental línea a línea del YAML del contrato. Mantiene el cursor jerárquico
    /// (ruta / método / esquema / propiedad pendiente) y acumula las directivas en tablas mutables
    /// que luego se congelan en <see cref="CompiledContractRules"/>.
    /// </summary>
    private sealed class YamlContractParser
    {
        private readonly Dictionary<string, Dictionary<string, DataProtectionRuleType>> _operations = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, RouteParamBuilder> _routeParamBuilders = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, HashSet<string>> _opSchemaRefs = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, SchemaBuilder> _schemas = new(StringComparer.OrdinalIgnoreCase);

        private bool _inComponents;
        private string _currentPath = string.Empty;
        private string _currentMethod = string.Empty;
        private string _currentSchema = string.Empty;
        private string _pendingProperty = string.Empty;
        private string _currentParamIn = string.Empty;

        public void ConsumeLine(string line)
        {
            string trimmed = line.Trim();

            if (line.StartsWith("components:"))
            {
                _inComponents = true;
                _currentPath = string.Empty;
                _currentMethod = string.Empty;
                _pendingProperty = string.Empty;
                return;
            }

            if (_inComponents)
                ConsumeComponentsLine(line, trimmed);
            else
                ConsumePathsLine(line, trimmed);
        }

        // ---- Sección paths ----------------------------------------------------

        private void ConsumePathsLine(string line, string trimmed)
        {
            if (TryBeginPath(line, trimmed) || TryBeginMethod(line, trimmed))
                return;

            CaptureParamLocation(trimmed);
            CaptureOperationSchemaRef(trimmed);
            CapturePendingProperty(trimmed);
            CaptureOperationProtectionRule(trimmed);
        }

        private bool TryBeginPath(string line, string trimmed)
        {
            if (!line.StartsWith("  /") || !trimmed.EndsWith(':'))
                return false;

            _currentPath = NormalizeRoute(trimmed.TrimEnd(':'));
            _currentMethod = string.Empty;
            _pendingProperty = string.Empty;
            _currentParamIn = string.Empty;
            return true;
        }

        private bool TryBeginMethod(string line, string trimmed)
        {
            if (!line.StartsWith("    ") || line.StartsWith("      ") || !trimmed.EndsWith(':'))
                return false;

            string maybeMethod = trimmed.TrimEnd(':').ToUpperInvariant();
            if (maybeMethod is not ("GET" or "POST" or "PUT" or "DELETE" or "PATCH"))
                return false;

            _currentMethod = maybeMethod;
            _pendingProperty = string.Empty;
            _currentParamIn = string.Empty;
            _opSchemaRefs.TryAdd($"{_currentMethod} {_currentPath}", new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            return true;
        }

        private void CaptureParamLocation(string trimmed)
        {
            if (!trimmed.StartsWith("in:"))
                return;

            var parts = trimmed.Split(':', 2);
            if (parts.Length > 1)
                _currentParamIn = parts[1].Trim().Trim('\'', '"').ToLowerInvariant();
        }

        private void CaptureOperationSchemaRef(string trimmed)
        {
            if (!trimmed.Contains(RefToken))
                return;

            var parts = trimmed.Split(RefToken, 2);
            if (parts.Length <= 1 || string.IsNullOrEmpty(_currentPath) || string.IsNullOrEmpty(_currentMethod))
                return;

            string opKey = $"{_currentMethod} {_currentPath}";
            if (!_opSchemaRefs.TryGetValue(opKey, out var refSet))
            {
                refSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                _opSchemaRefs[opKey] = refSet;
            }
            refSet.Add(ExtractRefSchemaName(parts[1]));
        }

        private void CapturePendingProperty(string trimmed)
        {
            if (trimmed.StartsWith("- name:") || trimmed.StartsWith("name:"))
            {
                var parts = trimmed.Split(':', 2);
                if (parts.Length > 1)
                {
                    _pendingProperty = parts[1].Trim().Trim('\'', '"');
                    if (trimmed.StartsWith("- name:"))
                        _currentParamIn = string.Empty;
                }
            }
            else if (trimmed.EndsWith(':'))
            {
                string key = trimmed.TrimEnd(':');
                if (!IsReservedYamlKeyword(key))
                    _pendingProperty = key;
            }
        }

        private void CaptureOperationProtectionRule(string trimmed)
        {
            if (!TryParseProtectionDirective(trimmed, out var ruleType)
                || string.IsNullOrEmpty(_pendingProperty)
                || string.IsNullOrEmpty(_currentPath)
                || string.IsNullOrEmpty(_currentMethod))
            {
                return;
            }

            string opKey = $"{_currentMethod} {_currentPath}";
            GetOrAddOperation(opKey)[_pendingProperty] = ruleType;

            var builder = GetOrAddRouteParamBuilder(opKey);
            int segmentIdx = FindTemplateSegmentIndex(_currentPath, _pendingProperty);
            if (segmentIdx >= 0 || _currentParamIn == "path")
                builder.PathRules.Add((segmentIdx, _pendingProperty, ruleType));
            else
                builder.QueryRules[_pendingProperty] = ruleType;
        }

        // ---- Sección components / schemas -----------------------------------

        private void ConsumeComponentsLine(string line, string trimmed)
        {
            if (TryBeginSchema(line, trimmed) || string.IsNullOrEmpty(_currentSchema))
                return;

            CaptureSchemaSubRefOrProperty(trimmed);

            if (TryParseProtectionDirective(trimmed, out var ruleType) && !string.IsNullOrEmpty(_pendingProperty))
                _schemas[_currentSchema].Props[_pendingProperty] = ruleType;
        }

        private bool TryBeginSchema(string line, string trimmed)
        {
            if (!line.StartsWith("    ") || line.StartsWith("      ") || !trimmed.EndsWith(':'))
                return false;

            _currentSchema = trimmed.TrimEnd(':');
            if (!_schemas.ContainsKey(_currentSchema))
                _schemas[_currentSchema] = new SchemaBuilder();
            _pendingProperty = string.Empty;
            return true;
        }

        private void CaptureSchemaSubRefOrProperty(string trimmed)
        {
            if (trimmed.Contains(RefToken))
            {
                var parts = trimmed.Split(RefToken, 2);
                if (parts.Length > 1)
                    _schemas[_currentSchema].SubRefs.Add(ExtractRefSchemaName(parts[1]));
            }
            else if (trimmed.EndsWith(':'))
            {
                string key = trimmed.TrimEnd(':');
                if (!IsReservedYamlKeyword(key))
                    _pendingProperty = key;
            }
        }

        private static bool TryParseProtectionDirective(string trimmed, out DataProtectionRuleType ruleType)
        {
            ruleType = DataProtectionRuleType.Full;
            if (!trimmed.Contains(ProtectionDirective))
                return false;

            var parts = trimmed.Split(':', 2);
            if (parts.Length <= 1)
                return false;

            ruleType = ParseRuleType(parts[1].Trim().Trim('\'', '"'));
            return true;
        }

        // ---- Resolución y congelado ---------------------------------------

        private Dictionary<string, DataProtectionRuleType> GetOrAddOperation(string opKey)
        {
            if (!_operations.TryGetValue(opKey, out var opDict))
            {
                opDict = new Dictionary<string, DataProtectionRuleType>(StringComparer.OrdinalIgnoreCase);
                _operations[opKey] = opDict;
            }
            return opDict;
        }

        private RouteParamBuilder GetOrAddRouteParamBuilder(string opKey)
        {
            if (!_routeParamBuilders.TryGetValue(opKey, out var builder))
            {
                builder = new RouteParamBuilder();
                _routeParamBuilders[opKey] = builder;
            }
            return builder;
        }

        public void ResolveSchemaReferences()
        {
            foreach (var (opKey, refSet) in _opSchemaRefs)
            {
                var opDict = GetOrAddOperation(opKey);
                var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var schemaRef in refSet)
                    ResolveSchemaProps(schemaRef, opDict, visited);
            }
        }

        private void ResolveSchemaProps(string schemaName, Dictionary<string, DataProtectionRuleType> target, HashSet<string> visited)
        {
            if (visited.Contains(schemaName) || !_schemas.TryGetValue(schemaName, out var schema))
                return;

            visited.Add(schemaName);
            foreach (var (property, rule) in schema.Props)
                target[property] = rule;

            foreach (var subRef in schema.SubRefs)
                ResolveSchemaProps(subRef, target, visited);
        }

        public CompiledContractRules Build(string title, string version, string contractKey)
        {
            var frozenOps = _operations.ToFrozenDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase);

            var routeParameterRules = new Dictionary<string, CompiledRouteParameterInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var (opKey, builder) in _routeParamBuilders)
            {
                var parts = opKey.Split(' ', 2);
                string routePath = parts.Length > 1 ? parts[1] : opKey;

                var info = new CompiledRouteParameterInfo
                {
                    NormalizedRoute = routePath,
                    TemplateSegments = routePath.Split('/', StringSplitOptions.RemoveEmptyEntries),
                    PathParamRules = builder.PathRules.ToArray(),
                    QueryParamRules = builder.QueryRules.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase)
                };

                routeParameterRules[opKey] = info;
                routeParameterRules[routePath] = info;
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

        private sealed class RouteParamBuilder
        {
            public List<(int SegmentIndex, string ParamName, DataProtectionRuleType Rule)> PathRules { get; } = [];
            public Dictionary<string, DataProtectionRuleType> QueryRules { get; } = new(StringComparer.OrdinalIgnoreCase);
        }

        private sealed class SchemaBuilder
        {
            public Dictionary<string, DataProtectionRuleType> Props { get; } = new(StringComparer.OrdinalIgnoreCase);
            public HashSet<string> SubRefs { get; } = new(StringComparer.OrdinalIgnoreCase);
        }
    }
}
