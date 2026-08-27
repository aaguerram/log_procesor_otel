using System.Collections.Frozen;

namespace ConsumerStreams.Domain.Models;

/// <summary>
/// Tipos de políticas de protección de datos institucionales extraídas de x-log-data-protection.
/// </summary>
public enum DataProtectionRuleType : byte
{
    /// <summary>
    /// Registro en claro sin alteraciones.
    /// </summary>
    Full = 0,

    /// <summary>
    /// Anonimización criptográfica SHA-256 en formato hexadecimal minúsculas.
    /// </summary>
    HashSha256 = 1,

    /// <summary>
    /// Enmascaramiento parcial con asteriscos preservando los últimos 4 caracteres.
    /// </summary>
    PartialLast4 = 2,

    /// <summary>
    /// Exclusión o supresión total del campo en el log de observabilidad.
    /// </summary>
    Remove = 3
}

/// <summary>
/// Metadatos de ruta y reglas de parámetros de Path y Query compilados para un endpoint.
/// </summary>
public sealed class CompiledRouteParameterInfo
{
    public required string NormalizedRoute { get; init; }
    public required string[] TemplateSegments { get; init; }
    public required (int TemplateSegmentIndex, string ParamName, DataProtectionRuleType Rule)[] PathParamRules { get; init; }
    public required FrozenDictionary<string, DataProtectionRuleType> QueryParamRules { get; init; }
}

/// <summary>
/// Árbol inmutable y optimizado en memoria de reglas de protección para un contrato específico y su versión.
/// </summary>
public sealed class CompiledContractRules
{
    public required string ServiceName { get; init; }
    public required string Version { get; init; }
    public required string ContractKey { get; init; }

    /// <summary>
    /// Mapa de operaciones: "METODO /ruta" -> "NombrePropiedad/Ruta" -> Tipo de Regla.
    /// Utiliza FrozenDictionary para lecturas concurrentes instantáneas sin contención ni locks.
    /// </summary>
    public required FrozenDictionary<string, FrozenDictionary<string, DataProtectionRuleType>> Operations { get; init; }

    /// <summary>
    /// Metadatos de parámetros de Path y Query compilados por endpoint o ruta.
    /// </summary>
    public FrozenDictionary<string, CompiledRouteParameterInfo> RouteParameterRules { get; init; } = FrozenDictionary<string, CompiledRouteParameterInfo>.Empty;

    /// <summary>
    /// Consulta la regla aplicable en O(1) con 0 asignaciones para la operación exacta.
    /// Prioriza la ruta jerárquica ("parent.property") sobre el nombre simple ("property").
    /// </summary>
    public DataProtectionRuleType GetRule(string httpMethod, string routeTemplate, string fullPropertyPath, string simplePropertyName)
    {
        if (string.IsNullOrEmpty(httpMethod) || string.IsNullOrEmpty(routeTemplate))
            return DataProtectionRuleType.Full;

        string operationKey = $"{httpMethod.ToUpperInvariant()} {routeTemplate}";
        if (Operations.TryGetValue(operationKey, out var opRules))
        {
            // 1. Coincidencia por ruta jerárquica exacta: "clientePrincipal.idCliente"
            if (!string.IsNullOrEmpty(fullPropertyPath) && opRules.TryGetValue(fullPropertyPath, out var pathRule))
            {
                return pathRule;
            }

            // 2. Coincidencia por nombre simple de propiedad: "idCliente"
            if (!string.IsNullOrEmpty(simplePropertyName) && opRules.TryGetValue(simplePropertyName, out var simpleRule))
            {
                return simpleRule;
            }
        }

        // Por defecto si no tiene directiva en la operación: Full (Intacto)
        return DataProtectionRuleType.Full;
    }

    /// <summary>
    /// Sobrecarga para consulta por nombre simple o ruta directa.
    /// </summary>
    public DataProtectionRuleType GetRule(string httpMethod, string routeTemplate, string propertyName) =>
        GetRule(httpMethod, routeTemplate, propertyName, propertyName);

    /// <summary>
    /// Busca la información compilada de parámetros de ruta y consulta para un endpoint.
    /// </summary>
    public CompiledRouteParameterInfo? FindRouteInfo(string httpMethod, string routeTemplate)
    {
        if (RouteParameterRules.Count == 0) return null;

        if (!string.IsNullOrEmpty(httpMethod) && !string.IsNullOrEmpty(routeTemplate))
        {
            string opKey = $"{httpMethod.ToUpperInvariant()} {routeTemplate}";
            if (RouteParameterRules.TryGetValue(opKey, out var info))
                return info;
        }

        if (!string.IsNullOrEmpty(routeTemplate) && RouteParameterRules.TryGetValue(routeTemplate, out var rInfo))
        {
            return rInfo;
        }

        return null;
    }
}
