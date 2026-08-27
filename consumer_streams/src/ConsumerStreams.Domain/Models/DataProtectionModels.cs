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
    /// Mapa global fallback de propiedades por si no se especifica ruta: "NombrePropiedad" -> Tipo de Regla.
    /// </summary>
    public required FrozenDictionary<string, DataProtectionRuleType> GlobalPropertyRules { get; init; }

    /// <summary>
    /// Consulta la regla aplicable en O(1) con 0 asignaciones.
    /// </summary>
    public DataProtectionRuleType GetRule(string httpMethod, string routeTemplate, string propertyName)
    {
        // 1. Intento por operación exacta: "METHOD /route"
        if (!string.IsNullOrEmpty(httpMethod) && !string.IsNullOrEmpty(routeTemplate))
        {
            string operationKey = $"{httpMethod.ToUpperInvariant()} {routeTemplate}";
            if (Operations.TryGetValue(operationKey, out var opRules) &&
                opRules.TryGetValue(propertyName, out var rule))
            {
                return rule;
            }
        }

        // 2. Fallback por nombre de propiedad en el contrato
        if (GlobalPropertyRules.TryGetValue(propertyName, out var globalRule))
        {
            return globalRule;
        }

        // 3. Por defecto si no tiene directiva: Full (Intacto)
        return DataProtectionRuleType.Full;
    }
}
