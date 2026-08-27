using ConsumerStreams.Domain.Models;

namespace ConsumerStreams.Domain.Ports;

/// <summary>
/// Puerto para la gestión de caché de contratos OpenAPI y sus reglas de protección de datos.
/// </summary>
public interface IContractRulesCachePort : IDisposable
{
    /// <summary>
    /// Obtiene o compila el árbol inmutable de reglas para un contrato OpenAPI YAML.
    /// Actualiza de forma atómica y thread-safe el TTL deslizante de 10 minutos sin bloqueos ni errores de concurrencia.
    /// </summary>
    CompiledContractRules GetOrCompile(string swaggerYaml);

    /// <summary>
    /// Retorna la cantidad actual de contratos activos en la memoria caché.
    /// </summary>
    int ActiveContractsCount { get; }
}
