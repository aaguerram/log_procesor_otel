using ConsumerStreams.Domain.Models;

namespace ConsumerStreams.Domain.Contracts;

/// <summary>
/// Compila un contrato OpenAPI (YAML) en el árbol inmutable de reglas <c>x-log-data-protection</c>.
/// Abstracción sobre <see cref="Utils.OpenApiContractCompiler"/> para poder inyectar y sustituir
/// la estrategia de compilación (respeta la inversión de dependencias).
/// </summary>
public interface IContractCompiler
{
    CompiledContractRules Compile(string swaggerYaml);
}
