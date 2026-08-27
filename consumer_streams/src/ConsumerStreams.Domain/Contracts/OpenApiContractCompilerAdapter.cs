using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Utils;

namespace ConsumerStreams.Domain.Contracts;

/// <summary>
/// Implementación por defecto de <see cref="IContractCompiler"/> que delega en el compilador
/// de un solo pase <see cref="OpenApiContractCompiler"/>.
/// </summary>
public sealed class OpenApiContractCompilerAdapter : IContractCompiler
{
    public CompiledContractRules Compile(string swaggerYaml) => OpenApiContractCompiler.Compile(swaggerYaml);
}
