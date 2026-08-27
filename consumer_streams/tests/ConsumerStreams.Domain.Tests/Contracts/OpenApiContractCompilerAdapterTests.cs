using ConsumerStreams.Domain.Contracts;
using ConsumerStreams.Domain.Tests.TestSupport;

namespace ConsumerStreams.Domain.Tests.Contracts;

public class OpenApiContractCompilerAdapterTests
{
    [Fact]
    public void Compile_DelegatesToOpenApiContractCompiler()
    {
        IContractCompiler compiler = new OpenApiContractCompilerAdapter();

        var rules = compiler.Compile(SampleContracts.TransferManagement);

        Assert.Equal("Transfer.Mspx.Prometeus.Management", rules.ServiceName);
    }
}
