using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Tests.TestSupport;
using ConsumerStreams.Domain.Utils;

namespace ConsumerStreams.Domain.Tests.Utils;

public class OpenApiContractCompilerTests
{
    private readonly CompiledContractRules _rules = OpenApiContractCompiler.Compile(SampleContracts.TransferManagement);

    [Fact]
    public void Compile_ExtractsTitleAndVersion()
    {
        Assert.Equal("Transfer.Mspx.Prometeus.Management", _rules.ServiceName);
        Assert.Equal("2.5.0", _rules.Version);
    }

    [Fact]
    public void Compile_EmptyContract_ReturnsEmptyRuleSet()
    {
        var empty = OpenApiContractCompiler.Compile("");

        Assert.Equal("Unknown", empty.ServiceName);
        Assert.Empty(empty.Operations);
    }

    [Fact]
    public void Compile_ContractKeyIsStableForSameYaml()
    {
        var a = OpenApiContractCompiler.Compile(SampleContracts.TransferManagement);
        var b = OpenApiContractCompiler.Compile(SampleContracts.TransferManagement);

        Assert.Equal(a.ContractKey, b.ContractKey);
    }

    [Theory]
    [InlineData("idClient", DataProtectionRuleType.HashSha256)]
    [InlineData("numCuenta", DataProtectionRuleType.PartialLast4)]
    public void Compile_ResolvesPathAndQueryParameterRules(string property, DataProtectionRuleType expected)
    {
        Assert.Equal(expected, _rules.GetRule("GET", "/contacts/by-id/{idClient}/{channel}", property));
    }

    [Fact]
    public void Compile_ParameterWithoutDirective_DefaultsToFull()
    {
        Assert.Equal(DataProtectionRuleType.Full, _rules.GetRule("GET", "/contacts/by-id/{idClient}/{channel}", "channel"));
    }

    [Theory]
    [InlineData("identificacion", DataProtectionRuleType.PartialLast4)]
    [InlineData("nombreCliente", DataProtectionRuleType.Full)]
    [InlineData("idCliente", DataProtectionRuleType.HashSha256)]
    [InlineData("objJsonResponse", DataProtectionRuleType.Remove)]
    public void Compile_ResolvesRequestBodySchemaPropertiesRecursively(string property, DataProtectionRuleType expected)
    {
        Assert.Equal(expected, _rules.GetRule("POST", "/contacts/local", property));
    }

    [Fact]
    public void Compile_ResponseSchemaPropertiesAreBoundToTheOperation()
    {
        Assert.Equal(DataProtectionRuleType.Full, _rules.GetRule("GET", "/contacts/by-id/{idClient}/{channel}", "saldo"));
    }

    [Fact]
    public void Compile_UnknownOperation_ReturnsFull()
    {
        Assert.Equal(DataProtectionRuleType.Full, _rules.GetRule("DELETE", "/does/not/exist", "anything"));
    }

    [Fact]
    public void FindRouteInfo_ReturnsPathParameterSegmentIndex()
    {
        var info = _rules.FindRouteInfo("GET", "/contacts/by-id/{idClient}/{channel}");

        Assert.NotNull(info);
        Assert.Contains(info!.PathParamRules, r => r.ParamName == "idClient" && r.Rule == DataProtectionRuleType.HashSha256);
    }

    [Theory]
    [InlineData("/x/{id:int}/{y}", "/x/{id}/{y}")]
    [InlineData("/x/{id}", "/x/{id}")]
    [InlineData("", "")]
    public void NormalizeRoute_StripsParameterTypeConstraints(string input, string expected)
    {
        Assert.Equal(expected, OpenApiContractCompiler.NormalizeRoute(input));
    }
}
