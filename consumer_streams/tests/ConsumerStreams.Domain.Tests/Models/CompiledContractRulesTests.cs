using System.Collections.Frozen;
using ConsumerStreams.Domain.Models;

namespace ConsumerStreams.Domain.Tests.Models;

public class CompiledContractRulesTests
{
    private static CompiledContractRules Rules(params (string OpKey, (string Prop, DataProtectionRuleType Rule)[] Props)[] operations)
        => new()
        {
            ServiceName = "svc",
            Version = "1.0",
            ContractKey = "k",
            Operations = operations.ToFrozenDictionary(
                o => o.OpKey,
                o => o.Props.ToFrozenDictionary(p => p.Prop, p => p.Rule, StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase)
        };

    [Fact]
    public void GetRule_PrefersHierarchicalPathOverSimpleName()
    {
        var rules = Rules(("GET /x", [
            ("cliente.idCliente", DataProtectionRuleType.HashSha256),
            ("idCliente", DataProtectionRuleType.Full)
        ]));

        Assert.Equal(DataProtectionRuleType.HashSha256, rules.GetRule("GET", "/x", "cliente.idCliente", "idCliente"));
    }

    [Fact]
    public void GetRule_FallsBackToSimpleNameWhenNoHierarchicalMatch()
    {
        var rules = Rules(("GET /x", [("idCliente", DataProtectionRuleType.PartialLast4)]));

        Assert.Equal(DataProtectionRuleType.PartialLast4, rules.GetRule("GET", "/x", "otro.idCliente", "idCliente"));
    }

    [Fact]
    public void GetRule_UnknownOperation_ReturnsFull()
    {
        var rules = Rules(("GET /x", [("p", DataProtectionRuleType.Remove)]));

        Assert.Equal(DataProtectionRuleType.Full, rules.GetRule("POST", "/x", "p", "p"));
    }

    [Fact]
    public void GetRule_IsCaseInsensitiveOnMethod()
    {
        var rules = Rules(("GET /x", [("p", DataProtectionRuleType.Remove)]));

        Assert.Equal(DataProtectionRuleType.Remove, rules.GetRule("get", "/x", "p"));
    }

    [Theory]
    [InlineData("", "/x")]
    [InlineData("GET", "")]
    public void GetRule_BlankMethodOrRoute_ReturnsFull(string method, string route)
    {
        var rules = Rules(("GET /x", [("p", DataProtectionRuleType.Remove)]));

        Assert.Equal(DataProtectionRuleType.Full, rules.GetRule(method, route, "p"));
    }
}
