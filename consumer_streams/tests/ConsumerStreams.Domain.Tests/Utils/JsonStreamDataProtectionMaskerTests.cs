using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ConsumerStreams.Domain.Configuration;
using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Tests.TestSupport;
using ConsumerStreams.Domain.Utils;

namespace ConsumerStreams.Domain.Tests.Utils;

public class JsonStreamDataProtectionMaskerTests
{
    private static readonly CompiledContractRules Rules = OpenApiContractCompiler.Compile(MaskingContract.Yaml);

    private static string Mask(string json, DataProtectionRulesSettings? settings = null)
    {
        var output = JsonStreamDataProtectionMasker.MaskPayload(
            Encoding.UTF8.GetBytes(json), Rules, settings ?? new DataProtectionRulesSettings());
        return Encoding.UTF8.GetString(output);
    }

    private static string Sha256Hex(string value)
        => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public void MaskPayload_HashesFieldsMarkedHashSha256()
    {
        var json = """{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","identificacion":"1712345678"}""";

        var result = Parse(Mask(json));

        Assert.Equal(Sha256Hex("1712345678"), result.GetProperty("identificacion").GetString());
    }

    [Fact]
    public void MaskPayload_PartiallyMasksFieldsMarkedPartialLast4()
    {
        var json = """{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","cuenta":"2200123456"}""";

        var result = Parse(Mask(json));

        Assert.Equal("******3456", result.GetProperty("cuenta").GetString());
    }

    [Fact]
    public void MaskPayload_DropsFieldsMarkedRemove()
    {
        var json = """{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","objJsonResponse":"secret-blob","nombreCliente":"Ada"}""";

        var result = Parse(Mask(json));

        Assert.False(result.TryGetProperty("objJsonResponse", out _));
        Assert.Equal("Ada", result.GetProperty("nombreCliente").GetString());
    }

    [Fact]
    public void MaskPayload_LeavesUnmarkedAndFullFieldsUntouched()
    {
        var json = """{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","nombreCliente":"Grace Hopper","monto":1500.5}""";

        var result = Parse(Mask(json));

        Assert.Equal("Grace Hopper", result.GetProperty("nombreCliente").GetString());
        Assert.Equal(1500.5, result.GetProperty("monto").GetDouble());
    }

    [Fact]
    public void MaskPayload_WhenEngineDisabled_ReturnsInputVerbatim()
    {
        var json = """{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","identificacion":"1712345678"}""";

        var result = Mask(json, new DataProtectionRulesSettings { Enabled = false });

        Assert.Equal(json, result);
    }

    [Fact]
    public void MaskPayload_WhenRuleTypeSwitchedOff_LeavesValueInClear()
    {
        var json = """{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","identificacion":"1712345678"}""";

        var result = Parse(Mask(json, new DataProtectionRulesSettings { HashSha256 = false }));

        Assert.Equal("1712345678", result.GetProperty("identificacion").GetString());
    }

    [Fact]
    public void MaskPayload_MasksPathParametersInsideUrlPath()
    {
        var json = """{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","url.path":"/accounts/2200123456/detail"}""";

        var result = Parse(Mask(json));

        Assert.Equal("/accounts/******3456/detail", result.GetProperty("url.path").GetString());
    }

    [Fact]
    public void MaskPayload_MasksQueryParameterValues()
    {
        var json = """{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","url.query":"token=abc123&page=2"}""";

        var result = Parse(Mask(json)).GetProperty("url.query").GetString()!;

        Assert.Contains($"token={Sha256Hex("abc123")}", result);
        Assert.Contains("page=2", result);
    }

    [Fact]
    public void MaskPayload_RecursesIntoJsonEmbeddedInResponseBodyPreview()
    {
        var inner = """{\"identificacion\":\"1712345678\",\"nombreCliente\":\"Ada\"}""";
        var json = $$"""{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","http.response.body_preview":"{{inner}}"}""";

        var maskedPreview = Parse(Mask(json)).GetProperty("http.response.body_preview").GetString()!;
        var innerParsed = JsonDocument.Parse(maskedPreview).RootElement;

        Assert.Equal(Sha256Hex("1712345678"), innerParsed.GetProperty("identificacion").GetString());
        Assert.Equal("Ada", innerParsed.GetProperty("nombreCliente").GetString());
    }

    [Fact]
    public void MaskPayload_EmptyInput_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, Mask(string.Empty));
    }
}
