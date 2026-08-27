using System.Text.Json;
using ConsumerStreams.Domain.Configuration;
using ConsumerStreams.Domain.Contracts;
using ConsumerStreams.Domain.DataProtection;
using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Domain.Tests.TestSupport;
using Moq;
using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Tests.DataProtection;

public class PayloadMaskingServiceTests
{
    private sealed class InMemoryCache(IContractCompiler compiler) : IContractRulesCachePort
    {
        public int Compilations { get; private set; }
        public int ActiveContractsCount => 1;
        public CompiledContractRules GetOrCompile(string swaggerYaml)
        {
            Compilations++;
            return compiler.Compile(swaggerYaml);
        }
        public void Dispose() { }
    }

    private readonly InMemoryCache _cache = new(new OpenApiContractCompilerAdapter());

    private PayloadMaskingService Service(DataProtectionRulesSettings? settings = null)
        => new(_cache, settings ?? new DataProtectionRulesSettings());

    [Fact]
    public void ApplyIfApplicable_TraceWithSwagger_AppliesMask()
    {
        var envelope = Envelopes.Valid(e =>
        {
            e.TelemetryType = TelemetryType.Trace;
            e.Swagger = MaskingContract.Yaml;
        });
        var json = """{"http.request.method":"GET","http.route":"/accounts/{numCuenta}/detail","cuenta":"2200123456"}""";

        var masked = Service().ApplyIfApplicable(envelope, json);

        Assert.Equal("******3456", JsonDocument.Parse(masked).RootElement.GetProperty("cuenta").GetString());
    }

    [Fact]
    public void ApplyIfApplicable_NonTrace_ReturnsOriginal()
    {
        var envelope = Envelopes.Valid(e =>
        {
            e.TelemetryType = TelemetryType.Metric;
            e.Swagger = MaskingContract.Yaml;
        });
        const string json = """{"cuenta":"2200123456"}""";

        Assert.Equal(json, Service().ApplyIfApplicable(envelope, json));
    }

    [Fact]
    public void ApplyIfApplicable_TraceWithoutSwagger_ReturnsOriginal()
    {
        var envelope = Envelopes.Valid(e => { e.TelemetryType = TelemetryType.Trace; e.Swagger = ""; });
        const string json = """{"cuenta":"2200123456"}""";

        Assert.Equal(json, Service().ApplyIfApplicable(envelope, json));
        Assert.Equal(0, _cache.Compilations);
    }

    [Fact]
    public void ApplyIfApplicable_EngineDisabled_ReturnsOriginal()
    {
        var envelope = Envelopes.Valid(e => { e.TelemetryType = TelemetryType.Trace; e.Swagger = MaskingContract.Yaml; });
        const string json = """{"cuenta":"2200123456"}""";

        Assert.Equal(json, Service(new DataProtectionRulesSettings { Enabled = false }).ApplyIfApplicable(envelope, json));
    }

    [Fact]
    public void ShouldMask_ReflectsTelemetryTypeAndSwaggerAndFlag()
    {
        var service = Service();

        Assert.True(service.ShouldMask(Envelopes.Valid(e => { e.TelemetryType = TelemetryType.Trace; e.Swagger = "x"; })));
        Assert.False(service.ShouldMask(Envelopes.Valid(e => { e.TelemetryType = TelemetryType.Log; e.Swagger = "x"; })));
    }
}
