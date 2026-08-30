using System.Text.Json;
using ConsumerStreams.Application.Services;
using ConsumerStreams.Application.UseCases;
using ConsumerStreams.Domain.Configuration;
using ConsumerStreams.Domain.Contracts;
using ConsumerStreams.Domain.DataProtection;
using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Observability;
using ConsumerStreams.Domain.Ports;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Produbanco.Security.V1;
using Google.Protobuf;

namespace ConsumerStreams.Application.Tests.UseCases;

public class StreamProcessingPipelineUseCaseTests
{
    private readonly Mock<IStreamConsumerPort> _consumer = new();
    private readonly Mock<IStreamProducerPort> _producer = new();
    private readonly Mock<IDlqProducerPort> _dlq = new();
    private readonly Mock<IVaultTokenProviderPort> _vault = new();
    private readonly Mock<IPayloadCryptoPort> _crypto = new();

    private Func<string?, byte[], IDictionary<string, string>?, CancellationToken, Task<bool>>? _handler;

    private sealed class PassthroughCache : IContractRulesCachePort
    {
        private readonly OpenApiContractCompilerAdapter _compiler = new();
        public int ActiveContractsCount => 0;
        public CompiledContractRules GetOrCompile(string swaggerYaml) => _compiler.Compile(swaggerYaml);
        public void Dispose() { }
    }

    public StreamProcessingPipelineUseCaseTests()
    {
        _consumer
            .Setup(c => c.StartStreamingAsync(It.IsAny<string>(),
                It.IsAny<Func<string?, byte[], IDictionary<string, string>?, CancellationToken, Task<bool>>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, Func<string?, byte[], IDictionary<string, string>?, CancellationToken, Task<bool>>, CancellationToken>(
                (_, handler, _) => _handler = handler)
            .Returns(Task.CompletedTask);

        _producer.Setup(p => p.ForwardEventAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<string>(),
                It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _dlq.Setup(d => d.PublishErrorEnvelopeAsync(It.IsAny<string>(), It.IsAny<string?>(),
                It.IsAny<EncryptedErrorPayloadEnvelope>(), It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _vault.Setup(v => v.ResolveKeyByTokenAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new VaultKeyMaterial
            {
                VaultTokenId = "TKN-1", CertThumbprint = "AA11", KeyVersion = "2026.1", AesKey256 = new byte[32]
            });
    }

    private async Task<Func<string?, byte[], IDictionary<string, string>?, CancellationToken, Task<bool>>> StartAsync()
    {
        var useCase = new StreamProcessingPipelineUseCase(
            _consumer.Object, _producer.Object, _dlq.Object,
            new TransactionEnricher(new FakeTimeProvider()),
            new EnvelopeDecryptionService(
                _vault.Object, _crypto.Object,
                new PayloadMaskingService(new PassthroughCache(), new DataProtectionRulesSettings())),
            new FakeTimeProvider(),
            NullLogger<StreamProcessingPipelineUseCase>.Instance);

        await useCase.ExecutePipelineAsync("src", "target", "errors", CancellationToken.None);
        Assert.NotNull(_handler);
        return _handler!;
    }

    private static EncryptedPayloadEnvelope ValidEnvelope(Action<EncryptedPayloadEnvelope>? customize = null)
    {
        var e = new EncryptedPayloadEnvelope
        {
            Data = ByteString.CopyFrom(new byte[] { 1, 2, 3, 4 }),
            Nonce = ByteString.CopyFrom(new byte[12]),
            AuthTag = ByteString.CopyFrom(new byte[16]),
            AlgorithmVersion = 1,
            CertThumbprint = "AA11BB22",
            VaultTokenId = "TKN-KV-V1",
            TransactionId = "TXN-1",
            TimestampUnixMs = 1_800_000_000_000,
            TelemetryType = TelemetryType.Trace,
            ServiceName = "Transfer.Mspx.Prometeus.Management"
        };
        customize?.Invoke(e);
        return e;
    }

    [Fact]
    public async Task Handler_ValidEnvelope_ForwardsDecryptedJsonWithEnrichedHeaders_AndCommits()
    {
        _crypto.Setup(c => c.DecryptEnvelopeToJson(It.IsAny<EncryptedPayloadEnvelope>(), It.IsAny<VaultKeyMaterial>()))
            .Returns("""{"TransactionId":"TXN-1","OriginAccount":"ACCT-42","Amount":2000}""");

        var handler = await StartAsync();
        string? forwardedJson = null;
        IDictionary<string, string>? forwardedHeaders = null;
        _producer.Setup(p => p.ForwardEventAsync("target", It.IsAny<string?>(), It.IsAny<string>(),
                It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()))
            .Callback<string, string?, string, IDictionary<string, string>?, CancellationToken>(
                (_, _, json, headers, _) => { forwardedJson = json; forwardedHeaders = headers; })
            .ReturnsAsync(true);

        var committed = await handler("k1", ValidEnvelope().ToByteArray(), new Dictionary<string, string>(), CancellationToken.None);

        Assert.True(committed);
        Assert.Contains("TXN-1", forwardedJson);
        Assert.Equal("HIGH", forwardedHeaders![StreamHeaders.RiskLevel]);
        Assert.Equal("Transfer_Mspx_Prometeus_Management_Trace", forwardedHeaders[StreamHeaders.TargetCollection]);
        _dlq.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Handler_InvalidEnvelope_RoutesToDlqAndCommitsOffset()
    {
        var handler = await StartAsync();

        var committed = await handler("k1", ValidEnvelope(e => e.ServiceName = "").ToByteArray(), null, CancellationToken.None);

        Assert.True(committed);
        _dlq.Verify(d => d.PublishErrorEnvelopeAsync("errors", It.IsAny<string?>(),
            It.IsAny<EncryptedErrorPayloadEnvelope>(), It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()), Times.Once);
        _producer.Verify(p => p.ForwardEventAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<string>(),
            It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Handler_DecryptionThrows_RoutesToDlq()
    {
        _crypto.Setup(c => c.DecryptEnvelopeToJson(It.IsAny<EncryptedPayloadEnvelope>(), It.IsAny<VaultKeyMaterial>()))
            .Throws(new InvalidOperationException("auth tag mismatch"));

        var handler = await StartAsync();
        var committed = await handler("k1", ValidEnvelope().ToByteArray(), null, CancellationToken.None);

        Assert.True(committed);
        _dlq.Verify(d => d.PublishErrorEnvelopeAsync(It.IsAny<string>(), It.IsAny<string?>(),
            It.Is<EncryptedErrorPayloadEnvelope>(env => env.ErrorDetail.Contains("auth tag mismatch")),
            It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Handler_UndeserializablePayload_RoutesToDlq()
    {
        _crypto.Setup(c => c.DecryptEnvelopeToJson(It.IsAny<EncryptedPayloadEnvelope>(), It.IsAny<VaultKeyMaterial>()))
            .Returns("null");

        var handler = await StartAsync();
        var committed = await handler("k1", ValidEnvelope().ToByteArray(), null, CancellationToken.None);

        Assert.True(committed);
        _dlq.Verify(d => d.PublishErrorEnvelopeAsync(It.IsAny<string>(), It.IsAny<string?>(),
            It.IsAny<EncryptedErrorPayloadEnvelope>(), It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Handler_LogSignalWithNonTransactionalShape_IsForwardedNotDlq()
    {
        // Señal Log de OpenTelemetry: 'eventId' viaja como número, incompatible con el
        // string RawTransactionEvent.EventId. Debe reenviarse igualmente (no ir a la DLQ),
        // ya que el JSON original es lo que se persiste en Cosmos DB.
        const string logJson =
            """{"timestamp":"2026-08-29T23:00:00Z","level":"Information","category":"Auth","message":"login ok","eventId":1001,"properties":{"userId":"USR-1"}}""";
        _crypto.Setup(c => c.DecryptEnvelopeToJson(It.IsAny<EncryptedPayloadEnvelope>(), It.IsAny<VaultKeyMaterial>()))
            .Returns(logJson);

        var handler = await StartAsync();
        string? forwardedJson = null;
        _producer.Setup(p => p.ForwardEventAsync("target", It.IsAny<string?>(), It.IsAny<string>(),
                It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()))
            .Callback<string, string?, string, IDictionary<string, string>?, CancellationToken>(
                (_, _, json, _, _) => forwardedJson = json)
            .ReturnsAsync(true);

        var committed = await handler(
            "k1", ValidEnvelope(e => e.TelemetryType = TelemetryType.Log).ToByteArray(), null, CancellationToken.None);

        Assert.True(committed);
        Assert.Equal(logJson, forwardedJson);
        _dlq.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Handler_MalformedNonProtobufBytes_RoutesToDlq()
    {
        var handler = await StartAsync();

        var committed = await handler("k1", [0x08, 0xFF, 0xFF, 0xFF], null, CancellationToken.None);

        Assert.True(committed);
        _dlq.Verify(d => d.PublishErrorEnvelopeAsync(It.IsAny<string>(), It.IsAny<string?>(),
            It.IsAny<EncryptedErrorPayloadEnvelope>(), It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Handler_DlqPublishAlsoFails_StillCommitsOffset()
    {
        _crypto.Setup(c => c.DecryptEnvelopeToJson(It.IsAny<EncryptedPayloadEnvelope>(), It.IsAny<VaultKeyMaterial>()))
            .Throws(new InvalidOperationException("boom"));
        _dlq.Setup(d => d.PublishErrorEnvelopeAsync(It.IsAny<string>(), It.IsAny<string?>(),
                It.IsAny<EncryptedErrorPayloadEnvelope>(), It.IsAny<IDictionary<string, string>?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("DLQ down"));

        var handler = await StartAsync();
        var committed = await handler("k1", ValidEnvelope().ToByteArray(), null, CancellationToken.None);

        Assert.True(committed);
    }
}
