using ConsumerStreams.Domain.Security;
using ConsumerStreams.Domain.Tests.TestSupport;
using Google.Protobuf;
using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.Tests.Security;

public class EnvelopeValidatorTests
{
    [Fact]
    public void Validate_ValidEnvelope_DoesNotThrow()
    {
        EnvelopeValidator.Validate(Envelopes.Valid());
    }

    [Fact]
    public void Validate_SwaggerIsOptional()
    {
        EnvelopeValidator.Validate(Envelopes.Valid(e => e.Swagger = string.Empty));
    }

    [Fact]
    public void Validate_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => EnvelopeValidator.Validate(null!));
    }

    public static TheoryData<string, Action<EncryptedPayloadEnvelope>> InvalidMutations() => new()
    {
        { "data", e => e.Data = ByteString.Empty },
        { "nonce", e => e.Nonce = ByteString.CopyFrom(new byte[8]) },
        { "auth_tag", e => e.AuthTag = ByteString.CopyFrom(new byte[10]) },
        { "algorithm_version", e => e.AlgorithmVersion = 0 },
        { "cert_thumbprint", e => e.CertThumbprint = "" },
        { "vault_token_id", e => e.VaultTokenId = "  " },
        { "transaction_id", e => e.TransactionId = "" },
        { "timestamp_unix_ms", e => e.TimestampUnixMs = 0 },
        { "telemetry_type", e => e.TelemetryType = TelemetryType.Unspecified },
        { "service_name", e => e.ServiceName = "" },
    };

    [Theory]
    [MemberData(nameof(InvalidMutations))]
    public void Validate_MissingMandatoryField_ThrowsWithFieldName(string fieldName, Action<EncryptedPayloadEnvelope> mutate)
    {
        var envelope = Envelopes.Valid(mutate);

        var ex = Assert.Throws<InvalidOperationException>(() => EnvelopeValidator.Validate(envelope));
        Assert.Contains(fieldName, ex.Message);
    }

    [Fact]
    public void IsValid_ReturnsFalseForNullOrInvalid_TrueForValid()
    {
        Assert.False(EnvelopeValidator.IsValid(null));
        Assert.False(EnvelopeValidator.IsValid(Envelopes.Valid(e => e.ServiceName = "")));
        Assert.True(EnvelopeValidator.IsValid(Envelopes.Valid()));
    }
}
