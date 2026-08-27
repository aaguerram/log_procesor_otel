using System.Text;
using ConsumerStreams.Domain.Configuration;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Domain.Utils;
using Produbanco.Security.V1;

namespace ConsumerStreams.Domain.DataProtection;

/// <summary>
/// Decide si un payload descifrado debe enmascararse según las políticas <c>x-log-data-protection</c>
/// y, en caso afirmativo, lo aplica. Sólo se enmascara cuando el mensaje es de tipo <see cref="TelemetryType.Trace"/>,
/// trae contrato <c>swagger</c> y el motor está habilitado.
/// </summary>
public sealed class PayloadMaskingService(
    IContractRulesCachePort contractRulesCache,
    DataProtectionRulesSettings settings)
{
    public bool ShouldMask(EncryptedPayloadEnvelope envelope)
        => settings.Enabled
           && envelope.TelemetryType == TelemetryType.Trace
           && !string.IsNullOrEmpty(envelope.Swagger);

    public string ApplyIfApplicable(EncryptedPayloadEnvelope envelope, string decryptedJson)
    {
        if (!ShouldMask(envelope))
        {
            return decryptedJson;
        }

        var rules = contractRulesCache.GetOrCompile(envelope.Swagger);
        var maskedBytes = JsonStreamDataProtectionMasker.MaskPayload(
            Encoding.UTF8.GetBytes(decryptedJson), rules, settings);

        return Encoding.UTF8.GetString(maskedBytes);
    }
}
