namespace ConsumerStreams.Domain.Observability;

/// <summary>Nombres canónicos de las cabeceras Kafka que emite el pipeline de streaming.</summary>
public static class StreamHeaders
{
    public const string StreamProcessor = "x-stream-processor";
    public const string DecryptionAlgorithm = "x-decryption-algorithm";
    public const string VaultToken = "x-vault-token";
    public const string ServiceName = "x-service-name";
    public const string TelemetryType = "x-telemetry-type";
    public const string TargetCollection = "x-target-collection";
    public const string ProcessedStatus = "x-processed-status";
    public const string RiskLevel = "x-risk-level";
    public const string LatencyMs = "x-latency-ms";

    public const string ErrorType = "x-error-type";
    public const string ErrorMessage = "x-error-message";
    public const string ErrorTimestamp = "x-error-timestamp";
    public const string SourceTopic = "x-source-topic";
    public const string ErrorHandler = "x-error-handler";

    public const string StreamProcessorValue = "ConsumerStreams.NativeAOT";
    public const string DecryptionAlgorithmValue = "AES-256-GCM";
    public const string ErrorHandlerValue = "ConsumerStreams.DLQ";
}
