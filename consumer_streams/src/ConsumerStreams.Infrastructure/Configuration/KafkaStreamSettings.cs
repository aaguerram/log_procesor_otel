namespace ConsumerStreams.Infrastructure.Configuration;

/// <summary>
/// Opciones de configuración para el consumidor y productor de streaming en Kafka.
/// </summary>
public class KafkaStreamSettings
{
    public const string SectionName = "KafkaStream";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "consumer-streams-aot-group";
    public string SourceTopic { get; set; } = "tp.observability.application-log.emitted.v1";
    public string TargetTopic { get; set; } = "tp.observability.application-log.processed.v1";
    public string AutoOffsetReset { get; set; } = "Earliest";
    public bool EnableAutoCommit { get; set; } = false;
    public int PollTimeoutMs { get; set; } = 1000;
}
