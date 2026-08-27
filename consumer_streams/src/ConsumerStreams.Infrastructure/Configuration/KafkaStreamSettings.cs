namespace ConsumerStreams.Infrastructure.Configuration;

/// <summary>
/// Opciones de configuración para el consumidor y productor de streaming en Kafka.
/// Sin valores quemados; se configuran dinámicamente desde IConfiguration / Variables de Entorno.
/// </summary>
public class KafkaStreamSettings
{
    public const string SectionName = "KafkaStream";

    public string BootstrapServers { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public string SourceTopic { get; set; } = string.Empty;
    public string TargetTopic { get; set; } = string.Empty;
    public string AutoOffsetReset { get; set; } = "Earliest";
    public bool EnableAutoCommit { get; set; }
    public int PollTimeoutMs { get; set; }
}
