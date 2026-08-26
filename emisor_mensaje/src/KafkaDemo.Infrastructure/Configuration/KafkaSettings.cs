namespace KafkaDemo.Infrastructure.Configuration;

/// <summary>
/// Opciones de configuración de conexión a Kafka.
/// </summary>
public class KafkaSettings
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string ClientId { get; set; } = "KafkaDemo-Producer";
    public string Acks { get; set; } = "all";
    public bool EnableIdempotence { get; set; } = true;
    public int MessageTimeoutMs { get; set; } = 10000;
    public int RequestTimeoutMs { get; set; } = 5000;
    public int SocketTimeoutMs { get; set; } = 10000;
    public int RetryBackoffMs { get; set; } = 500;
    public int MessageSendMaxRetries { get; set; } = 3;
}
