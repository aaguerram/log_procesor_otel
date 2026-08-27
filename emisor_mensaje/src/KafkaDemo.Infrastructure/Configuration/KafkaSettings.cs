namespace KafkaDemo.Infrastructure.Configuration;

/// <summary>
/// Opciones de configuración de conexión a Kafka.
/// Sin valores quemados; se configuran dinámicamente desde IConfiguration / Variables de Entorno.
/// </summary>
public class KafkaSettings
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string Acks { get; set; } = "all";
    public bool EnableIdempotence { get; set; } = true;
    public int MessageTimeoutMs { get; set; }
    public int RequestTimeoutMs { get; set; }
    public int SocketTimeoutMs { get; set; }
    public int RetryBackoffMs { get; set; }
    public int MessageSendMaxRetries { get; set; }
}
