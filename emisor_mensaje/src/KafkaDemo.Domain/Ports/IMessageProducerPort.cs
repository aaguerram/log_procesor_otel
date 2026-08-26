using KafkaDemo.Domain.Models;

namespace KafkaDemo.Domain.Ports;

/// <summary>
/// Puerto de salida para el envío y publicación de mensajes a Kafka.
/// </summary>
public interface IMessageProducerPort
{
    Task<MessageResult> SendMessageAsync(KafkaMessage message, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<MessageResult>> SendBatchAsync(IEnumerable<KafkaMessage> messages, CancellationToken cancellationToken = default);
}
