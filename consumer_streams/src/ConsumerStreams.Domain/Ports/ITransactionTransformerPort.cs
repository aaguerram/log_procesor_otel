using ConsumerStreams.Domain.Models;

namespace ConsumerStreams.Domain.Ports;

/// <summary>
/// Puerto de dominio para transformación, validación y enriquecimiento del evento transaccional.
/// </summary>
public interface ITransactionTransformerPort
{
    ProcessedTransactionEvent TransformAndEnrich(RawTransactionEvent rawEvent);
}
