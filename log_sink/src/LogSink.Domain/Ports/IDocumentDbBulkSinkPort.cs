using LogSink.Domain.Models;

namespace LogSink.Domain.Ports;

/// <summary>
/// Puerto de persistencia masiva (Bulk Sink) hacia Azure Cosmos DB / DocumentDB.
/// </summary>
public interface IDocumentDbBulkSinkPort
{
    /// <summary>
    /// Inserta en paralelo el JSON exacto recibido de cada evento. Los documentos que fallen
    /// tras la política de resiliencia se derivan individualmente a la DLQ.
    /// </summary>
    Task<BulkSinkResult> BulkInsertRawJsonLogsAsync(
        IReadOnlyList<LogSinkItem> items,
        CancellationToken cancellationToken = default);
}
