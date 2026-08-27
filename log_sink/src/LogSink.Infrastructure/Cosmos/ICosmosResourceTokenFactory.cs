namespace LogSink.Infrastructure.Cosmos;

/// <summary>
/// Genera el token de autorización maestro (<c>type=master</c>) del protocolo REST de
/// Azure Cosmos DB para una operación concreta sobre un recurso.
/// </summary>
public interface ICosmosResourceTokenFactory
{
    /// <param name="verb">Verbo HTTP (GET, POST, ...).</param>
    /// <param name="resourceType">Tipo de recurso Cosmos (<c>docs</c>, <c>colls</c>, ...).</param>
    /// <param name="resourceLink">Ruta del recurso (<c>dbs/{db}/colls/{coll}</c>).</param>
    /// <param name="utcDate">Cabecera <c>x-ms-date</c> en formato RFC1123.</param>
    /// <param name="primaryKeyBase64">Clave primaria de la cuenta Cosmos en Base64.</param>
    string Create(string verb, string resourceType, string resourceLink, string utcDate, string primaryKeyBase64);
}
