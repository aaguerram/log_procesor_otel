using System.Net;

namespace LogSink.Infrastructure.Cosmos;

/// <summary>
/// Fallo transitorio de Cosmos DB (throttling 429 o error de servidor 5xx) que debe
/// activar los reintentos y contar para el Circuit Breaker.
/// </summary>
public sealed class CosmosTransientException(string message, HttpStatusCode? statusCode = null)
    : Exception(message)
{
    public HttpStatusCode? StatusCode { get; } = statusCode;
}
