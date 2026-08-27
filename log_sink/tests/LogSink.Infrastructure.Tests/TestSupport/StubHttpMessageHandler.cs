using System.Net;

namespace LogSink.Infrastructure.Tests.TestSupport;

/// <summary>
/// <see cref="HttpMessageHandler"/> controlado por una cola de respuestas, para probar
/// clientes HTTP sin red. Registra las peticiones recibidas.
/// </summary>
public sealed class StubHttpMessageHandler : HttpMessageHandler
{
    private readonly Queue<Func<HttpRequestMessage, HttpResponseMessage>> _responders = new();

    public List<HttpRequestMessage> Requests { get; } = [];

    public StubHttpMessageHandler RespondWith(HttpStatusCode statusCode, (string Name, string Value)[]? headers = null)
    {
        _responders.Enqueue(_ =>
        {
            var response = new HttpResponseMessage(statusCode);
            foreach (var (name, value) in headers ?? [])
            {
                response.Headers.TryAddWithoutValidation(name, value);
            }

            return response;
        });
        return this;
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Requests.Add(request);
        var responder = _responders.Count > 0 ? _responders.Dequeue() : (_ => new HttpResponseMessage(HttpStatusCode.Created));
        return Task.FromResult(responder(request));
    }
}
