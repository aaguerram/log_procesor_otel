using System.Text;
using Confluent.Kafka;

namespace LogSink.Infrastructure.Messaging;

/// <summary>
/// Conversión entre las cabeceras nativas de Confluent.Kafka y un diccionario de dominio
/// <c>string -&gt; string</c>. Elimina el bucle de mapeo que estaba repetido en cada adaptador
/// de productor y consumidor.
/// </summary>
public static class KafkaHeaderMapper
{
    /// <summary>Materializa las cabeceras de un mensaje consumido como diccionario UTF-8.</summary>
    public static Dictionary<string, string> ToDictionary(Headers? headers)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (headers is null)
        {
            return result;
        }

        foreach (var header in headers)
        {
            result[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
        }

        return result;
    }

    /// <summary>Construye cabeceras Kafka a partir de un diccionario, ignorando valores nulos.</summary>
    public static Headers ToKafkaHeaders(IEnumerable<KeyValuePair<string, string>>? headers)
    {
        var result = new Headers();
        if (headers is null)
        {
            return result;
        }

        foreach (var (key, value) in headers)
        {
            if (value is not null)
            {
                result.Add(key, Encoding.UTF8.GetBytes(value));
            }
        }

        return result;
    }
}
