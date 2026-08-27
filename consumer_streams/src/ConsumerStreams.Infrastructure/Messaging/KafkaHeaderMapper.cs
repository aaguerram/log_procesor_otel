using System.Text;
using Confluent.Kafka;

namespace ConsumerStreams.Infrastructure.Messaging;

/// <summary>
/// Conversión entre las cabeceras nativas de Confluent.Kafka y un diccionario <c>string -&gt; string</c>.
/// Elimina el bucle de mapeo repetido en cada adaptador de productor / consumidor.
/// </summary>
public static class KafkaHeaderMapper
{
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
