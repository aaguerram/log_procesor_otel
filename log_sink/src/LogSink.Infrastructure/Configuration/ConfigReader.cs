using Microsoft.Extensions.Configuration;

namespace LogSink.Infrastructure.Configuration;

/// <summary>
/// Lectura tipada y tolerante de <see cref="IConfiguration"/>: acepta una lista de claves
/// alternativas (jerárquicas <c>Seccion:Clave</c> o planas <c>TECH-INT-...</c> / <c>TECH_INT_...</c>)
/// y devuelve el primer valor no vacío. Concentra el patrón de resolución que antes estaba
/// duplicado decenas de veces en <see cref="DependencyInjection"/>.
/// </summary>
public static class ConfigReader
{
    /// <summary>Primer valor no vacío entre las claves indicadas, o <c>null</c>.</summary>
    public static string? FirstValue(this IConfiguration configuration, params string[] keys)
    {
        foreach (var key in keys)
        {
            var value = configuration[key];
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        return null;
    }

    /// <summary>Primer valor no vacío, o lanza <see cref="InvalidOperationException"/> con mensaje de configuración.</summary>
    public static string Required(this IConfiguration configuration, string friendlyName, params string[] keys)
    {
        return configuration.FirstValue(keys)
            ?? throw new InvalidOperationException(
                $"[CONFIG ERROR] '{friendlyName}' no está configurado en appsettings.json ni en las variables de entorno.");
    }

    /// <summary>Primer valor no vacío, o el valor por defecto indicado.</summary>
    public static string ValueOrDefault(this IConfiguration configuration, string defaultValue, params string[] keys)
        => configuration.FirstValue(keys) ?? defaultValue;

    public static int IntOrDefault(this IConfiguration configuration, int defaultValue, params string[] keys)
        => int.TryParse(configuration.FirstValue(keys), out var parsed) ? parsed : defaultValue;

    public static double DoubleOrDefault(this IConfiguration configuration, double defaultValue, params string[] keys)
        => double.TryParse(configuration.FirstValue(keys), out var parsed) ? parsed : defaultValue;

    /// <summary>
    /// Bandera booleana con semántica "activada salvo que se indique <c>false</c> explícitamente".
    /// </summary>
    public static bool FlagEnabledByDefault(this IConfiguration configuration, params string[] keys)
        => !bool.TryParse(configuration.FirstValue(keys), out var parsed) || parsed;
}
