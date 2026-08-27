using Microsoft.Extensions.Configuration;

namespace ConsumerStreams.Infrastructure.Configuration;

/// <summary>
/// Lectura tipada y tolerante de <see cref="IConfiguration"/>: primera clave no vacía entre las
/// alternativas jerárquicas (<c>Seccion:Clave</c>) y planas (<c>TECH-INT-...</c> / <c>TECH_INT_...</c>).
/// Concentra el patrón de resolución antes duplicado en <see cref="DependencyInjection"/>.
/// </summary>
public static class ConfigReader
{
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

    public static string Required(this IConfiguration configuration, string friendlyName, params string[] keys)
        => configuration.FirstValue(keys)
           ?? throw new InvalidOperationException(
               $"[CONFIG ERROR] '{friendlyName}' no está configurado en appsettings.json ni en las variables de entorno.");

    public static string ValueOrDefault(this IConfiguration configuration, string defaultValue, params string[] keys)
        => configuration.FirstValue(keys) ?? defaultValue;

    public static int IntOrDefault(this IConfiguration configuration, int defaultValue, params string[] keys)
        => int.TryParse(configuration.FirstValue(keys), out var parsed) ? parsed : defaultValue;

    public static bool BoolOrDefault(this IConfiguration configuration, bool defaultValue, params string[] keys)
        => bool.TryParse(configuration.FirstValue(keys), out var parsed) ? parsed : defaultValue;

    /// <summary>Bandera "activada salvo <c>false</c> explícito".</summary>
    public static bool FlagEnabledByDefault(this IConfiguration configuration, params string[] keys)
        => !bool.TryParse(configuration.FirstValue(keys), out var parsed) || parsed;
}
