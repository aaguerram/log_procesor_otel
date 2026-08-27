namespace KafkaDemo.Domain.Configuration;

/// <summary>
/// Opciones de configuración para el podado y optimización de trazas OpenTelemetry.
/// </summary>
public sealed class TracePruningSettings
{
    /// <summary>
    /// Habilita o deshabilita el podado de listas en trazas GET.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Número máximo de elementos a preservar por cada lista/arreglo JSON. Por defecto 10.
    /// </summary>
    public int MaxArrayItems { get; set; } = 10;

    /// <summary>
    /// Profundidad máxima de inspección jerárquica para podado de listas. Por defecto 5.
    /// </summary>
    public int MaxDepth { get; set; } = 5;
}
