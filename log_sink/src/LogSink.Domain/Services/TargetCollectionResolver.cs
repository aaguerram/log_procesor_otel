namespace LogSink.Domain.Services;

/// <summary>
/// Determina la colección de Cosmos DB destino de un evento a partir de sus cabeceras.
/// Regla: se prioriza <c>x-target-collection</c>; en su defecto se compone
/// <c>{x-service-name con puntos a guiones bajos}_{x-telemetry-type}</c>.
/// Lógica pura y sin estado, extraída del caso de uso para poder probarla en aislamiento.
/// </summary>
public static class TargetCollectionResolver
{
    /// <returns>El nombre de colección resuelto, o <c>null</c> si las cabeceras no permiten deducirlo.</returns>
    public static string? Resolve(IReadOnlyDictionary<string, string> headers)
    {
        ArgumentNullException.ThrowIfNull(headers);

        if (headers.TryGetValue(ObservabilityHeaders.TargetCollection, out var explicitCollection)
            && !string.IsNullOrWhiteSpace(explicitCollection))
        {
            return explicitCollection;
        }

        if (headers.TryGetValue(ObservabilityHeaders.ServiceName, out var serviceName)
            && !string.IsNullOrWhiteSpace(serviceName)
            && headers.TryGetValue(ObservabilityHeaders.TelemetryType, out var telemetryType)
            && !string.IsNullOrWhiteSpace(telemetryType))
        {
            return $"{Sanitize(serviceName)}_{telemetryType}";
        }

        return null;
    }

    private static string Sanitize(string serviceName) => serviceName.Replace('.', '_');
}
