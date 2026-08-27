namespace ConsumerStreams.Domain.Observability;

/// <summary>
/// Compone el nombre de la colección Cosmos DB destino: <c>{servicio con puntos a guiones bajos}_{tipo}</c>.
/// Es la contraparte productora de la resolución que hace <c>log_sink</c> al consumir.
/// </summary>
public static class TargetCollectionResolver
{
    public static string Resolve(string serviceName, string telemetryLabel)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceName);
        ArgumentException.ThrowIfNullOrWhiteSpace(telemetryLabel);

        return $"{serviceName.Replace('.', '_')}_{telemetryLabel}";
    }
}
