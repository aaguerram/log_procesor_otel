namespace ConsumerStreams.Domain.Configuration;

/// <summary>
/// Configuración de reglas de protección y gobierno de datos x-log-data-protection en ConsumerStreams.
/// </summary>
public sealed class DataProtectionRulesSettings
{
    /// <summary>
    /// Activa o desactiva de forma global el motor de protección de datos en logs.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Habilita la regla @Log.Hash(SHA256) para anonimizar identificadores personales (idClient, idCuenta, identificacion).
    /// </summary>
    public bool HashSha256 { get; set; } = true;

    /// <summary>
    /// Habilita la regla @Log.Partial(LAST_4) para enmascarar cuentas y tarjetas preservando solo los últimos 4 dígitos.
    /// </summary>
    public bool PartialLast4 { get; set; } = true;

    /// <summary>
    /// Habilita la regla @Log.Remove para excluir/suprimir completamente campos sensibles o pesados (ej. ObjJsonResponse).
    /// </summary>
    public bool Remove { get; set; } = true;

    /// <summary>
    /// Habilita la regla @Log.Full para registrar campos en claro sin modificaciones (montos, estados, descripciones).
    /// </summary>
    public bool Full { get; set; } = true;
}
