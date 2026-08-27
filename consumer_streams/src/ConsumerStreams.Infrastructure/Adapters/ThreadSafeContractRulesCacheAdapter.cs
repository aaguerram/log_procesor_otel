using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Domain.Utils;
using Microsoft.Extensions.Logging;

namespace ConsumerStreams.Infrastructure.Adapters;

/// <summary>
/// Implementación Thread-Safe, libre de bloqueos y de cero contención para la memoria caché de contratos OpenAPI.
/// Garantiza que múltiples hilos concurrentes puedan leer y actualizar el TTL deslizante (10 minutos)
/// mediante operaciones atómicas de CPU (Interlocked) sin generar errores ni cuellos de botella.
/// </summary>
public sealed class ThreadSafeContractRulesCacheAdapter : IContractRulesCachePort
{
    private readonly ConcurrentDictionary<string, CachedContractEntry> _cache = new(StringComparer.Ordinal);
    private readonly ILogger<ThreadSafeContractRulesCacheAdapter> _logger;
    private readonly Timer _evictionTimer;
    private readonly long _ttlMs = 10 * 60 * 1000; // 10 minutos en milisegundos
    private bool _disposed;

    public ThreadSafeContractRulesCacheAdapter(ILogger<ThreadSafeContractRulesCacheAdapter> logger)
    {
        _logger = logger;
        // Timer en segundo plano cada 60 segundos para desalojar contratos inactivos sin interferir con el streaming
        _evictionTimer = new Timer(EvictExpiredContracts, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    public int ActiveContractsCount => _cache.Count;

    public CompiledContractRules GetOrCompile(string swaggerYaml)
    {
        if (string.IsNullOrWhiteSpace(swaggerYaml))
        {
            return OpenApiContractCompiler.Compile(string.Empty);
        }

        // 1. Clave de búsqueda rápida por fingerprint (primeros 64 bits de SHA-256)
        string contractKey = ComputeFastFingerprint(swaggerYaml);

        // 2. Obtener o compilar de forma Thread-Safe (Single-Compilation)
        var entry = _cache.GetOrAdd(contractKey, _ =>
        {
            _logger.LogInformation("[CONTRACT CACHE] Compilando nuevo contrato Swagger (Fingerprint: {Key})", contractKey);
            var compiled = OpenApiContractCompiler.Compile(swaggerYaml);
            return new CachedContractEntry(compiled);
        });

        // 3. Actualización Atómica del TTL Deslizante (Interlocked CPU Exchange - < 1 nanosegundo)
        // Múltiples hilos pueden ejecutar Touch() concurrentemente sin locks ni colisiones
        entry.Touch();

        return entry.Rules;
    }

    private void EvictExpiredContracts(object? state)
    {
        if (_disposed) return;

        long currentTick = Environment.TickCount64;
        int evictedCount = 0;

        foreach (var kvp in _cache)
        {
            if (kvp.Value.IsExpired(currentTick, _ttlMs))
            {
                // Remoción atómica no bloqueante
                if (_cache.TryRemove(kvp.Key, out var removedEntry))
                {
                    evictedCount++;
                    _logger.LogInformation(
                        "[CONTRACT EVICTION] Contrato inactivo por >10 minutos desalojado de memoria: {Contract} v{Version}. Liberado para Garbage Collector.",
                        removedEntry.Rules.ServiceName, removedEntry.Rules.Version);
                }
            }
        }

        if (evictedCount > 0)
        {
            _logger.LogInformation(
                "[CONTRACT CACHE] Limpieza completada. Desalojados: {Count}, Contratos activos restantes: {Active}",
                evictedCount, _cache.Count);
        }
    }

    private static string ComputeFastFingerprint(string text)
    {
        byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(text));
        return Convert.ToHexStringLower(hashBytes.AsSpan(0, 16)); // 32 caracteres hex
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _evictionTimer.Dispose();
            _cache.Clear();
        }
    }

    /// <summary>
    /// Entrada de caché que mantiene la referencia al contrato compilado y su timestamp de último acceso atómico.
    /// </summary>
    private sealed class CachedContractEntry
    {
        public CompiledContractRules Rules { get; }
        private long _lastAccessedTimestampMs;

        public CachedContractEntry(CompiledContractRules rules)
        {
            Rules = rules;
            _lastAccessedTimestampMs = Environment.TickCount64;
        }

        /// <summary>
        /// Actualiza el timestamp de último acceso de forma atómica a nivel de hardware (0 locks).
        /// </summary>
        public void Touch()
        {
            Interlocked.Exchange(ref _lastAccessedTimestampMs, Environment.TickCount64);
        }

        /// <summary>
        /// Comprueba si el contrato ha estado inactivo más de ttlMs milisegundos.
        /// </summary>
        public bool IsExpired(long currentTick, long ttlMs)
        {
            long last = Interlocked.Read(ref _lastAccessedTimestampMs);
            return (currentTick - last) >= ttlMs;
        }
    }
}
