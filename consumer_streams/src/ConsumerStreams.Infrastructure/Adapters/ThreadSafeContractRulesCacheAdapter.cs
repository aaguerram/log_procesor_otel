using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using ConsumerStreams.Domain.Contracts;
using ConsumerStreams.Domain.Models;
using ConsumerStreams.Domain.Ports;
using ConsumerStreams.Infrastructure.Logging;
using Microsoft.Extensions.Logging;

namespace ConsumerStreams.Infrastructure.Adapters;

/// <summary>
/// Caché thread-safe de contratos OpenAPI compilados con TTL deslizante de 10 minutos.
/// La compilación se delega en <see cref="IContractCompiler"/> (inversión de dependencias) y el
/// tiempo proviene de <see cref="TimeProvider"/> para poder probar la expiración de forma determinista.
/// </summary>
public sealed class ThreadSafeContractRulesCacheAdapter : IContractRulesCachePort
{
    private static readonly TimeSpan SlidingTtl = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan EvictionInterval = TimeSpan.FromMinutes(1);

    private readonly ConcurrentDictionary<string, CachedContractEntry> _cache = new(StringComparer.Ordinal);
    private readonly IContractCompiler _compiler;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ThreadSafeContractRulesCacheAdapter> _logger;
    private readonly ITimer _evictionTimer;
    private bool _disposed;

    public ThreadSafeContractRulesCacheAdapter(
        IContractCompiler compiler,
        TimeProvider timeProvider,
        ILogger<ThreadSafeContractRulesCacheAdapter> logger)
    {
        _compiler = compiler;
        _timeProvider = timeProvider;
        _logger = logger;
        _evictionTimer = timeProvider.CreateTimer(_ => EvictExpiredContracts(), null, EvictionInterval, EvictionInterval);
    }

    public int ActiveContractsCount => _cache.Count;

    public CompiledContractRules GetOrCompile(string swaggerYaml)
    {
        if (string.IsNullOrWhiteSpace(swaggerYaml))
        {
            return _compiler.Compile(string.Empty);
        }

        var contractKey = ComputeFingerprint(swaggerYaml);
        var entry = _cache.GetOrAdd(contractKey, key =>
        {
            InfrastructureLog.ContractCompiling(_logger, key);
            return new CachedContractEntry(_compiler.Compile(swaggerYaml), _timeProvider.GetUtcNow());
        });

        entry.Touch(_timeProvider.GetUtcNow());
        return entry.Rules;
    }

    private void EvictExpiredContracts()
    {
        if (_disposed)
        {
            return;
        }

        var now = _timeProvider.GetUtcNow();
        var evicted = 0;

        foreach (var (key, entry) in _cache)
        {
            if (now - entry.LastAccessedUtc >= SlidingTtl && _cache.TryRemove(key, out var removed))
            {
                evicted++;
                InfrastructureLog.ContractEvicted(_logger, removed.Rules.ServiceName, removed.Rules.Version);
            }
        }

        if (evicted > 0)
        {
            InfrastructureLog.ContractCacheCleanup(_logger, evicted, _cache.Count);
        }
    }

    private static string ComputeFingerprint(string text)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(text));
        return Convert.ToHexStringLower(hash.AsSpan(0, 16));
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _evictionTimer.Dispose();
        _cache.Clear();
    }

    private sealed class CachedContractEntry(CompiledContractRules rules, DateTimeOffset createdUtc)
    {
        private long _lastAccessedUtcTicks = createdUtc.UtcTicks;

        public CompiledContractRules Rules { get; } = rules;

        public DateTimeOffset LastAccessedUtc => new(Interlocked.Read(ref _lastAccessedUtcTicks), TimeSpan.Zero);

        public void Touch(DateTimeOffset now) => Interlocked.Exchange(ref _lastAccessedUtcTicks, now.UtcTicks);
    }
}
