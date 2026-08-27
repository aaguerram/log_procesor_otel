# Graph Report - demo_kafka  (2026-08-27)

## Corpus Check
- 100 files · ~82,328 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 809 nodes · 1281 edges · 51 communities (47 shown, 4 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 76 edges (avg confidence: 0.84)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `645095a2`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- .ExecutePipelineAsync
- KafkaMessage
- ProcessedTransactionEvent
- LogDocument
- TopicInfo
- .StartStreamingAsync
- app.js
- KafkaDemo.Infrastructure.csproj
- ConsumerStreams.Infrastructure.csproj
- http
- KafkaStreamProducerAdapter
- CosmosDbBulkSinkAdapter.cs
- LogSink.Infrastructure.csproj
- Revisión y aprobación del documento
- Lineamiento para nombrado de variables
- .PruneOuterTrace
- Lineamiento para nombrado y despliegue de imágenes
- RawTransactionEvent
- .StartBatchConsumerAsync
- SendMessagesUseCase.cs
- KafkaBatchConsumerAdapter
- BatchSendResultDto
- AzureKeyVaultTokenAdapter
- CosmosDbBulkSinkAdapter
- ITransactionTransformerPort
- traces_agrupados/README.md
- SinkSettings
- .AddConsumerStreamsInfrastructure
- CompiledContractRules
- KafkaProducerAdapter
- KafkaSettings
- MessageResult
- KafkaDlqProducerAdapter
- KafkaDemo.Domain.Ports
- .AddLogSinkInfrastructure
- LogSink.Infrastructure.Configuration
- .AddKafkaInfrastructure
- VaultKeyMaterial
- BulkSinkWorkerService.cs
- KafkaStreamSettings
- ConsumerStreams.Domain.Ports
- 🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)
- ConsumerStreams.Domain.Models
- DocumentDbEmulator.csproj
- StoredDocument

## God Nodes (most connected - your core abstractions)
1. `LogDocument` - 34 edges
2. `ProcessedTransactionEvent` - 30 edges
3. `RawTransactionEvent` - 26 edges
4. `SinkSettings` - 22 edges
5. `CompiledContractRules` - 20 edges
6. `DataProtectionRulesSettings` - 16 edges
7. `KafkaMessage` - 16 edges
8. `CosmosDbBulkSinkAdapter` - 15 edges
9. `StreamProcessingPipelineUseCase` - 14 edges
10. `MessageResult` - 14 edges

## Surprising Connections (you probably didn't know these)
- `StreamProcessingPipelineUseCase` --references--> `DataProtectionRulesSettings`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Configuration/DataProtectionRulesSettings.cs
- `StreamProcessingPipelineUseCase` --references--> `IPayloadCryptoPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `StreamProcessingPipelineUseCase` --references--> `IVaultTokenProviderPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `StreamProcessingPipelineUseCase` --references--> `ITransactionTransformerPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IStreamPorts.cs
- `StreamWorkerService` --references--> `StreamProcessingPipelineUseCase`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Worker/StreamWorkerService.cs → consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs

## Import Cycles
- None detected.

## Communities (51 total, 4 thin omitted)

### Community 0 - ".ExecutePipelineAsync"
Cohesion: 0.24
Nodes (6): CancellationToken, EncryptedPayloadEnvelope, Exception, IDictionary, Task, UniformPartitionKeyGenerator

### Community 1 - "KafkaMessage"
Cohesion: 0.20
Nodes (10): DateTime, IDictionary, KafkaMessage, BinaryValue, Headers, IsBinary, Key, Timestamp (+2 more)

### Community 2 - "ProcessedTransactionEvent"
Cohesion: 0.08
Nodes (26): ProcessedTransactionEvent, Amount, AuditMetadata, Channel, Currency, DestinationAccount, FraudScore, Kind (+18 more)

### Community 3 - "LogDocument"
Cohesion: 0.06
Nodes (32): DateTime, Dictionary, LogDocument, Amount, AuditMetadata, Channel, Currency, DestinationAccount (+24 more)

### Community 5 - "TopicInfo"
Cohesion: 0.08
Nodes (32): CancellationToken, IReadOnlyList, Task, ManageTopicsUseCase, IDictionary, TopicCreationRequest, Configs, NumPartitions (+24 more)

### Community 6 - ".StartStreamingAsync"
Cohesion: 0.23
Nodes (8): CancellationToken, Func, IDictionary, Task, CancellationToken, Func, IDictionary, Task

### Community 7 - "app.js"
Cohesion: 0.16
Nodes (26): appendBatchToStream(), appendSingleToStream(), checkHealth(), dom, escapeHtml(), fetchTopics(), formatTextareaJson(), generateDispersedKey() (+18 more)

### Community 8 - "KafkaDemo.Infrastructure.csproj"
Cohesion: 0.11
Nodes (19): net10.0, Microsoft.NET.Sdk, net10.0, Microsoft.Extensions.Hosting (10.0.11), Microsoft.NET.Sdk, net10.0, Google.Protobuf (3.36.0), Grpc.Tools (2.83.0) (+11 more)

### Community 9 - "ConsumerStreams.Infrastructure.csproj"
Cohesion: 0.11
Nodes (18): net10.0, Microsoft.Extensions.Logging.Abstractions (10.0.11), Microsoft.NET.Sdk, net10.0, Google.Protobuf (3.36.0), Grpc.Tools (2.83.0), Microsoft.NET.Sdk, net10.0 (+10 more)

### Community 10 - "http"
Cohesion: 0.13
Nodes (15): ASPNETCORE_ENVIRONMENT, applicationUrl, commandName, dotnetRunMessages, environmentVariables, launchBrowser, applicationUrl, commandName (+7 more)

### Community 11 - "KafkaStreamProducerAdapter"
Cohesion: 0.31
Nodes (6): KafkaStreamProducerAdapter, CancellationToken, IDictionary, ILogger, IProducer, Task

### Community 12 - "CosmosDbBulkSinkAdapter.cs"
Cohesion: 0.28
Nodes (5): LogSink.Application.UseCases, LogSink.Domain.Models, LogSink.Application.Serialization, JsonSerializerContext, SinkJsonContext

### Community 13 - "LogSink.Infrastructure.csproj"
Cohesion: 0.13
Nodes (15): net10.0, Microsoft.Extensions.Logging.Abstractions (10.0.0), Microsoft.NET.Sdk, net10.0, Microsoft.NET.Sdk, net10.0, Confluent.Kafka (2.15.0), Microsoft.Extensions.Logging.Abstractions (10.0.0) (+7 more)

### Community 14 - "Revisión y aprobación del documento"
Cohesion: 0.10
Nodes (20): 1. Formatos de datos (payload), 1. Procesamiento de eventos de negocio, 2. Arquitectura basada en eventos (EDA), 3. Persistencia y relectura de mensajes., 4. Fan-out (broadcast), 5. Datos y analítica, Alcance, Casos de Uso (+12 more)

### Community 15 - "Lineamiento para nombrado de variables"
Cohesion: 0.11
Nodes (18): 1. Consideraciones generales, 2.1 Tipo(Type), 2.2 Alcance(Scope), 2.3 Fuente(Source), 2.4 Recurso, 2.5 Atributo, 2.6 Ejemplo de lineamiento, 2. Convención (+10 more)

### Community 16 - ".PruneOuterTrace"
Cohesion: 0.44
Nodes (5): MethodImpl, ReadOnlySpan, Utf8JsonReader, Utf8JsonWriter, OTelTracePruner

### Community 17 - "Lineamiento para nombrado y despliegue de imágenes"
Cohesion: 0.18
Nodes (10): Checklist de aceptación, **Consideraciones**, Consideraciones, **<element-name\>**, Lineamiento para nombrado y despliegue de imágenes, Propósito, Revisión y aprobación del documento, Ruta de Acceso (+2 more)

### Community 19 - "RawTransactionEvent"
Cohesion: 0.08
Nodes (24): RawTransactionEvent, Amount, Channel, Currency, DestinationAccount, DurationMs, EmittedAt, EventId (+16 more)

### Community 20 - ".StartBatchConsumerAsync"
Cohesion: 0.33
Nodes (7): BulkSinkResult, CancellationToken, Func, IReadOnlyList, Task, TimeSpan, IDocumentDbBulkSinkPort

### Community 21 - "SendMessagesUseCase.cs"
Cohesion: 0.21
Nodes (7): KafkaDemo.Application.DTOs, KafkaDemo.Domain.Configuration, KafkaDemo.Application.UseCases, KafkaDemo.Infrastructure, KafkaDemo.Domain.Utils, UniformPartitionKeyGenerator, SendBatchRequest

### Community 22 - "KafkaBatchConsumerAdapter"
Cohesion: 0.15
Nodes (12): ConsumeResult, Offset, IReadOnlyDictionary, KafkaBatchItem, CancellationToken, Func, IConsumer, ILogger (+4 more)

### Community 23 - "BatchSendResultDto"
Cohesion: 0.08
Nodes (26): DateTime, Dictionary, IReadOnlyList, BatchSendResultDto, ElapsedMilliseconds, Results, TargetTopic, TotalRequested (+18 more)

### Community 24 - "AzureKeyVaultTokenAdapter"
Cohesion: 0.18
Nodes (12): CachedCredentialsEntry, CosmosDbCredentials, IVaultTokenProviderPort, CancellationToken, ConcurrentDictionary, DateTimeOffset, ILogger, Task (+4 more)

### Community 25 - "CosmosDbBulkSinkAdapter"
Cohesion: 0.23
Nodes (12): Exception, HttpStatusCode, LogSinkItem, CancellationToken, ILogger, IReadOnlyList, Task, CosmosDbBulkSinkAdapter (+4 more)

### Community 32 - "SinkSettings"
Cohesion: 0.06
Nodes (33): BackgroundService, CircuitBreakerSettings, BreakDurationSeconds, FailureRatio, MinimumThroughput, SamplingDurationSeconds, ResilienceSettings, CircuitBreaker (+25 more)

### Community 33 - ".AddConsumerStreamsInfrastructure"
Cohesion: 0.16
Nodes (13): StreamProcessingPipelineUseCase, ILogger, IContractRulesCachePort, ActiveContractsCount, IStreamConsumerPort, IStreamProducerPort, KafkaStreamConsumerAdapter, IConsumer (+5 more)

### Community 34 - "CompiledContractRules"
Cohesion: 0.06
Nodes (39): CachedContractEntry, DataProtectionRulesSettings, Enabled, Full, HashSha256, MaskUrlPathAndQuery, PartialLast4, Remove (+31 more)

### Community 35 - "KafkaProducerAdapter"
Cohesion: 0.27
Nodes (7): CancellationToken, IEnumerable, ILogger, IProducer, IReadOnlyList, Task, KafkaProducerAdapter

### Community 36 - "KafkaSettings"
Cohesion: 0.18
Nodes (10): KafkaSettings, Acks, BootstrapServers, ClientId, EnableIdempotence, MessageSendMaxRetries, MessageTimeoutMs, RequestTimeoutMs (+2 more)

### Community 37 - "MessageResult"
Cohesion: 0.16
Nodes (12): DateTime, MessageResult, Key, Partition, Status, Timestamp, Topic, CancellationToken (+4 more)

### Community 38 - "KafkaDlqProducerAdapter"
Cohesion: 0.20
Nodes (8): IDictionary, IDlqProducerPort, CancellationToken, IDictionary, ILogger, IProducer, Task, KafkaDlqProducerAdapter

### Community 39 - "KafkaDemo.Domain.Ports"
Cohesion: 0.29
Nodes (4): KafkaDemo.Domain.Ports, KafkaDemo.Infrastructure.Configuration, KafkaDemo.Infrastructure.Adapters, KafkaDemo.Domain.Models

### Community 40 - ".AddLogSinkInfrastructure"
Cohesion: 0.24
Nodes (8): CancellationToken, ILogger, Task, TimeSpan, BulkSinkPipelineUseCase, IBatchConsumerPort, IConfiguration, IServiceCollection

### Community 41 - "LogSink.Infrastructure.Configuration"
Cohesion: 0.46
Nodes (4): LogSink.Infrastructure.Configuration, LogSink.Infrastructure.Adapters, LogSink.Domain.Ports, DependencyInjection

### Community 42 - ".AddKafkaInfrastructure"
Cohesion: 0.06
Nodes (38): CancellationToken, Task, SendMessagesUseCase, TracePruningSettings, Enabled, MaxArrayItems, MaxDepth, CancellationToken (+30 more)

### Community 43 - "VaultKeyMaterial"
Cohesion: 0.09
Nodes (23): IPayloadCryptoPort, IVaultTokenProviderPort, VaultKeyMaterial, AesKey256, CertThumbprint, KeyVersion, VaultTokenId, CancellationToken (+15 more)

### Community 46 - "KafkaStreamSettings"
Cohesion: 0.12
Nodes (14): KafkaStreamSettings, AutoOffsetReset, BootstrapServers, EnableAutoCommit, ErrorTopic, GroupId, PollTimeoutMs, SourceTopic (+6 more)

### Community 48 - "ConsumerStreams.Domain.Ports"
Cohesion: 0.22
Nodes (7): ConsumerStreams.Application.UseCases, ConsumerStreams.Infrastructure.Adapters, ConsumerStreams.Infrastructure, ConsumerStreams.Infrastructure.Configuration, ConsumerStreams.Domain.Ports, ConsumerStreams.Application.Services, ConsumerStreams.Worker

### Community 52 - "🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)"
Cohesion: 0.18
Nodes (10): 🏛️ Arquitectura y Flujo de Procesamiento End-to-End, 🔑 Caché de Certificados y Credenciales en Memoria (TTL 1 Hora), 📈 Escalabilidad Horizontal (Multi-Réplicas), 📁 Estructura del Repositorio, ⚡ Generador de Claves de Partición de Alta Dispersión (SplitMix64), 💾 Micro-Batching y Bulk Sink hacia Azure Cosmos DB / DocumentDB, 🔒 Patrón Criptográfico: Self-Contained Encryption Envelope, 🐳 Portal de Accesos y Servicios (Docker Compose) (+2 more)

### Community 54 - "ConsumerStreams.Domain.Models"
Cohesion: 0.23
Nodes (5): StreamJsonContext, ConsumerStreams.Application.Serialization, ConsumerStreams.Domain.Configuration, ConsumerStreams.Domain.Utils, ConsumerStreams.Domain.Models

### Community 60 - "DocumentDbEmulator.csproj"
Cohesion: 0.50
Nodes (3): net10.0, Microsoft.NET.Sdk.Web, MongoDB.Driver (2.28.0)

## Knowledge Gaps
- **305 isolated node(s):** `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk`, `Enabled`, `HashSha256` (+300 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **4 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `IContractRulesCachePort` connect `.AddConsumerStreamsInfrastructure` to `CompiledContractRules`?**
  _High betweenness centrality (0.146) - this node is a cross-community bridge._
- **Why does `KafkaAdminAdapter` connect `TopicInfo` to `.AddConsumerStreamsInfrastructure`, `.AddKafkaInfrastructure`, `KafkaDemo.Domain.Ports`?**
  _High betweenness centrality (0.116) - this node is a cross-community bridge._
- **Why does `KafkaBatchConsumerAdapter` connect `KafkaBatchConsumerAdapter` to `.AddLogSinkInfrastructure`, `LogSink.Infrastructure.Configuration`, `SinkSettings`, `.AddConsumerStreamsInfrastructure`?**
  _High betweenness centrality (0.101) - this node is a cross-community bridge._
- **What connects `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk` to the rest of the system?**
  _305 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `ProcessedTransactionEvent` be split into smaller, more focused modules?**
  _Cohesion score 0.07692307692307693 - nodes in this community are weakly interconnected._
- **Should `LogDocument` be split into smaller, more focused modules?**
  _Cohesion score 0.0625 - nodes in this community are weakly interconnected._
- **Should `TopicInfo` be split into smaller, more focused modules?**
  _Cohesion score 0.07755102040816327 - nodes in this community are weakly interconnected._