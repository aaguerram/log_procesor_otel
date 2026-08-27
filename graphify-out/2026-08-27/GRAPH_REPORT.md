# Graph Report - demo_kafka  (2026-08-27)

## Corpus Check
- 99 files · ~81,052 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 773 nodes · 1215 edges · 49 communities (46 shown, 3 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 70 edges (avg confidence: 0.84)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `9b127be0`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- KafkaStreamConsumerAdapter
- KafkaMessage
- ProcessedTransactionEvent
- LogDocument
- TopicInfo
- .ExecutePipelineAsync
- app.js
- KafkaDemo.Infrastructure.csproj
- ConsumerStreams.Infrastructure.csproj
- http
- KafkaStreamProducerAdapter
- LogSink.Infrastructure.Configuration
- LogSink.Infrastructure.csproj
- Revisión y aprobación del documento
- Lineamiento para nombrado de variables
- .PruneOuterTrace
- Lineamiento para nombrado y despliegue de imágenes
- RawTransactionEvent
- .StartBatchConsumerAsync
- BulkSinkWorkerService
- KafkaBatchConsumerAdapter
- SendMessageRequestDto
- .AddLogSinkInfrastructure
- CosmosDbBulkSinkAdapter
- ITransactionTransformerPort
- traces_agrupados/README.md
- SinkSettings
- .AddConsumerStreamsInfrastructure
- CompiledContractRules
- KafkaSettings
- KafkaDtos.cs
- ClusterHealthDto
- KafkaDemo.Domain.Ports
- SendMessagesUseCase.cs
- CreateTopicDto
- VaultKeyMaterial
- VaultKeyMaterial
- KafkaStreamSettings
- ConsumerStreams.Domain.Ports
- 🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)
- ConsumerStreams.Domain.Models
- DocumentDbEmulator.csproj
- StoredDocument

## God Nodes (most connected - your core abstractions)
1. `LogDocument` - 35 edges
2. `ProcessedTransactionEvent` - 30 edges
3. `RawTransactionEvent` - 26 edges
4. `CompiledContractRules` - 20 edges
5. `SinkSettings` - 18 edges
6. `DataProtectionRulesSettings` - 16 edges
7. `KafkaMessage` - 16 edges
8. `MessageResult` - 14 edges
9. `TopicInfo` - 14 edges
10. `VaultKeyMaterial` - 14 edges

## Surprising Connections (you probably didn't know these)
- `StreamProcessingPipelineUseCase` --references--> `DataProtectionRulesSettings`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Configuration/DataProtectionRulesSettings.cs
- `StreamProcessingPipelineUseCase` --references--> `IContractRulesCachePort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IContractRulesCachePort.cs
- `StreamProcessingPipelineUseCase` --references--> `ITransactionTransformerPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IStreamPorts.cs
- `StreamWorkerService` --references--> `StreamProcessingPipelineUseCase`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Worker/StreamWorkerService.cs → consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs
- `ThreadSafeContractRulesCacheAdapter` --implements--> `IContractRulesCachePort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Infrastructure/Adapters/ThreadSafeContractRulesCacheAdapter.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IContractRulesCachePort.cs

## Import Cycles
- None detected.

## Communities (49 total, 3 thin omitted)

### Community 0 - "KafkaStreamConsumerAdapter"
Cohesion: 0.22
Nodes (7): KafkaStreamConsumerAdapter, CancellationToken, Func, IConsumer, IDictionary, ILogger, Task

### Community 1 - "KafkaMessage"
Cohesion: 0.05
Nodes (46): IReadOnlyList, BatchSendResultDto, ElapsedMilliseconds, Results, TargetTopic, TotalRequested, TotalSent, CancellationToken (+38 more)

### Community 2 - "ProcessedTransactionEvent"
Cohesion: 0.08
Nodes (26): ProcessedTransactionEvent, Amount, AuditMetadata, Channel, Currency, DestinationAccount, FraudScore, Kind (+18 more)

### Community 3 - "LogDocument"
Cohesion: 0.06
Nodes (32): DateTime, Dictionary, LogDocument, Amount, AuditMetadata, Channel, Currency, DestinationAccount (+24 more)

### Community 5 - "TopicInfo"
Cohesion: 0.08
Nodes (32): CancellationToken, IReadOnlyList, Task, ManageTopicsUseCase, IDictionary, TopicCreationRequest, Configs, NumPartitions (+24 more)

### Community 6 - ".ExecutePipelineAsync"
Cohesion: 0.24
Nodes (7): CancellationToken, EncryptedPayloadEnvelope, Task, CancellationToken, Func, IDictionary, Task

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
Cohesion: 0.17
Nodes (9): IContractRulesCachePort, ActiveContractsCount, KafkaStreamProducerAdapter, CancellationToken, IDictionary, ILogger, IProducer, Task (+1 more)

### Community 12 - "LogSink.Infrastructure.Configuration"
Cohesion: 0.15
Nodes (11): LogSink.Infrastructure.Configuration, LogSink.Infrastructure.Adapters, LogSink.Application.UseCases, LogSink.Domain.Ports, LogSink.Domain.Models, LogSink.Infrastructure, LogSink.Worker, LogSink.Application.Serialization (+3 more)

### Community 13 - "LogSink.Infrastructure.csproj"
Cohesion: 0.14
Nodes (14): net10.0, Microsoft.Extensions.Logging.Abstractions (10.0.0), Microsoft.NET.Sdk, net10.0, Microsoft.NET.Sdk, net10.0, Confluent.Kafka (2.15.0), Microsoft.Extensions.Logging.Abstractions (10.0.0) (+6 more)

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
Cohesion: 0.19
Nodes (13): CancellationToken, ILogger, Task, TimeSpan, BulkSinkPipelineUseCase, CancellationToken, Func, IReadOnlyList (+5 more)

### Community 21 - "BulkSinkWorkerService"
Cohesion: 0.29
Nodes (6): BackgroundService, CancellationToken, ILogger, IOptions, Task, BulkSinkWorkerService

### Community 22 - "KafkaBatchConsumerAdapter"
Cohesion: 0.15
Nodes (12): ConsumeResult, Offset, IReadOnlyDictionary, KafkaBatchItem, CancellationToken, Func, IConsumer, ILogger (+4 more)

### Community 23 - "SendMessageRequestDto"
Cohesion: 0.29
Nodes (7): SendMessageRequestDto, Headers, Key, ServiceName, TelemetryType, Topic, Value

### Community 24 - ".AddLogSinkInfrastructure"
Cohesion: 0.15
Nodes (13): CachedCredentialsEntry, IVaultTokenProviderPort, CancellationToken, ConcurrentDictionary, DateTimeOffset, ILogger, Task, TimeSpan (+5 more)

### Community 25 - "CosmosDbBulkSinkAdapter"
Cohesion: 0.24
Nodes (11): HttpClient, BulkSinkResult, CosmosDbCredentials, CancellationToken, ILogger, IReadOnlyList, Task, CosmosDbBulkSinkAdapter (+3 more)

### Community 32 - "SinkSettings"
Cohesion: 0.15
Nodes (13): SinkSettings, BatchSize, BatchTimeoutMs, BootstrapServers, ContainerName, CosmosEndpoint, CosmosPrimaryKey, DatabaseName (+5 more)

### Community 33 - ".AddConsumerStreamsInfrastructure"
Cohesion: 0.27
Nodes (9): StreamProcessingPipelineUseCase, ILogger, IPayloadCryptoPort, IVaultTokenProviderPort, IStreamConsumerPort, IStreamProducerPort, AesGcmPayloadCryptoAdapter, IConfiguration (+1 more)

### Community 34 - "CompiledContractRules"
Cohesion: 0.06
Nodes (40): CachedContractEntry, DataProtectionRulesSettings, Enabled, Full, HashSha256, MaskUrlPathAndQuery, PartialLast4, Remove (+32 more)

### Community 36 - "KafkaSettings"
Cohesion: 0.18
Nodes (10): KafkaSettings, Acks, BootstrapServers, ClientId, EnableIdempotence, MessageSendMaxRetries, MessageTimeoutMs, RequestTimeoutMs (+2 more)

### Community 37 - "KafkaDtos.cs"
Cohesion: 0.32
Nodes (4): KafkaDemo.Application.DTOs, KafkaDemo.Application.UseCases, KafkaDemo.Infrastructure, SendBatchRequest

### Community 38 - "ClusterHealthDto"
Cohesion: 0.33
Nodes (6): DateTime, ClusterHealthDto, CheckedAt, IsConnected, Status, TotalTopics

### Community 39 - "KafkaDemo.Domain.Ports"
Cohesion: 0.29
Nodes (4): KafkaDemo.Domain.Ports, KafkaDemo.Infrastructure.Configuration, KafkaDemo.Infrastructure.Adapters, KafkaDemo.Domain.Models

### Community 40 - "SendMessagesUseCase.cs"
Cohesion: 0.40
Nodes (3): KafkaDemo.Domain.Configuration, KafkaDemo.Domain.Utils, UniformPartitionKeyGenerator

### Community 41 - "CreateTopicDto"
Cohesion: 0.33
Nodes (6): Dictionary, CreateTopicDto, Configs, Partitions, ReplicationFactor, TopicName

### Community 42 - "VaultKeyMaterial"
Cohesion: 0.09
Nodes (27): CancellationToken, EncryptedPayloadEnvelope, IDictionary, Task, TelemetryType, IPayloadCryptoPort, IVaultTokenProviderPort, VaultKeyMaterial (+19 more)

### Community 43 - "VaultKeyMaterial"
Cohesion: 0.10
Nodes (20): VaultKeyMaterial, AesKey256, CertThumbprint, KeyVersion, VaultTokenId, CancellationToken, EncryptedPayloadEnvelope, Task (+12 more)

### Community 46 - "KafkaStreamSettings"
Cohesion: 0.13
Nodes (13): KafkaStreamSettings, AutoOffsetReset, BootstrapServers, EnableAutoCommit, GroupId, PollTimeoutMs, SourceTopic, TargetTopic (+5 more)

### Community 48 - "ConsumerStreams.Domain.Ports"
Cohesion: 0.20
Nodes (8): DependencyInjection, ConsumerStreams.Application.UseCases, ConsumerStreams.Infrastructure.Adapters, ConsumerStreams.Infrastructure, ConsumerStreams.Infrastructure.Configuration, ConsumerStreams.Domain.Ports, ConsumerStreams.Application.Services, ConsumerStreams.Worker

### Community 52 - "🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)"
Cohesion: 0.18
Nodes (10): 🏛️ Arquitectura y Flujo de Procesamiento End-to-End, 🔑 Caché de Certificados y Credenciales en Memoria (TTL 1 Hora), 📈 Escalabilidad Horizontal (Multi-Réplicas), 📁 Estructura del Repositorio, ⚡ Generador de Claves de Partición de Alta Dispersión (SplitMix64), 💾 Micro-Batching y Bulk Sink hacia Azure Cosmos DB / DocumentDB, 🔒 Patrón Criptográfico: Self-Contained Encryption Envelope, 🐳 Portal de Accesos y Servicios (Docker Compose) (+2 more)

### Community 54 - "ConsumerStreams.Domain.Models"
Cohesion: 0.21
Nodes (5): StreamJsonContext, UniformPartitionKeyGenerator, ConsumerStreams.Application.Serialization, ConsumerStreams.Domain.Utils, ConsumerStreams.Domain.Models

### Community 60 - "DocumentDbEmulator.csproj"
Cohesion: 0.50
Nodes (3): net10.0, Microsoft.NET.Sdk.Web, MongoDB.Driver (2.28.0)

## Knowledge Gaps
- **291 isolated node(s):** `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk`, `Enabled`, `HashSha256` (+286 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `IContractRulesCachePort` connect `KafkaStreamProducerAdapter` to `.AddConsumerStreamsInfrastructure`, `CompiledContractRules`?**
  _High betweenness centrality (0.143) - this node is a cross-community bridge._
- **Why does `KafkaBatchConsumerAdapter` connect `KafkaBatchConsumerAdapter` to `SinkSettings`, `KafkaStreamProducerAdapter`, `LogSink.Infrastructure.Configuration`, `.StartBatchConsumerAsync`, `.AddLogSinkInfrastructure`?**
  _High betweenness centrality (0.131) - this node is a cross-community bridge._
- **Why does `KafkaAdminAdapter` connect `TopicInfo` to `KafkaMessage`, `KafkaStreamProducerAdapter`, `KafkaDemo.Domain.Ports`?**
  _High betweenness centrality (0.117) - this node is a cross-community bridge._
- **What connects `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk` to the rest of the system?**
  _291 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `KafkaMessage` be split into smaller, more focused modules?**
  _Cohesion score 0.050816696914700546 - nodes in this community are weakly interconnected._
- **Should `ProcessedTransactionEvent` be split into smaller, more focused modules?**
  _Cohesion score 0.07692307692307693 - nodes in this community are weakly interconnected._
- **Should `LogDocument` be split into smaller, more focused modules?**
  _Cohesion score 0.0625 - nodes in this community are weakly interconnected._