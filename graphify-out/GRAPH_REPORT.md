# Graph Report - demo_kafka  (2026-08-26)

## Corpus Check
- 99 files · ~79,057 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 755 nodes · 1176 edges · 49 communities (46 shown, 3 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 66 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `c5792058`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- .ExecutePipelineAsync
- KafkaMessage
- ProcessedTransactionEvent
- LogDocument
- TopicInfo
- .AddConsumerStreamsInfrastructure
- app.js
- KafkaDemo.Infrastructure.csproj
- ConsumerStreams.Infrastructure.csproj
- http
- KafkaDtos.cs
- ISinkPorts.cs
- LogSink.Infrastructure.csproj
- Revisión y aprobación del documento
- Lineamiento para nombrado de variables
- .PruneOuterTrace
- Lineamiento para nombrado y despliegue de imágenes
- RawTransactionEvent
- VaultKeyMaterial
- .AddLogSinkInfrastructure
- KafkaBatchConsumerAdapter
- BulkSinkWorkerService
- AzureKeyVaultTokenAdapter
- CosmosDbBulkSinkAdapter
- traces_agrupados/README.md
- SinkSettings
- KafkaStreamConsumerAdapter
- CompiledContractRules
- CreateTopicDto
- SendMessageRequestDto
- KafkaSettings
- KafkaDemo.Domain.Ports
- SendMessagesUseCase.cs
- ClusterHealthDto
- .AddKafkaInfrastructure
- AzureKeyVaultTokenAdapter
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
4. `SinkSettings` - 18 edges
5. `CompiledContractRules` - 16 edges
6. `KafkaMessage` - 16 edges
7. `MessageResult` - 14 edges
8. `TopicInfo` - 14 edges
9. `VaultKeyMaterial` - 14 edges
10. `ThreadSafeContractRulesCacheAdapter` - 13 edges

## Surprising Connections (you probably didn't know these)
- `TransactionEnricher` --implements--> `ITransactionTransformerPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/Services/TransactionEnricher.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IStreamPorts.cs
- `StreamProcessingPipelineUseCase` --references--> `DataProtectionRulesSettings`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Configuration/DataProtectionRulesSettings.cs
- `StreamProcessingPipelineUseCase` --references--> `IPayloadCryptoPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `StreamProcessingPipelineUseCase` --references--> `IVaultTokenProviderPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `StreamWorkerService` --references--> `StreamProcessingPipelineUseCase`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Worker/StreamWorkerService.cs → consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs

## Import Cycles
- None detected.

## Communities (49 total, 3 thin omitted)

### Community 0 - ".ExecutePipelineAsync"
Cohesion: 0.19
Nodes (9): CancellationToken, Task, CancellationToken, Func, IDictionary, Task, CancellationToken, IDictionary (+1 more)

### Community 1 - "KafkaMessage"
Cohesion: 0.06
Nodes (36): IReadOnlyList, BatchSendResultDto, ElapsedMilliseconds, Results, TargetTopic, TotalRequested, TotalSent, DateTime (+28 more)

### Community 2 - "ProcessedTransactionEvent"
Cohesion: 0.08
Nodes (26): ProcessedTransactionEvent, Amount, AuditMetadata, Channel, Currency, DestinationAccount, FraudScore, Kind (+18 more)

### Community 3 - "LogDocument"
Cohesion: 0.06
Nodes (32): DateTime, Dictionary, LogDocument, Amount, AuditMetadata, Channel, Currency, DestinationAccount (+24 more)

### Community 5 - "TopicInfo"
Cohesion: 0.08
Nodes (32): CancellationToken, IReadOnlyList, Task, ManageTopicsUseCase, IDictionary, TopicCreationRequest, Configs, NumPartitions (+24 more)

### Community 6 - ".AddConsumerStreamsInfrastructure"
Cohesion: 0.18
Nodes (14): StreamProcessingPipelineUseCase, ILogger, IContractRulesCachePort, ActiveContractsCount, IStreamConsumerPort, IStreamProducerPort, ITransactionTransformerPort, KafkaStreamProducerAdapter (+6 more)

### Community 7 - "app.js"
Cohesion: 0.15
Nodes (27): appendBatchToStream(), appendSingleToStream(), checkHealth(), dom, escapeHtml(), fetchTopics(), formatTextareaJson(), generateDispersedKey() (+19 more)

### Community 8 - "KafkaDemo.Infrastructure.csproj"
Cohesion: 0.11
Nodes (19): net10.0, Microsoft.NET.Sdk, net10.0, Microsoft.Extensions.Hosting (10.0.11), Microsoft.NET.Sdk, net10.0, Google.Protobuf (3.36.0), Grpc.Tools (2.83.0) (+11 more)

### Community 9 - "ConsumerStreams.Infrastructure.csproj"
Cohesion: 0.11
Nodes (18): net10.0, Microsoft.Extensions.Logging.Abstractions (10.0.11), Microsoft.NET.Sdk, net10.0, Google.Protobuf (3.36.0), Grpc.Tools (2.83.0), Microsoft.NET.Sdk, net10.0 (+10 more)

### Community 10 - "http"
Cohesion: 0.13
Nodes (15): ASPNETCORE_ENVIRONMENT, applicationUrl, commandName, dotnetRunMessages, environmentVariables, launchBrowser, applicationUrl, commandName (+7 more)

### Community 11 - "KafkaDtos.cs"
Cohesion: 0.32
Nodes (4): KafkaDemo.Application.DTOs, KafkaDemo.Application.UseCases, KafkaDemo.Infrastructure, SendBatchRequest

### Community 12 - "ISinkPorts.cs"
Cohesion: 0.17
Nodes (9): LogSink.Infrastructure.Configuration, LogSink.Infrastructure.Adapters, LogSink.Application.UseCases, LogSink.Domain.Ports, LogSink.Domain.Models, LogSink.Infrastructure, LogSink.Worker, LogSink.Application.Serialization (+1 more)

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

### Community 20 - "VaultKeyMaterial"
Cohesion: 0.21
Nodes (9): IPayloadCryptoPort, VaultKeyMaterial, AesKey256, CertThumbprint, KeyVersion, VaultTokenId, EncryptedPayloadEnvelope, AesGcmPayloadCryptoAdapter (+1 more)

### Community 21 - ".AddLogSinkInfrastructure"
Cohesion: 0.16
Nodes (16): CancellationToken, ILogger, Task, TimeSpan, BulkSinkPipelineUseCase, BulkSinkResult, CancellationToken, Func (+8 more)

### Community 22 - "KafkaBatchConsumerAdapter"
Cohesion: 0.15
Nodes (12): ConsumeResult, Offset, IReadOnlyDictionary, KafkaBatchItem, CancellationToken, Func, IConsumer, ILogger (+4 more)

### Community 23 - "BulkSinkWorkerService"
Cohesion: 0.29
Nodes (6): BackgroundService, CancellationToken, ILogger, IOptions, Task, BulkSinkWorkerService

### Community 24 - "AzureKeyVaultTokenAdapter"
Cohesion: 0.20
Nodes (10): CachedCredentialsEntry, CancellationToken, ConcurrentDictionary, DateTimeOffset, ILogger, Task, TimeSpan, AzureKeyVaultTokenAdapter (+2 more)

### Community 25 - "CosmosDbBulkSinkAdapter"
Cohesion: 0.27
Nodes (10): CosmosDbCredentials, IVaultTokenProviderPort, CancellationToken, ILogger, IReadOnlyList, Task, CosmosDbBulkSinkAdapter, RUs (+2 more)

### Community 32 - "SinkSettings"
Cohesion: 0.15
Nodes (13): SinkSettings, BatchSize, BatchTimeoutMs, BootstrapServers, ContainerName, CosmosEndpoint, CosmosPrimaryKey, DatabaseName (+5 more)

### Community 33 - "KafkaStreamConsumerAdapter"
Cohesion: 0.22
Nodes (7): KafkaStreamConsumerAdapter, CancellationToken, Func, IConsumer, IDictionary, ILogger, Task

### Community 34 - "CompiledContractRules"
Cohesion: 0.07
Nodes (31): CachedContractEntry, DataProtectionRulesSettings, Enabled, Full, HashSha256, PartialLast4, Remove, CompiledContractRules (+23 more)

### Community 35 - "CreateTopicDto"
Cohesion: 0.33
Nodes (6): Dictionary, CreateTopicDto, Configs, Partitions, ReplicationFactor, TopicName

### Community 36 - "SendMessageRequestDto"
Cohesion: 0.29
Nodes (7): SendMessageRequestDto, Headers, Key, ServiceName, TelemetryType, Topic, Value

### Community 37 - "KafkaSettings"
Cohesion: 0.18
Nodes (10): KafkaSettings, Acks, BootstrapServers, ClientId, EnableIdempotence, MessageSendMaxRetries, MessageTimeoutMs, RequestTimeoutMs (+2 more)

### Community 39 - "KafkaDemo.Domain.Ports"
Cohesion: 0.29
Nodes (4): KafkaDemo.Domain.Ports, KafkaDemo.Infrastructure.Configuration, KafkaDemo.Infrastructure.Adapters, KafkaDemo.Domain.Models

### Community 40 - "SendMessagesUseCase.cs"
Cohesion: 0.33
Nodes (3): KafkaDemo.Domain.Configuration, KafkaDemo.Domain.Utils, UniformPartitionKeyGenerator

### Community 41 - "ClusterHealthDto"
Cohesion: 0.33
Nodes (6): DateTime, ClusterHealthDto, CheckedAt, IsConnected, Status, TotalTopics

### Community 42 - ".AddKafkaInfrastructure"
Cohesion: 0.07
Nodes (38): CancellationToken, Task, SendMessagesUseCase, TracePruningSettings, Enabled, MaxArrayItems, MaxDepth, CancellationToken (+30 more)

### Community 43 - "AzureKeyVaultTokenAdapter"
Cohesion: 0.14
Nodes (14): IVaultTokenProviderPort, CancellationToken, Task, AzureKeyVaultTokenAdapter, CachedVaultEntry, IsExpired, CachedVaultEntry, CancellationToken (+6 more)

### Community 46 - "KafkaStreamSettings"
Cohesion: 0.13
Nodes (13): KafkaStreamSettings, AutoOffsetReset, BootstrapServers, EnableAutoCommit, GroupId, PollTimeoutMs, SourceTopic, TargetTopic (+5 more)

### Community 48 - "ConsumerStreams.Domain.Ports"
Cohesion: 0.19
Nodes (8): TransactionEnricher, ConsumerStreams.Application.UseCases, ConsumerStreams.Infrastructure.Adapters, ConsumerStreams.Infrastructure, ConsumerStreams.Infrastructure.Configuration, ConsumerStreams.Domain.Ports, ConsumerStreams.Application.Services, ConsumerStreams.Worker

### Community 52 - "🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)"
Cohesion: 0.18
Nodes (10): 🏛️ Arquitectura y Flujo de Procesamiento End-to-End, 🔑 Caché de Certificados y Credenciales en Memoria (TTL 1 Hora), 📈 Escalabilidad Horizontal (Multi-Réplicas), 📁 Estructura del Repositorio, ⚡ Generador de Claves de Partición de Alta Dispersión (SplitMix64), 💾 Micro-Batching y Bulk Sink hacia Azure Cosmos DB / DocumentDB, 🔒 Patrón Criptográfico: Self-Contained Encryption Envelope, 🐳 Portal de Accesos y Servicios (Docker Compose) (+2 more)

### Community 54 - "ConsumerStreams.Domain.Models"
Cohesion: 0.16
Nodes (7): StreamJsonContext, UniformPartitionKeyGenerator, ConsumerStreams.Application.Serialization, ConsumerStreams.Domain.Utils, ConsumerStreams.Domain.Models, JsonSerializerContext, SinkJsonContext

## Knowledge Gaps
- **285 isolated node(s):** `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk`, `Enabled`, `HashSha256` (+280 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `IContractRulesCachePort` connect `.AddConsumerStreamsInfrastructure` to `CompiledContractRules`, `ConsumerStreams.Domain.Models`?**
  _High betweenness centrality (0.132) - this node is a cross-community bridge._
- **Why does `KafkaBatchConsumerAdapter` connect `KafkaBatchConsumerAdapter` to `SinkSettings`, `ISinkPorts.cs`, `.AddLogSinkInfrastructure`, `.AddConsumerStreamsInfrastructure`?**
  _High betweenness centrality (0.130) - this node is a cross-community bridge._
- **Why does `KafkaAdminAdapter` connect `TopicInfo` to `.AddKafkaInfrastructure`, `.AddConsumerStreamsInfrastructure`, `KafkaDemo.Domain.Ports`?**
  _High betweenness centrality (0.115) - this node is a cross-community bridge._
- **What connects `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk` to the rest of the system?**
  _285 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `KafkaMessage` be split into smaller, more focused modules?**
  _Cohesion score 0.06463414634146342 - nodes in this community are weakly interconnected._
- **Should `ProcessedTransactionEvent` be split into smaller, more focused modules?**
  _Cohesion score 0.07692307692307693 - nodes in this community are weakly interconnected._
- **Should `LogDocument` be split into smaller, more focused modules?**
  _Cohesion score 0.0625 - nodes in this community are weakly interconnected._