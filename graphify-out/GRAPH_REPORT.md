# Graph Report - demo_kafka  (2026-08-27)

## Corpus Check
- 99 files · ~79,406 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 757 nodes · 1179 edges · 48 communities (45 shown, 3 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 66 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `a4013b70`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- KafkaStreamProducerAdapter
- MessageResult
- ProcessedTransactionEvent
- LogDocument
- TopicInfo
- .AddConsumerStreamsInfrastructure
- app.js
- KafkaDemo.Infrastructure.csproj
- ConsumerStreams.Infrastructure.csproj
- http
- .ExecutePipelineAsync
- ISinkPorts.cs
- LogSink.Infrastructure.csproj
- Revisión y aprobación del documento
- Lineamiento para nombrado de variables
- TracePruningSettings
- Lineamiento para nombrado y despliegue de imágenes
- RawTransactionEvent
- .StartBatchConsumerAsync
- .AddLogSinkInfrastructure
- KafkaBatchConsumerAdapter
- AzureKeyVaultTokenAdapter
- CosmosDbBulkSinkAdapter
- traces_agrupados/README.md
- SinkSettings
- KafkaStreamConsumerAdapter
- CompiledContractRules
- KafkaMessage
- SendMessageRequestDto
- KafkaProducerAdapter
- .SendBatchAsync
- KafkaDemo.Domain.Ports
- .GenerateAndSendBatchAsync
- .AddKafkaInfrastructure
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
4. `SinkSettings` - 18 edges
5. `CompiledContractRules` - 16 edges
6. `KafkaMessage` - 16 edges
7. `MessageResult` - 14 edges
8. `TopicInfo` - 14 edges
9. `VaultKeyMaterial` - 14 edges
10. `StreamProcessingPipelineUseCase` - 13 edges

## Surprising Connections (you probably didn't know these)
- `StreamProcessingPipelineUseCase` --references--> `DataProtectionRulesSettings`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Configuration/DataProtectionRulesSettings.cs
- `StreamProcessingPipelineUseCase` --references--> `IContractRulesCachePort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IContractRulesCachePort.cs
- `StreamWorkerService` --references--> `StreamProcessingPipelineUseCase`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Worker/StreamWorkerService.cs → consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs
- `ThreadSafeContractRulesCacheAdapter` --implements--> `IContractRulesCachePort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Infrastructure/Adapters/ThreadSafeContractRulesCacheAdapter.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IContractRulesCachePort.cs
- `AzureKeyVaultTokenAdapter` --implements--> `IVaultTokenProviderPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Infrastructure/Adapters/AzureKeyVaultTokenAdapter.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs

## Import Cycles
- None detected.

## Communities (48 total, 3 thin omitted)

### Community 0 - "KafkaStreamProducerAdapter"
Cohesion: 0.17
Nodes (9): IContractRulesCachePort, ActiveContractsCount, KafkaStreamProducerAdapter, CancellationToken, IDictionary, ILogger, IProducer, Task (+1 more)

### Community 1 - "MessageResult"
Cohesion: 0.14
Nodes (14): IReadOnlyList, BatchSendResultDto, ElapsedMilliseconds, Results, TargetTopic, TotalRequested, TotalSent, DateTime (+6 more)

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
Cohesion: 0.25
Nodes (11): TransactionEnricher, StreamProcessingPipelineUseCase, ILogger, IPayloadCryptoPort, IVaultTokenProviderPort, IStreamConsumerPort, IStreamProducerPort, ITransactionTransformerPort (+3 more)

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

### Community 11 - ".ExecutePipelineAsync"
Cohesion: 0.18
Nodes (8): CancellationToken, EncryptedPayloadEnvelope, Task, CancellationToken, Func, IDictionary, Task, UniformPartitionKeyGenerator

### Community 12 - "ISinkPorts.cs"
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

### Community 16 - "TracePruningSettings"
Cohesion: 0.24
Nodes (9): TracePruningSettings, Enabled, MaxArrayItems, MaxDepth, MethodImpl, ReadOnlySpan, Utf8JsonReader, Utf8JsonWriter (+1 more)

### Community 17 - "Lineamiento para nombrado y despliegue de imágenes"
Cohesion: 0.18
Nodes (10): Checklist de aceptación, **Consideraciones**, Consideraciones, **<element-name\>**, Lineamiento para nombrado y despliegue de imágenes, Propósito, Revisión y aprobación del documento, Ruta de Acceso (+2 more)

### Community 19 - "RawTransactionEvent"
Cohesion: 0.08
Nodes (24): RawTransactionEvent, Amount, Channel, Currency, DestinationAccount, DurationMs, EmittedAt, EventId (+16 more)

### Community 20 - ".StartBatchConsumerAsync"
Cohesion: 0.33
Nodes (7): BulkSinkResult, CancellationToken, Func, IReadOnlyList, Task, TimeSpan, IDocumentDbBulkSinkPort

### Community 21 - ".AddLogSinkInfrastructure"
Cohesion: 0.14
Nodes (14): BackgroundService, CancellationToken, ILogger, Task, TimeSpan, BulkSinkPipelineUseCase, IBatchConsumerPort, IConfiguration (+6 more)

### Community 22 - "KafkaBatchConsumerAdapter"
Cohesion: 0.15
Nodes (12): ConsumeResult, Offset, IReadOnlyDictionary, KafkaBatchItem, CancellationToken, Func, IConsumer, ILogger (+4 more)

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

### Community 35 - "KafkaMessage"
Cohesion: 0.20
Nodes (10): DateTime, IDictionary, KafkaMessage, BinaryValue, Headers, IsBinary, Key, Timestamp (+2 more)

### Community 36 - "SendMessageRequestDto"
Cohesion: 0.15
Nodes (13): Dictionary, CreateTopicDto, Configs, Partitions, ReplicationFactor, TopicName, SendMessageRequestDto, Headers (+5 more)

### Community 37 - "KafkaProducerAdapter"
Cohesion: 0.27
Nodes (7): CancellationToken, IEnumerable, ILogger, IProducer, IReadOnlyList, Task, KafkaProducerAdapter

### Community 38 - ".SendBatchAsync"
Cohesion: 0.32
Nodes (5): CancellationToken, IEnumerable, IReadOnlyList, Task, IMessageProducerPort

### Community 39 - "KafkaDemo.Domain.Ports"
Cohesion: 0.11
Nodes (16): KafkaDemo.Application.DTOs, KafkaDemo.Domain.Configuration, KafkaDemo.Domain.Ports, KafkaDemo.Application.UseCases, KafkaDemo.Infrastructure, KafkaDemo.Domain.Utils, KafkaDemo.Infrastructure.Configuration, KafkaDemo.Infrastructure.Adapters (+8 more)

### Community 40 - ".GenerateAndSendBatchAsync"
Cohesion: 0.36
Nodes (4): CancellationToken, Task, SendMessagesUseCase, UniformPartitionKeyGenerator

### Community 42 - ".AddKafkaInfrastructure"
Cohesion: 0.06
Nodes (41): CancellationToken, EncryptedPayloadEnvelope, IDictionary, Task, TelemetryType, IPayloadCryptoPort, IVaultTokenProviderPort, VaultKeyMaterial (+33 more)

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
Cohesion: 0.25
Nodes (5): StreamJsonContext, ConsumerStreams.Application.Serialization, ConsumerStreams.Domain.Utils, ConsumerStreams.Domain.Models, JsonSerializerContext

## Knowledge Gaps
- **285 isolated node(s):** `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk`, `Enabled`, `HashSha256` (+280 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `IContractRulesCachePort` connect `KafkaStreamProducerAdapter` to `CompiledContractRules`, `.AddConsumerStreamsInfrastructure`?**
  _High betweenness centrality (0.134) - this node is a cross-community bridge._
- **Why does `KafkaBatchConsumerAdapter` connect `KafkaBatchConsumerAdapter` to `KafkaStreamProducerAdapter`, `SinkSettings`, `ISinkPorts.cs`, `.AddLogSinkInfrastructure`?**
  _High betweenness centrality (0.130) - this node is a cross-community bridge._
- **Why does `KafkaAdminAdapter` connect `TopicInfo` to `KafkaStreamProducerAdapter`, `.AddKafkaInfrastructure`, `KafkaDemo.Domain.Ports`?**
  _High betweenness centrality (0.116) - this node is a cross-community bridge._
- **What connects `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk` to the rest of the system?**
  _285 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `MessageResult` be split into smaller, more focused modules?**
  _Cohesion score 0.14285714285714285 - nodes in this community are weakly interconnected._
- **Should `ProcessedTransactionEvent` be split into smaller, more focused modules?**
  _Cohesion score 0.07692307692307693 - nodes in this community are weakly interconnected._
- **Should `LogDocument` be split into smaller, more focused modules?**
  _Cohesion score 0.0625 - nodes in this community are weakly interconnected._