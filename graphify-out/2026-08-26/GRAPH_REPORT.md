# Graph Report - demo_kafka  (2026-08-26)

## Corpus Check
- 70 files · ~15,572 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 575 nodes · 893 edges · 30 communities (28 shown, 2 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 56 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `13578ba5`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- .ExecutePipelineAsync
- KafkaMessage
- ProcessedTransactionEvent
- LogDocument
- SinkSettings
- TopicInfo
- .AddConsumerStreamsInfrastructure
- app.js
- KafkaDemo.Infrastructure.csproj
- ConsumerStreams.Infrastructure.csproj
- http
- .AddKafkaInfrastructure
- LogSink.Infrastructure.Configuration
- LogSink.Infrastructure.csproj
- StreamWorkerService
- LogSink.Domain.Models
- KafkaBatchConsumerAdapter
- VaultKeyMaterial
- VaultKeyMaterial
- CosmosDbBulkSinkAdapter
- AzureKeyVaultTokenAdapter
- KafkaStreamSettings
- .StartBatchConsumerAsync
- ConsumerStreams.Domain.Ports
- BulkSinkPipelineUseCase
- 🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)
- StreamProcessingPipelineUseCase.cs
- KafkaStreamProducerAdapter
- DocumentDbEmulator.csproj
- StoredDocument

## God Nodes (most connected - your core abstractions)
1. `LogDocument` - 26 edges
2. `ProcessedTransactionEvent` - 21 edges
3. `SinkSettings` - 18 edges
4. `RawTransactionEvent` - 17 edges
5. `KafkaMessage` - 16 edges
6. `MessageResult` - 14 edges
7. `TopicInfo` - 14 edges
8. `VaultKeyMaterial` - 14 edges
9. `KafkaAdminAdapter` - 12 edges
10. `KafkaStreamSettings` - 11 edges

## Surprising Connections (you probably didn't know these)
- `StreamProcessingPipelineUseCase` --references--> `IVaultTokenProviderPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `StreamWorkerService` --references--> `StreamProcessingPipelineUseCase`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Worker/StreamWorkerService.cs → consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs
- `KafkaStreamConsumerAdapter` --implements--> `IStreamConsumerPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Infrastructure/Adapters/KafkaStreamConsumerAdapter.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IStreamPorts.cs
- `KafkaStreamProducerAdapter` --implements--> `IStreamProducerPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Infrastructure/Adapters/KafkaStreamProducerAdapter.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IStreamPorts.cs
- `StreamWorkerService` --references--> `KafkaStreamSettings`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Worker/StreamWorkerService.cs → consumer_streams/src/ConsumerStreams.Infrastructure/Configuration/KafkaStreamSettings.cs

## Import Cycles
- None detected.

## Communities (30 total, 2 thin omitted)

### Community 0 - ".ExecutePipelineAsync"
Cohesion: 0.18
Nodes (10): CancellationToken, Task, CancellationToken, Func, IDictionary, Task, CancellationToken, Func (+2 more)

### Community 1 - "KafkaMessage"
Cohesion: 0.06
Nodes (36): KafkaDemo.Domain.Utils, IReadOnlyList, BatchSendResultDto, ElapsedMilliseconds, Results, TargetTopic, TotalRequested, TotalSent (+28 more)

### Community 2 - "ProcessedTransactionEvent"
Cohesion: 0.07
Nodes (32): ProcessedTransactionEvent, Amount, AuditMetadata, Channel, Currency, DestinationAccount, FraudScore, OriginAccount (+24 more)

### Community 3 - "LogDocument"
Cohesion: 0.05
Nodes (40): DateTime, Dictionary, BulkSinkResult, LogDocument, Amount, AuditMetadata, Channel, Currency (+32 more)

### Community 4 - "SinkSettings"
Cohesion: 0.14
Nodes (13): SinkSettings, BatchSize, BatchTimeoutMs, BootstrapServers, ContainerName, CosmosEndpoint, CosmosPrimaryKey, DatabaseName (+5 more)

### Community 5 - "TopicInfo"
Cohesion: 0.07
Nodes (38): DateTime, ClusterHealthDto, CheckedAt, IsConnected, Status, TotalTopics, CancellationToken, IReadOnlyList (+30 more)

### Community 6 - ".AddConsumerStreamsInfrastructure"
Cohesion: 0.20
Nodes (12): TransactionEnricher, StreamProcessingPipelineUseCase, ILogger, IPayloadCryptoPort, EncryptedPayloadEnvelope, IStreamConsumerPort, IStreamProducerPort, ITransactionTransformerPort (+4 more)

### Community 7 - "app.js"
Cohesion: 0.20
Nodes (19): appendBatchToStream(), appendSingleToStream(), checkHealth(), dom, escapeHtml(), fetchTopics(), formatTextareaJson(), handleCreateTopic() (+11 more)

### Community 8 - "KafkaDemo.Infrastructure.csproj"
Cohesion: 0.11
Nodes (19): net10.0, Microsoft.NET.Sdk, net10.0, Microsoft.Extensions.Hosting (10.0.11), Microsoft.NET.Sdk, net10.0, Google.Protobuf (3.36.0), Grpc.Tools (2.83.0) (+11 more)

### Community 9 - "ConsumerStreams.Infrastructure.csproj"
Cohesion: 0.11
Nodes (18): net10.0, Microsoft.Extensions.Logging.Abstractions (10.0.11), Microsoft.NET.Sdk, net10.0, Google.Protobuf (3.36.0), Grpc.Tools (2.83.0), Microsoft.NET.Sdk, net10.0 (+10 more)

### Community 10 - "http"
Cohesion: 0.13
Nodes (15): ASPNETCORE_ENVIRONMENT, applicationUrl, commandName, dotnetRunMessages, environmentVariables, launchBrowser, applicationUrl, commandName (+7 more)

### Community 11 - ".AddKafkaInfrastructure"
Cohesion: 0.05
Nodes (37): KafkaDemo.Application.DTOs, KafkaDemo.Domain.Ports, KafkaDemo.Application.UseCases, KafkaDemo.Infrastructure, KafkaDemo.Infrastructure.Configuration, KafkaDemo.Infrastructure.Adapters, KafkaDemo.Domain.Models, Dictionary (+29 more)

### Community 12 - "LogSink.Infrastructure.Configuration"
Cohesion: 0.29
Nodes (6): LogSink.Infrastructure.Configuration, LogSink.Infrastructure.Adapters, LogSink.Application.UseCases, LogSink.Domain.Ports, LogSink.Infrastructure, LogSink.Worker

### Community 13 - "LogSink.Infrastructure.csproj"
Cohesion: 0.14
Nodes (14): net10.0, Microsoft.Extensions.Logging.Abstractions (10.0.0), Microsoft.NET.Sdk, net10.0, Microsoft.NET.Sdk, net10.0, Confluent.Kafka (2.15.0), Microsoft.Extensions.Logging.Abstractions (10.0.0) (+6 more)

### Community 14 - "StreamWorkerService"
Cohesion: 0.20
Nodes (7): StreamWorkerService, CancellationToken, ILogger, IOptions, Task, ConsumerStreams.Infrastructure, ConsumerStreams.Worker

### Community 15 - "LogSink.Domain.Models"
Cohesion: 0.50
Nodes (3): LogSink.Domain.Models, LogSink.Application.Serialization, SinkJsonContext

### Community 16 - "KafkaBatchConsumerAdapter"
Cohesion: 0.50
Nodes (3): IConsumer, ILogger, KafkaBatchConsumerAdapter

### Community 42 - "VaultKeyMaterial"
Cohesion: 0.09
Nodes (26): CancellationToken, EncryptedPayloadEnvelope, IDictionary, Task, IPayloadCryptoPort, IVaultTokenProviderPort, VaultKeyMaterial, AesKey256 (+18 more)

### Community 43 - "VaultKeyMaterial"
Cohesion: 0.11
Nodes (19): IVaultTokenProviderPort, VaultKeyMaterial, AesKey256, CertThumbprint, KeyVersion, VaultTokenId, CancellationToken, Task (+11 more)

### Community 44 - "CosmosDbBulkSinkAdapter"
Cohesion: 0.25
Nodes (9): IBatchConsumerPort, IDocumentDbBulkSinkPort, IVaultTokenProviderPort, ILogger, CosmosDbBulkSinkAdapter, IConfiguration, IServiceCollection, DependencyInjection (+1 more)

### Community 45 - "AzureKeyVaultTokenAdapter"
Cohesion: 0.40
Nodes (5): CachedCredentialsEntry, ConcurrentDictionary, ILogger, TimeSpan, AzureKeyVaultTokenAdapter

### Community 46 - "KafkaStreamSettings"
Cohesion: 0.15
Nodes (11): KafkaStreamConsumerAdapter, IConsumer, ILogger, KafkaStreamSettings, AutoOffsetReset, BootstrapServers, EnableAutoCommit, GroupId (+3 more)

### Community 47 - ".StartBatchConsumerAsync"
Cohesion: 0.20
Nodes (9): ConsumeResult, Offset, IReadOnlyDictionary, KafkaBatchItem, CancellationToken, Func, IReadOnlyList, Task (+1 more)

### Community 48 - "ConsumerStreams.Domain.Ports"
Cohesion: 0.31
Nodes (5): DependencyInjection, ConsumerStreams.Infrastructure.Adapters, ConsumerStreams.Infrastructure.Configuration, ConsumerStreams.Domain.Ports, ConsumerStreams.Application.Services

### Community 51 - "BulkSinkPipelineUseCase"
Cohesion: 0.18
Nodes (11): BackgroundService, CancellationToken, ILogger, Task, TimeSpan, BulkSinkPipelineUseCase, CancellationToken, ILogger (+3 more)

### Community 52 - "🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)"
Cohesion: 0.18
Nodes (10): 🏛️ Arquitectura y Flujo de Procesamiento End-to-End, 🔑 Caché de Certificados y Credenciales en Memoria (TTL 1 Hora), 📈 Escalabilidad Horizontal (Multi-Réplicas), 📁 Estructura del Repositorio, ⚡ Generador de Claves de Partición de Alta Dispersión (SplitMix64), 💾 Micro-Batching y Bulk Sink hacia Azure Cosmos DB / DocumentDB, 🔒 Patrón Criptográfico: Self-Contained Encryption Envelope, 🐳 Portal de Accesos y Servicios (Docker Compose) (+2 more)

### Community 54 - "StreamProcessingPipelineUseCase.cs"
Cohesion: 0.18
Nodes (7): StreamJsonContext, UniformPartitionKeyGenerator, ConsumerStreams.Application.UseCases, ConsumerStreams.Application.Serialization, ConsumerStreams.Domain.Utils, ConsumerStreams.Domain.Models, JsonSerializerContext

### Community 56 - "KafkaStreamProducerAdapter"
Cohesion: 0.22
Nodes (7): KafkaStreamProducerAdapter, CancellationToken, IDictionary, ILogger, IProducer, Task, IDisposable

## Knowledge Gaps
- **202 isolated node(s):** `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk`, `net10.0`, `Google.Protobuf (3.36.0)` (+197 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **2 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `KafkaBatchConsumerAdapter` connect `KafkaBatchConsumerAdapter` to `SinkSettings`, `LogSink.Infrastructure.Configuration`, `CosmosDbBulkSinkAdapter`, `.StartBatchConsumerAsync`, `KafkaStreamProducerAdapter`?**
  _High betweenness centrality (0.147) - this node is a cross-community bridge._
- **Why does `KafkaAdminAdapter` connect `TopicInfo` to `KafkaStreamProducerAdapter`, `.AddKafkaInfrastructure`?**
  _High betweenness centrality (0.133) - this node is a cross-community bridge._
- **Why does `KafkaStreamConsumerAdapter` connect `KafkaStreamSettings` to `ConsumerStreams.Domain.Ports`, `.ExecutePipelineAsync`, `KafkaStreamProducerAdapter`, `.AddConsumerStreamsInfrastructure`?**
  _High betweenness centrality (0.130) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `KafkaMessage` (e.g. with `.GenerateAndSendBatchAsync()` and `.SendCustomMessageAsync()`) actually correct?**
  _`KafkaMessage` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk` to the rest of the system?**
  _202 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `KafkaMessage` be split into smaller, more focused modules?**
  _Cohesion score 0.06342494714587738 - nodes in this community are weakly interconnected._
- **Should `ProcessedTransactionEvent` be split into smaller, more focused modules?**
  _Cohesion score 0.0659536541889483 - nodes in this community are weakly interconnected._