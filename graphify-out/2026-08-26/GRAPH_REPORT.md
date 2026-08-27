# Graph Report - demo_kafka  (2026-08-26)

## Corpus Check
- 92 files · ~74,165 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 685 nodes · 1032 edges · 38 communities (35 shown, 3 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 61 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `cbf7d657`
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
- KafkaDemo.Domain.Ports
- SinkSettings
- LogSink.Infrastructure.csproj
- Revisión y aprobación del documento
- Lineamiento para nombrado de variables
- VaultKeyMaterial
- Lineamiento para nombrado y despliegue de imágenes
- RawTransactionEvent
- KafkaSettings
- .PruneOuterTrace
- CosmosDbBulkSinkAdapter
- traces_agrupados/README.md
- .AddKafkaInfrastructure
- AzureKeyVaultTokenAdapter
- KafkaStreamSettings
- ConsumerStreams.Domain.Ports
- 🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)
- StreamProcessingPipelineUseCase.cs
- KafkaStreamProducerAdapter
- DocumentDbEmulator.csproj
- StoredDocument

## God Nodes (most connected - your core abstractions)
1. `LogDocument` - 35 edges
2. `ProcessedTransactionEvent` - 30 edges
3. `RawTransactionEvent` - 26 edges
4. `SinkSettings` - 18 edges
5. `KafkaMessage` - 16 edges
6. `MessageResult` - 14 edges
7. `TopicInfo` - 14 edges
8. `VaultKeyMaterial` - 14 edges
9. `CosmosDbBulkSinkAdapter` - 13 edges
10. `KafkaAdminAdapter` - 12 edges

## Surprising Connections (you probably didn't know these)
- `StreamProcessingPipelineUseCase` --references--> `IPayloadCryptoPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `StreamProcessingPipelineUseCase` --references--> `IVaultTokenProviderPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `StreamProcessingPipelineUseCase` --references--> `IStreamProducerPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IStreamPorts.cs
- `StreamWorkerService` --references--> `StreamProcessingPipelineUseCase`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Worker/StreamWorkerService.cs → consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs
- `CachedVaultEntry` --references--> `VaultKeyMaterial`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Infrastructure/Adapters/AzureKeyVaultTokenAdapter.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs

## Import Cycles
- None detected.

## Communities (38 total, 3 thin omitted)

### Community 0 - ".ExecutePipelineAsync"
Cohesion: 0.16
Nodes (11): CancellationToken, Task, IStreamProducerPort, CancellationToken, Func, IDictionary, Task, CancellationToken (+3 more)

### Community 1 - "KafkaMessage"
Cohesion: 0.06
Nodes (36): IReadOnlyList, BatchSendResultDto, ElapsedMilliseconds, Results, TargetTopic, TotalRequested, TotalSent, DateTime (+28 more)

### Community 2 - "ProcessedTransactionEvent"
Cohesion: 0.08
Nodes (26): ProcessedTransactionEvent, Amount, AuditMetadata, Channel, Currency, DestinationAccount, FraudScore, Kind (+18 more)

### Community 3 - "LogDocument"
Cohesion: 0.06
Nodes (31): DateTime, Dictionary, LogDocument, Amount, AuditMetadata, Channel, Currency, DestinationAccount (+23 more)

### Community 5 - "TopicInfo"
Cohesion: 0.08
Nodes (32): CancellationToken, IReadOnlyList, Task, ManageTopicsUseCase, IDictionary, TopicCreationRequest, Configs, NumPartitions (+24 more)

### Community 6 - ".AddConsumerStreamsInfrastructure"
Cohesion: 0.20
Nodes (11): TransactionEnricher, StreamProcessingPipelineUseCase, ILogger, IStreamConsumerPort, ITransactionTransformerPort, KafkaStreamConsumerAdapter, IConsumer, ILogger (+3 more)

### Community 7 - "app.js"
Cohesion: 0.19
Nodes (21): appendBatchToStream(), appendSingleToStream(), checkHealth(), dom, escapeHtml(), fetchTopics(), formatTextareaJson(), generateDispersedKey() (+13 more)

### Community 8 - "KafkaDemo.Infrastructure.csproj"
Cohesion: 0.11
Nodes (19): net10.0, Microsoft.NET.Sdk, net10.0, Microsoft.Extensions.Hosting (10.0.11), Microsoft.NET.Sdk, net10.0, Google.Protobuf (3.36.0), Grpc.Tools (2.83.0) (+11 more)

### Community 9 - "ConsumerStreams.Infrastructure.csproj"
Cohesion: 0.11
Nodes (18): net10.0, Microsoft.Extensions.Logging.Abstractions (10.0.11), Microsoft.NET.Sdk, net10.0, Google.Protobuf (3.36.0), Grpc.Tools (2.83.0), Microsoft.NET.Sdk, net10.0 (+10 more)

### Community 10 - "http"
Cohesion: 0.13
Nodes (15): ASPNETCORE_ENVIRONMENT, applicationUrl, commandName, dotnetRunMessages, environmentVariables, launchBrowser, applicationUrl, commandName (+7 more)

### Community 11 - "KafkaDemo.Domain.Ports"
Cohesion: 0.07
Nodes (28): KafkaDemo.Application.DTOs, KafkaDemo.Domain.Configuration, KafkaDemo.Domain.Ports, KafkaDemo.Application.UseCases, KafkaDemo.Infrastructure, KafkaDemo.Domain.Utils, KafkaDemo.Infrastructure.Configuration, KafkaDemo.Infrastructure.Adapters (+20 more)

### Community 12 - "SinkSettings"
Cohesion: 0.06
Nodes (30): BackgroundService, LogSink.Infrastructure.Configuration, LogSink.Infrastructure.Adapters, LogSink.Application.UseCases, LogSink.Domain.Ports, LogSink.Domain.Models, LogSink.Infrastructure, LogSink.Worker (+22 more)

### Community 13 - "LogSink.Infrastructure.csproj"
Cohesion: 0.14
Nodes (14): net10.0, Microsoft.Extensions.Logging.Abstractions (10.0.0), Microsoft.NET.Sdk, net10.0, Microsoft.NET.Sdk, net10.0, Confluent.Kafka (2.15.0), Microsoft.Extensions.Logging.Abstractions (10.0.0) (+6 more)

### Community 14 - "Revisión y aprobación del documento"
Cohesion: 0.10
Nodes (20): 1. Formatos de datos (payload), 1. Procesamiento de eventos de negocio, 2. Arquitectura basada en eventos (EDA), 3. Persistencia y relectura de mensajes., 4. Fan-out (broadcast), 5. Datos y analítica, Alcance, Casos de Uso (+12 more)

### Community 15 - "Lineamiento para nombrado de variables"
Cohesion: 0.11
Nodes (18): 1. Consideraciones generales, 2.1 Tipo(Type), 2.2 Alcance(Scope), 2.3 Fuente(Source), 2.4 Recurso, 2.5 Atributo, 2.6 Ejemplo de lineamiento, 2. Convención (+10 more)

### Community 16 - "VaultKeyMaterial"
Cohesion: 0.21
Nodes (9): IPayloadCryptoPort, VaultKeyMaterial, AesKey256, CertThumbprint, KeyVersion, VaultTokenId, EncryptedPayloadEnvelope, AesGcmPayloadCryptoAdapter (+1 more)

### Community 17 - "Lineamiento para nombrado y despliegue de imágenes"
Cohesion: 0.18
Nodes (10): Checklist de aceptación, **Consideraciones**, Consideraciones, **<element-name\>**, Lineamiento para nombrado y despliegue de imágenes, Propósito, Revisión y aprobación del documento, Ruta de Acceso (+2 more)

### Community 19 - "RawTransactionEvent"
Cohesion: 0.08
Nodes (24): RawTransactionEvent, Amount, Channel, Currency, DestinationAccount, DurationMs, EmittedAt, EventId (+16 more)

### Community 20 - "KafkaSettings"
Cohesion: 0.18
Nodes (10): KafkaSettings, Acks, BootstrapServers, ClientId, EnableIdempotence, MessageSendMaxRetries, MessageTimeoutMs, RequestTimeoutMs (+2 more)

### Community 21 - ".PruneOuterTrace"
Cohesion: 0.50
Nodes (4): OTelTracePruner, ReadOnlySpan, Utf8JsonReader, Utf8JsonWriter

### Community 22 - "CosmosDbBulkSinkAdapter"
Cohesion: 0.05
Nodes (50): CachedCredentialsEntry, ConsumeResult, Offset, IReadOnlyDictionary, CancellationToken, ILogger, Task, TimeSpan (+42 more)

### Community 42 - ".AddKafkaInfrastructure"
Cohesion: 0.07
Nodes (36): CancellationToken, Task, SendMessagesUseCase, TracePruningSettings, Enabled, MaxArrayItems, MaxDepth, CancellationToken (+28 more)

### Community 43 - "AzureKeyVaultTokenAdapter"
Cohesion: 0.14
Nodes (14): IVaultTokenProviderPort, CancellationToken, Task, AzureKeyVaultTokenAdapter, CachedVaultEntry, IsExpired, CachedVaultEntry, CancellationToken (+6 more)

### Community 46 - "KafkaStreamSettings"
Cohesion: 0.13
Nodes (13): KafkaStreamSettings, AutoOffsetReset, BootstrapServers, EnableAutoCommit, GroupId, PollTimeoutMs, SourceTopic, TargetTopic (+5 more)

### Community 48 - "ConsumerStreams.Domain.Ports"
Cohesion: 0.22
Nodes (7): ConsumerStreams.Application.UseCases, ConsumerStreams.Infrastructure.Adapters, ConsumerStreams.Infrastructure, ConsumerStreams.Infrastructure.Configuration, ConsumerStreams.Domain.Ports, ConsumerStreams.Application.Services, ConsumerStreams.Worker

### Community 52 - "🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)"
Cohesion: 0.18
Nodes (10): 🏛️ Arquitectura y Flujo de Procesamiento End-to-End, 🔑 Caché de Certificados y Credenciales en Memoria (TTL 1 Hora), 📈 Escalabilidad Horizontal (Multi-Réplicas), 📁 Estructura del Repositorio, ⚡ Generador de Claves de Partición de Alta Dispersión (SplitMix64), 💾 Micro-Batching y Bulk Sink hacia Azure Cosmos DB / DocumentDB, 🔒 Patrón Criptográfico: Self-Contained Encryption Envelope, 🐳 Portal de Accesos y Servicios (Docker Compose) (+2 more)

### Community 54 - "StreamProcessingPipelineUseCase.cs"
Cohesion: 0.22
Nodes (5): StreamJsonContext, UniformPartitionKeyGenerator, ConsumerStreams.Application.Serialization, ConsumerStreams.Domain.Utils, ConsumerStreams.Domain.Models

### Community 56 - "KafkaStreamProducerAdapter"
Cohesion: 0.22
Nodes (7): KafkaStreamProducerAdapter, CancellationToken, IDictionary, ILogger, IProducer, Task, IDisposable

## Knowledge Gaps
- **268 isolated node(s):** `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk`, `net10.0`, `Google.Protobuf (3.36.0)` (+263 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `KafkaBatchConsumerAdapter` connect `CosmosDbBulkSinkAdapter` to `KafkaStreamProducerAdapter`, `SinkSettings`?**
  _High betweenness centrality (0.122) - this node is a cross-community bridge._
- **Why does `KafkaStreamConsumerAdapter` connect `.AddConsumerStreamsInfrastructure` to `ConsumerStreams.Domain.Ports`, `.ExecutePipelineAsync`, `KafkaStreamProducerAdapter`, `KafkaStreamSettings`?**
  _High betweenness centrality (0.112) - this node is a cross-community bridge._
- **Why does `KafkaAdminAdapter` connect `TopicInfo` to `KafkaStreamProducerAdapter`, `.AddKafkaInfrastructure`, `KafkaDemo.Domain.Ports`?**
  _High betweenness centrality (0.111) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `KafkaMessage` (e.g. with `.GenerateAndSendBatchAsync()` and `.SendCustomMessageAsync()`) actually correct?**
  _`KafkaMessage` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk` to the rest of the system?**
  _268 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `KafkaMessage` be split into smaller, more focused modules?**
  _Cohesion score 0.0627177700348432 - nodes in this community are weakly interconnected._
- **Should `ProcessedTransactionEvent` be split into smaller, more focused modules?**
  _Cohesion score 0.07692307692307693 - nodes in this community are weakly interconnected._