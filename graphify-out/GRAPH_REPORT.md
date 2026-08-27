# Graph Report - demo_kafka  (2026-08-26)

## Corpus Check
- 76 files · ~43,128 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 626 nodes · 941 edges · 33 communities (31 shown, 2 thin omitted)
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
- KafkaDemo.Domain.Ports
- LogSink.Infrastructure.Configuration
- LogSink.Infrastructure.csproj
- Revisión y aprobación del documento
- Lineamiento para nombrado de variables
- VaultKeyMaterial
- Lineamiento para nombrado y despliegue de imágenes
- KafkaStreamConsumerAdapter
- .StartBatchConsumerAsync
- VaultKeyMaterial
- AzureKeyVaultTokenAdapter
- .AddLogSinkInfrastructure
- AzureKeyVaultTokenAdapter
- KafkaStreamSettings
- KafkaBatchConsumerAdapter
- ConsumerStreams.Domain.Ports
- .ExecuteBulkSinkPipelineAsync
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
- `StreamProcessingPipelineUseCase` --references--> `IPayloadCryptoPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `StreamProcessingPipelineUseCase` --references--> `IVaultTokenProviderPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `StreamWorkerService` --references--> `StreamProcessingPipelineUseCase`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Worker/StreamWorkerService.cs → consumer_streams/src/ConsumerStreams.Application/UseCases/StreamProcessingPipelineUseCase.cs
- `CachedVaultEntry` --references--> `VaultKeyMaterial`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Infrastructure/Adapters/AzureKeyVaultTokenAdapter.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/ICryptoPorts.cs
- `KafkaStreamConsumerAdapter` --implements--> `IStreamConsumerPort`  [EXTRACTED]
  consumer_streams/src/ConsumerStreams.Infrastructure/Adapters/KafkaStreamConsumerAdapter.cs → consumer_streams/src/ConsumerStreams.Domain/Ports/IStreamPorts.cs

## Import Cycles
- None detected.

## Communities (33 total, 2 thin omitted)

### Community 0 - ".ExecutePipelineAsync"
Cohesion: 0.31
Nodes (6): CancellationToken, Task, CancellationToken, Func, IDictionary, Task

### Community 1 - "KafkaMessage"
Cohesion: 0.06
Nodes (38): IReadOnlyList, BatchSendResultDto, ElapsedMilliseconds, Results, TargetTopic, TotalRequested, TotalSent, CancellationToken (+30 more)

### Community 2 - "ProcessedTransactionEvent"
Cohesion: 0.07
Nodes (32): ProcessedTransactionEvent, Amount, AuditMetadata, Channel, Currency, DestinationAccount, FraudScore, OriginAccount (+24 more)

### Community 3 - "LogDocument"
Cohesion: 0.07
Nodes (32): DateTime, Dictionary, BulkSinkResult, LogDocument, Amount, AuditMetadata, Channel, Currency (+24 more)

### Community 4 - "SinkSettings"
Cohesion: 0.15
Nodes (13): SinkSettings, BatchSize, BatchTimeoutMs, BootstrapServers, ContainerName, CosmosEndpoint, CosmosPrimaryKey, DatabaseName (+5 more)

### Community 5 - "TopicInfo"
Cohesion: 0.07
Nodes (36): CancellationToken, IReadOnlyList, Task, ManageTopicsUseCase, SendMessagesUseCase, IDictionary, TopicCreationRequest, Configs (+28 more)

### Community 6 - ".AddConsumerStreamsInfrastructure"
Cohesion: 0.23
Nodes (10): TransactionEnricher, StreamProcessingPipelineUseCase, ILogger, IStreamConsumerPort, IStreamProducerPort, ITransactionTransformerPort, DependencyInjection, IConfiguration (+2 more)

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

### Community 11 - "KafkaDemo.Domain.Ports"
Cohesion: 0.05
Nodes (37): KafkaDemo.Application.DTOs, KafkaDemo.Domain.Ports, KafkaDemo.Application.UseCases, KafkaDemo.Infrastructure, KafkaDemo.Domain.Utils, KafkaDemo.Infrastructure.Configuration, KafkaDemo.Infrastructure.Adapters, KafkaDemo.Domain.Models (+29 more)

### Community 12 - "LogSink.Infrastructure.Configuration"
Cohesion: 0.15
Nodes (12): StreamJsonContext, LogSink.Infrastructure.Configuration, LogSink.Infrastructure.Adapters, LogSink.Application.UseCases, LogSink.Domain.Ports, LogSink.Domain.Models, LogSink.Infrastructure, LogSink.Worker (+4 more)

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

### Community 18 - "KafkaStreamConsumerAdapter"
Cohesion: 0.22
Nodes (7): KafkaStreamConsumerAdapter, CancellationToken, Func, IConsumer, IDictionary, ILogger, Task

### Community 19 - ".StartBatchConsumerAsync"
Cohesion: 0.31
Nodes (6): CancellationToken, Func, IReadOnlyList, Task, TimeSpan, IVaultTokenProviderPort

### Community 42 - "VaultKeyMaterial"
Cohesion: 0.09
Nodes (26): CancellationToken, EncryptedPayloadEnvelope, IDictionary, Task, IPayloadCryptoPort, IVaultTokenProviderPort, VaultKeyMaterial, AesKey256 (+18 more)

### Community 43 - "AzureKeyVaultTokenAdapter"
Cohesion: 0.14
Nodes (14): IVaultTokenProviderPort, CancellationToken, Task, AzureKeyVaultTokenAdapter, CachedVaultEntry, IsExpired, CachedVaultEntry, CancellationToken (+6 more)

### Community 44 - ".AddLogSinkInfrastructure"
Cohesion: 0.36
Nodes (6): ILogger, BulkSinkPipelineUseCase, IBatchConsumerPort, IDocumentDbBulkSinkPort, IConfiguration, IServiceCollection

### Community 45 - "AzureKeyVaultTokenAdapter"
Cohesion: 0.20
Nodes (11): CachedCredentialsEntry, CosmosDbCredentials, CancellationToken, ConcurrentDictionary, DateTimeOffset, ILogger, Task, TimeSpan (+3 more)

### Community 46 - "KafkaStreamSettings"
Cohesion: 0.13
Nodes (13): KafkaStreamSettings, AutoOffsetReset, BootstrapServers, EnableAutoCommit, GroupId, PollTimeoutMs, SourceTopic, TargetTopic (+5 more)

### Community 47 - "KafkaBatchConsumerAdapter"
Cohesion: 0.15
Nodes (12): ConsumeResult, Offset, IReadOnlyDictionary, KafkaBatchItem, CancellationToken, Func, IConsumer, ILogger (+4 more)

### Community 48 - "ConsumerStreams.Domain.Ports"
Cohesion: 0.26
Nodes (6): ConsumerStreams.Application.UseCases, ConsumerStreams.Infrastructure.Adapters, ConsumerStreams.Infrastructure, ConsumerStreams.Infrastructure.Configuration, ConsumerStreams.Domain.Ports, ConsumerStreams.Worker

### Community 51 - ".ExecuteBulkSinkPipelineAsync"
Cohesion: 0.18
Nodes (9): BackgroundService, CancellationToken, Task, TimeSpan, CancellationToken, ILogger, IOptions, Task (+1 more)

### Community 52 - "🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)"
Cohesion: 0.18
Nodes (10): 🏛️ Arquitectura y Flujo de Procesamiento End-to-End, 🔑 Caché de Certificados y Credenciales en Memoria (TTL 1 Hora), 📈 Escalabilidad Horizontal (Multi-Réplicas), 📁 Estructura del Repositorio, ⚡ Generador de Claves de Partición de Alta Dispersión (SplitMix64), 💾 Micro-Batching y Bulk Sink hacia Azure Cosmos DB / DocumentDB, 🔒 Patrón Criptográfico: Self-Contained Encryption Envelope, 🐳 Portal de Accesos y Servicios (Docker Compose) (+2 more)

### Community 54 - "StreamProcessingPipelineUseCase.cs"
Cohesion: 0.25
Nodes (4): UniformPartitionKeyGenerator, ConsumerStreams.Application.Serialization, ConsumerStreams.Domain.Utils, ConsumerStreams.Domain.Models

### Community 56 - "KafkaStreamProducerAdapter"
Cohesion: 0.22
Nodes (7): KafkaStreamProducerAdapter, CancellationToken, IDictionary, ILogger, IProducer, Task, IDisposable

## Knowledge Gaps
- **237 isolated node(s):** `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk`, `net10.0`, `Google.Protobuf (3.36.0)` (+232 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **2 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `KafkaBatchConsumerAdapter` connect `KafkaBatchConsumerAdapter` to `KafkaStreamProducerAdapter`, `LogSink.Infrastructure.Configuration`, `SinkSettings`, `.AddLogSinkInfrastructure`?**
  _High betweenness centrality (0.124) - this node is a cross-community bridge._
- **Why does `KafkaAdminAdapter` connect `TopicInfo` to `KafkaStreamProducerAdapter`, `KafkaDemo.Domain.Ports`?**
  _High betweenness centrality (0.112) - this node is a cross-community bridge._
- **Why does `KafkaStreamConsumerAdapter` connect `KafkaStreamConsumerAdapter` to `ConsumerStreams.Domain.Ports`, `KafkaStreamSettings`, `KafkaStreamProducerAdapter`, `.AddConsumerStreamsInfrastructure`?**
  _High betweenness centrality (0.110) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `KafkaMessage` (e.g. with `.GenerateAndSendBatchAsync()` and `.SendCustomMessageAsync()`) actually correct?**
  _`KafkaMessage` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk` to the rest of the system?**
  _237 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `KafkaMessage` be split into smaller, more focused modules?**
  _Cohesion score 0.0595959595959596 - nodes in this community are weakly interconnected._
- **Should `ProcessedTransactionEvent` be split into smaller, more focused modules?**
  _Cohesion score 0.0659536541889483 - nodes in this community are weakly interconnected._