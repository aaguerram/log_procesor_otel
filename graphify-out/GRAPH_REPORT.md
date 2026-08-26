# Graph Report - demo_kafka  (2026-08-26)

## Corpus Check
- 170 files · ~82,718 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 686 nodes · 1018 edges · 71 communities (69 shown, 2 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 56 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

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
- EncryptedPayloadEnvelope
- LogSink.Infrastructure.csproj
- EncryptedPayloadEnvelope
- VaultKeyMaterial
- AzureKeyVaultTokenAdapter
- .AddLogSinkInfrastructure
- AzureKeyVaultTokenAdapter
- KafkaStreamSettings
- KafkaBatchConsumerAdapter
- ConsumerStreams.Domain.Ports
- VaultKeyMaterial
- CosmosDbBulkSinkAdapter
- .StartBatchConsumerAsync
- 🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)
- KafkaSettings
- StreamProcessingPipelineUseCase.cs
- KafkaStreamConsumerAdapter
- KafkaStreamProducerAdapter
- DocumentDbEmulator.csproj
- StoredDocument

## God Nodes (most connected - your core abstractions)
1. `EncryptedPayloadEnvelope` - 36 edges
2. `EncryptedPayloadEnvelope` - 27 edges
3. `LogDocument` - 26 edges
4. `ProcessedTransactionEvent` - 21 edges
5. `SinkSettings` - 18 edges
6. `RawTransactionEvent` - 17 edges
7. `KafkaMessage` - 16 edges
8. `MessageResult` - 14 edges
9. `TopicInfo` - 14 edges
10. `VaultKeyMaterial` - 14 edges

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

## Communities (71 total, 2 thin omitted)

### Community 0 - ".ExecutePipelineAsync"
Cohesion: 0.27
Nodes (7): CancellationToken, Task, IStreamProducerPort, CancellationToken, Func, IDictionary, Task

### Community 1 - "KafkaMessage"
Cohesion: 0.07
Nodes (31): CancellationToken, Task, DateTime, IDictionary, KafkaMessage, BinaryValue, Headers, IsBinary (+23 more)

### Community 2 - "ProcessedTransactionEvent"
Cohesion: 0.07
Nodes (32): ProcessedTransactionEvent, Amount, AuditMetadata, Channel, Currency, DestinationAccount, FraudScore, OriginAccount (+24 more)

### Community 3 - "LogDocument"
Cohesion: 0.09
Nodes (23): DateTime, Dictionary, LogDocument, Amount, AuditMetadata, Channel, Currency, DestinationAccount (+15 more)

### Community 4 - "SinkSettings"
Cohesion: 0.06
Nodes (31): BackgroundService, StreamJsonContext, LogSink.Infrastructure.Configuration, LogSink.Infrastructure.Adapters, LogSink.Application.UseCases, LogSink.Domain.Ports, LogSink.Domain.Models, LogSink.Infrastructure (+23 more)

### Community 5 - "TopicInfo"
Cohesion: 0.08
Nodes (33): CancellationToken, IReadOnlyList, Task, ManageTopicsUseCase, SendMessagesUseCase, IDictionary, TopicCreationRequest, Configs (+25 more)

### Community 6 - ".AddConsumerStreamsInfrastructure"
Cohesion: 0.29
Nodes (8): TransactionEnricher, StreamProcessingPipelineUseCase, ILogger, IStreamConsumerPort, ITransactionTransformerPort, DependencyInjection, IConfiguration, IServiceCollection

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
Cohesion: 0.06
Nodes (34): KafkaDemo.Application.DTOs, KafkaDemo.Domain.Ports, KafkaDemo.Application.UseCases, KafkaDemo.Infrastructure, KafkaDemo.Domain.Utils, KafkaDemo.Infrastructure.Configuration, KafkaDemo.Infrastructure.Adapters, KafkaDemo.Domain.Models (+26 more)

### Community 12 - "EncryptedPayloadEnvelope"
Cohesion: 0.09
Nodes (26): ByteString, CodedInputStream, CodedOutputStream, DebuggerNonUserCodeAttribute, FileDescriptor, GeneratedCode, MessageDescriptor, MessageParser (+18 more)

### Community 13 - "LogSink.Infrastructure.csproj"
Cohesion: 0.14
Nodes (14): net10.0, Microsoft.Extensions.Logging.Abstractions (10.0.0), Microsoft.NET.Sdk, net10.0, Microsoft.NET.Sdk, net10.0, Confluent.Kafka (2.15.0), Microsoft.Extensions.Logging.Abstractions (10.0.0) (+6 more)

### Community 41 - "EncryptedPayloadEnvelope"
Cohesion: 0.09
Nodes (26): ByteString, CodedInputStream, CodedOutputStream, DebuggerNonUserCodeAttribute, FileDescriptor, GeneratedCode, MessageDescriptor, MessageParser (+18 more)

### Community 42 - "VaultKeyMaterial"
Cohesion: 0.08
Nodes (29): Produbanco.Security.V1, CancellationToken, IDictionary, Task, IPayloadCryptoPort, IVaultTokenProviderPort, VaultKeyMaterial, AesKey256 (+21 more)

### Community 43 - "AzureKeyVaultTokenAdapter"
Cohesion: 0.18
Nodes (11): AzureKeyVaultTokenAdapter, CachedVaultEntry, IsExpired, CachedVaultEntry, CancellationToken, ConcurrentDictionary, DateTimeOffset, ILogger (+3 more)

### Community 44 - ".AddLogSinkInfrastructure"
Cohesion: 0.36
Nodes (6): ILogger, BulkSinkPipelineUseCase, IBatchConsumerPort, IDocumentDbBulkSinkPort, IConfiguration, IServiceCollection

### Community 45 - "AzureKeyVaultTokenAdapter"
Cohesion: 0.19
Nodes (12): CachedCredentialsEntry, CosmosDbCredentials, IVaultTokenProviderPort, CancellationToken, ConcurrentDictionary, DateTimeOffset, ILogger, Task (+4 more)

### Community 46 - "KafkaStreamSettings"
Cohesion: 0.13
Nodes (13): KafkaStreamSettings, AutoOffsetReset, BootstrapServers, EnableAutoCommit, GroupId, PollTimeoutMs, SourceTopic, TargetTopic (+5 more)

### Community 47 - "KafkaBatchConsumerAdapter"
Cohesion: 0.15
Nodes (12): ConsumeResult, Offset, IReadOnlyDictionary, KafkaBatchItem, CancellationToken, Func, IConsumer, ILogger (+4 more)

### Community 48 - "ConsumerStreams.Domain.Ports"
Cohesion: 0.22
Nodes (7): ConsumerStreams.Application.UseCases, ConsumerStreams.Infrastructure.Adapters, ConsumerStreams.Infrastructure, ConsumerStreams.Infrastructure.Configuration, ConsumerStreams.Domain.Ports, ConsumerStreams.Application.Services, ConsumerStreams.Worker

### Community 49 - "VaultKeyMaterial"
Cohesion: 0.19
Nodes (10): IPayloadCryptoPort, IVaultTokenProviderPort, VaultKeyMaterial, AesKey256, CertThumbprint, KeyVersion, VaultTokenId, CancellationToken (+2 more)

### Community 50 - "CosmosDbBulkSinkAdapter"
Cohesion: 0.25
Nodes (8): CancellationToken, ILogger, IReadOnlyList, Task, CosmosDbBulkSinkAdapter, RUs, SemaphoreSlim, Success

### Community 51 - ".StartBatchConsumerAsync"
Cohesion: 0.19
Nodes (9): CancellationToken, Task, TimeSpan, BulkSinkResult, CancellationToken, Func, IReadOnlyList, Task (+1 more)

### Community 52 - "🚀 Red Hat AMQ Streams, Azure DocumentDB & Azure Key Vault - .NET 10 Native AOT (Arquitectura Hexagonal)"
Cohesion: 0.18
Nodes (10): 🏛️ Arquitectura y Flujo de Procesamiento End-to-End, 🔑 Caché de Certificados y Credenciales en Memoria (TTL 1 Hora), 📈 Escalabilidad Horizontal (Multi-Réplicas), 📁 Estructura del Repositorio, ⚡ Generador de Claves de Partición de Alta Dispersión (SplitMix64), 💾 Micro-Batching y Bulk Sink hacia Azure Cosmos DB / DocumentDB, 🔒 Patrón Criptográfico: Self-Contained Encryption Envelope, 🐳 Portal de Accesos y Servicios (Docker Compose) (+2 more)

### Community 53 - "KafkaSettings"
Cohesion: 0.18
Nodes (10): KafkaSettings, Acks, BootstrapServers, ClientId, EnableIdempotence, MessageSendMaxRetries, MessageTimeoutMs, RequestTimeoutMs (+2 more)

### Community 54 - "StreamProcessingPipelineUseCase.cs"
Cohesion: 0.25
Nodes (4): UniformPartitionKeyGenerator, ConsumerStreams.Application.Serialization, ConsumerStreams.Domain.Utils, ConsumerStreams.Domain.Models

### Community 55 - "KafkaStreamConsumerAdapter"
Cohesion: 0.22
Nodes (7): KafkaStreamConsumerAdapter, CancellationToken, Func, IConsumer, IDictionary, ILogger, Task

### Community 56 - "KafkaStreamProducerAdapter"
Cohesion: 0.22
Nodes (7): KafkaStreamProducerAdapter, CancellationToken, IDictionary, ILogger, IProducer, Task, IDisposable

## Knowledge Gaps
- **224 isolated node(s):** `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk`, `net10.0`, `Google.Protobuf (3.36.0)` (+219 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **2 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `EncryptedPayloadEnvelope` connect `EncryptedPayloadEnvelope` to `VaultKeyMaterial`, `VaultKeyMaterial`, `EncryptedPayloadEnvelope`?**
  _High betweenness centrality (0.161) - this node is a cross-community bridge._
- **Why does `KafkaBatchConsumerAdapter` connect `KafkaBatchConsumerAdapter` to `KafkaStreamProducerAdapter`, `SinkSettings`, `.AddLogSinkInfrastructure`?**
  _High betweenness centrality (0.102) - this node is a cross-community bridge._
- **Why does `CosmosDbBulkSinkAdapter` connect `CosmosDbBulkSinkAdapter` to `VaultKeyMaterial`, `SinkSettings`, `AzureKeyVaultTokenAdapter`, `.AddLogSinkInfrastructure`?**
  _High betweenness centrality (0.087) - this node is a cross-community bridge._
- **What connects `net10.0`, `Microsoft.Extensions.Logging.Abstractions (10.0.11)`, `Microsoft.NET.Sdk` to the rest of the system?**
  _224 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `KafkaMessage` be split into smaller, more focused modules?**
  _Cohesion score 0.07396870554765292 - nodes in this community are weakly interconnected._
- **Should `ProcessedTransactionEvent` be split into smaller, more focused modules?**
  _Cohesion score 0.0659536541889483 - nodes in this community are weakly interconnected._
- **Should `LogDocument` be split into smaller, more focused modules?**
  _Cohesion score 0.08695652173913043 - nodes in this community are weakly interconnected._