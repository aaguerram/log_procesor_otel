# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A local, end-to-end **observability streaming pipeline** demo for Produbanco: OpenTelemetry
traces/metrics/logs are emitted, encrypted, streamed through Kafka, masked according to the
originating service's OpenAPI contract, risk-scored, and bulk-persisted to a Cosmos DB
emulator. Four independent **.NET 10 (C# 14)** services, each its own hexagonal
(Ports & Adapters) solution. The two stream workers compile to **Native AOT** Linux binaries.
All prose, comments, and log messages are in Spanish — match that when editing.

## Build & run

There is **no root solution**. Each service builds from its own `.slnx`:

```powershell
dotnet build emisor_mensaje/KafkaDemoHexagonal.slnx
dotnet build consumer_streams/ConsumerStreams.slnx
dotnet build log_sink/LogSink.slnx
dotnet build documentdb_emulator/DocumentDbEmulator.csproj
```

Full stack (Kafka KRaft broker + all services + UIs):

```powershell
docker compose up -d --build
docker compose up -d --scale kafka-consumer-streams=2 --scale kafka-log-sink=2   # active/active scaling
```

Run one service against a local broker (`localhost:9092`, from `appsettings.json`):

```powershell
dotnet run --project emisor_mensaje/src/KafkaDemo.Web           # dashboard on http://localhost:5000
dotnet run --project consumer_streams/src/ConsumerStreams.Worker
dotnet run --project log_sink/src/LogSink.Worker
```

Native AOT publish (as the Dockerfiles do; needs `clang zlib1g-dev libssl-dev` on Linux):

```powershell
dotnet publish consumer_streams/src/ConsumerStreams.Worker -c Release -r linux-x64 /p:PublishAot=true
```

Requires the **.NET 10 SDK** (repo built with 10.0.203).

## Tests

There is **no unit/xUnit project**. Verification is done with Python 3 scripts in `data_guia/`
run against the **live docker stack**, from the repo root:

```powershell
python data_guia/test_trace_flow.py        # emit one GET trace -> assert it lands in Cosmos DB
python data_guia/test_multi_publish.py      # publish same event 3x -> assert 3 stored docs
python data_guia/test_swagger_in_protobuf.py
python data_guia/validate_exact_json.py
```

They POST to `http://localhost:5000/api/messages/send` and read back from the Cosmos emulator
at `http://localhost:8081`.

## Service URLs (docker-compose)

| Port | Service |
|---|---|
| 5000 | Emisor web dashboard / REST API (`kafka-web`) |
| 6000, 8080 | Kafka UI (Provectus) |
| 8081 | Cosmos DB emulator REST + Data Explorer UI |
| 8082 | Cosmos DB emulator UI (alt) |
| 3000 | Mongo web GUI (mongoclient) |
| 27017 | Mongo wire engine backing the emulator |
| 8443 | Azure Key Vault emulator (lowkey-vault) |
| 9092 | Kafka broker (KRaft, no ZooKeeper) |

## Architecture — the data flow

```
emisor_mensaje (Web :5000)                consumer_streams (AOT worker)          log_sink (AOT worker)
  build OTel payload                        Protobuf-decode envelope               consume in batches
  OTelTracePruner (GET traces)              resolve Vault key (RAM TTL 1h)         (500 docs / 250 ms)
  AES-256-GCM encrypt                       AES-256-GCM decrypt                    parallel HTTP bulk upsert
  wrap in EncryptedPayloadEnvelope   -->    mask via OpenAPI x-log-data-protection to Cosmos DB audit_logs
  publish Protobuf bytes                    TransactionEnricher (risk/fraud score) manual max-offset commit
        |                                   publish CLEARTEXT JSON  -->                   |
        v                                          |          \                          v
  tp.observability.application-log       .processed.v1 (30p)   .error.v1  <-- poison    .processed.dlq.v1
        .emitted.v1  (40 partitions)                            (EncryptedErrorPayloadEnvelope)
```

- **Envelope** (`*/src/*/Protos/encrypted_envelope.proto`, ns `Produbanco.Security.V1`): self-contained
  AES-256-GCM ciphertext + nonce + auth tag + Vault token id + cert thumbprint + trace metadata +
  `telemetry_type` + `service_name`. `swagger` (the OpenAPI YAML) is the **only optional field**;
  `StreamProcessingPipelineUseCase.ValidateMandatoryEnvelopeFields` rejects anything else missing.
  `encrypted_error_envelope.proto` = same shape + `error_detail`, used only for the `.error.v1` DLQ.
- **The proto files are duplicated** in `emisor_mensaje` and `consumer_streams` (each compiles its own
  via `Grpc.Tools`). Edit every copy together and keep them byte-identical.
- **Crypto key is a deterministic shared seed**, not a real Vault secret:
  `AesKey256 = SHA256("PRODUBANCO-SECRET-KEY-SEED-produbanco-encryption-cert-2026")`, computed
  identically in both `AzureKeyVaultTokenAdapter`s. The lowkey-vault container and
  `DefaultAzureCredential` path exist but the seed is the effective key — changing the seed string
  breaks decryption across services. Each crypto-capable service caches key material in a
  `ConcurrentDictionary` with a 1-hour TTL.
- **Partition keys**: `UniformPartitionKeyGenerator.GenerateDispersedKey` (SplitMix64 avalanche,
  zero-alloc) — also duplicated in `emisor_mensaje` and `consumer_streams` `Domain/Utils`; keep in sync.
  Keys already shaped `PK-…` (len > 20) are passed through verbatim.
- **Topics** are auto-created by the emisor (`GenerateAndSendBatchAsync`, 40 partitions) if absent;
  the broker default is `num.partitions=3`.
- Consumers use **manual offset commit** (`EnableAutoCommit=false`, `EnableAutoOffsetStore=false`),
  committing only after successful processing / persistence. Poison messages are routed to the DLQ
  and their offset **is** committed (poison-pill handling) so a partition never stalls.

## Data-protection / masking (the area under active development)

`consumer_streams` masks the decrypted JSON **only when `telemetry_type == Trace`** and the envelope
carries a `swagger` contract:

- `OpenApiContractCompiler.Compile(yaml)` parses the OpenAPI YAML in one pass into a
  `CompiledContractRules` frozen tree of `x-log-data-protection` rules, keyed by
  `method + route + hierarchical property path` (`parent.property`, operation-scoped — there is
  deliberately **no global property fallback**, see recent commits).
- `ThreadSafeContractRulesCacheAdapter` caches compiled rules by contract hash.
- `JsonStreamDataProtectionMasker.MaskPayload` applies rules over UTF-8 bytes with
  `Utf8JsonReader`/`Utf8JsonWriter` and zero heap allocation. Rule types: `HashSha256`,
  `PartialLast4`, `Remove`, `Full`. With `MaskUrlPathAndQuery` it also masks path params and query
  values inside `url.path` / `url.query` / `http.target` / `url.full`, and recurses into JSON
  embedded as a string in `http.{request,response}.body_preview`.
- Each rule type has a global on/off switch in `DataProtectionRules:*` config.

## Conventions & constraints

- **Native AOT services do all JSON through `System.Text.Json` source generators** —
  `StreamJsonContext` (consumer_streams), `SinkJsonContext` (log_sink). No reflection-based
  serialization; every new DTO must be added to the relevant `[JsonSerializable]` context.
  Worker csprojs set `PublishAot`, `InvariantGlobalization`, and `TrimmerRootAssembly` for
  `Confluent.Kafka` + `Google.Protobuf`. `emisor_mensaje/KafkaDemo.Web` is plain JIT ASP.NET.
- **Config resolution order** (all services, in `Infrastructure/DependencyInjection.cs`):
  hierarchical `Section:Key` → `TECH-INT-…` env var → `TECH_INT_…` env var. Missing essential
  config **throws at startup** (fail-fast). docker-compose passes the double-underscore form
  (`LogSink__BatchSize`, `KafkaStream__SourceTopic`, `Kafka__BootstrapServers`).
- **Governance standards** live in `data_guia/*.md` and are authoritative:
  - Kafka topics: `tp.<domain>.<resource>.<event>.<version>`, lowercase, dot-separated, no
    environment name, functional (never a service/tech name). DLQ: `<…>.dlq.<version>`.
  - Env-var / integration variable names: `<TYPE>-<SCOPE>-<SOURCE>-<RESOURCE>_<ATTR>`, UPPERCASE,
    ArchiMate layer acronyms (`TECH`/`APPL`/`BUSI`), e.g. `TECH-INT-MSG-KAFKA_BROKERS`.
  - Media/image asset names: kebab-case, letters only, ≤30 chars.
- `documentdb_emulator/Program.cs` is a hand-written minimal Cosmos DB REST shim (in-memory store
  + async mirror to real `mongo:6` for the GUI). It is not a real Cosmos DB — only the handful of
  routes the sink and UI use are implemented.
- `emisor_mensaje` also has a `KafkaDemo.ConsoleApp` (in the `.slnx`, its own `Dockerfile`) but it
  is commented out in docker-compose; `KafkaDemo.Web` is the active producer.
- `graphify-out/` is generated codebase-graph output; run `graphify update .` after code changes.
