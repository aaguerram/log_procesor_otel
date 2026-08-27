# 💾 `log_sink` — Sumidero masivo hacia Azure Cosmos DB / DocumentDB

*Worker* final del pipeline de observabilidad. Consume el JSON en claro de `…processed.v1` en
**micro-lotes**, resuelve la colección Cosmos DB destino a partir de las cabeceras y hace
***upsert* masivo en paralelo** contra el endpoint REST del emulador, con una política de
**resiliencia (reintentos + *circuit breaker*)** y **DLQ individual** por documento.

Compila a un **binario Native AOT** para Linux (ELF autónomo, sin JIT): arranque en milisegundos
y contenedor mínimo (`runtime-deps:10.0`).

- **Solución:** `LogSink.slnx` · **.NET 10 (C# 14)** · Arquitectura Hexagonal · **Native AOT**
- **Consume:** `tp.observability.application-log.processed.v1` (JSON en claro; *value* opaco)
- **Persiste en:** emulador `documentdb_emulator` — DB `ProdubancoObservability`, colección por servicio y señal
- **DLQ:** `tp.observability.application-log.processed.dlq.v1` (JSON del documento + cabeceras de error)

> Contexto del pipeline completo y demás servicios: ver el [README raíz](../README.md).
> Quien produce `…processed.v1` es [`consumer_streams`](../consumer_streams/README.md).

---

## 📋 Contenido

1. [Funcionalidades](#-funcionalidades)
2. [Definiciones](#-definiciones)
3. [Arquitectura hexagonal del servicio](#-arquitectura-hexagonal-del-servicio)
4. [Flujo de un lote](#-flujo-de-un-lote)
5. [Micro-batching](#-micro-batching)
6. [Resiliencia: reintentos y circuit breaker](#-resiliencia-reintentos-y-circuit-breaker)
7. [DLQ individual por documento](#-dlq-individual-por-documento)
8. [Protocolo REST de Cosmos DB](#-protocolo-rest-de-cosmos-db)
9. [Escenarios de uso](#-escenarios-de-uso)
10. [Consideraciones de implementación](#-consideraciones-de-implementación)
11. [Configuración](#-configuración)
12. [Cabeceras Kafka](#-cabeceras-kafka)
13. [Compilación, ejecución y pruebas](#-compilación-ejecución-y-pruebas)
14. [Estructura de carpetas](#-estructura-de-carpetas)

---

## ✨ Funcionalidades

| # | Funcionalidad | Detalle |
| :- | :--- | :--- |
| 1 | **Consumo por micro-lotes** | Acumula hasta **500 documentos** o **250 ms** (lo que ocurra primero) antes de persistir (`KafkaBatchConsumerAdapter`). |
| 2 | **Resolución de colección dinámica** | `TargetCollectionResolver`: prioriza la cabecera `x-target-collection`; si falta, compone `{x-service-name «.»→«_»}_{x-telemetry-type}` (p. ej. `Transfer_Mspx_Prometeus_Management_Trace`); si tampoco, cae a `ContainerName`. |
| 3 | **_Upsert_ masivo en paralelo** | Fan-out controlado por `SemaphoreSlim(100)` + `Task.WhenAll`; cada documento es un `POST` REST independiente con `x-ms-documentdb-is-upsert: True`. |
| 4 | **Firma HMAC del token maestro** | `CosmosResourceTokenFactory`: esquema de autorización `type=master&ver=1.0&sig=<HMAC-SHA256>` del protocolo REST de Cosmos DB. |
| 5 | **Resiliencia (Polly.Core 8.7)** | Por documento: **2 reintentos** (retardo constante 1 s) → **circuit breaker** (ratio 0,5 / ventana 10 s / mínimo 4 / apertura 15 s). Timeout HTTP efectivo **3 s**. |
| 6 | **DLQ individual** | El documento que agota los reintentos —o llega con el circuito **abierto**— se enruta **uno a uno** a `…processed.dlq.v1` con cabeceras `x-error-*` / `x-circuit-state`. |
| 7 | **_Commit_ manual de offsets** | `EnableAutoCommit=false`; tras cada lote se confirma el **offset máximo + 1** por `TopicPartition`. Ningún documento bloquea la partición. |
| 8 | **Métricas por lote** | Documentos OK / fallidos / enviados a DLQ, **Request Units (RUs)** agregadas y latencia del *bulk*. Contadores *thread-safe*. |
| 9 | **Paso a través del documento** | El *value* de Kafka se reenvía a Cosmos **exactamente como llegó** (bytes UTF-8). No se deserializa, valida ni transforma. |
| 10 | **Native AOT** | `PublishAot`, `OptimizationPreference=Speed`, `InvariantGlobalization`, `StackTraceSupport=true`; sin reflexión (Polly.Core es AOT-safe, no se usa `System.Text.Json`). |
| 11 | **Caché de credenciales (TTL 1 h)** | `ConcurrentDictionary` en RAM para las credenciales de Cosmos. |
| 12 | **Escalado activo-activo** | Coordinación por *Consumer Group* de Kafka: *N* réplicas se reparten las particiones de `…processed.v1`. |

---

## 📖 Definiciones

| Término | Significado en este servicio |
| :--- | :--- |
| **Micro-batch / micro-lote** | Grupo de hasta `BatchSize` (500) documentos, o los acumulados al vencer la ventana `BatchTimeoutMs` (250 ms) contada desde el **primer** mensaje del lote. |
| **`LogSinkItem`** | Unidad de persistencia: `RawJson` (el documento tal cual), `PartitionKey` (la *key* de Kafka, `PK-<hash>`), `TargetCollection` (resuelta de cabeceras, o `null`). |
| **Colección destino** | Contenedor Cosmos DB. `{x-service-name con «.»→«_»}_{Trace\|Metric\|Log}`. Aísla la auditoría por servicio y por señal. |
| **_Upsert_** | `POST /dbs/{db}/colls/{coll}/docs` con `x-ms-documentdb-is-upsert: True` — inserta o reemplaza según el `id` del documento. |
| **Request Units (RU)** | Unidad de coste de Cosmos DB. Se lee de la cabecera de respuesta `x-ms-request-charge` (el emulador devuelve `1.0`) y se agrega por lote. |
| **`CosmosTransientException`** | Fallo **reintentable**: HTTP `429` (*throttling*) o `5xx`. Cuenta para el *circuit breaker*. |
| **Error no recuperable** | HTTP `4xx` distinto de `429` → `InvalidOperationException`. **No se reintenta**: va directo a la DLQ. |
| **_Circuit breaker_** | Corta el tráfico a Cosmos cuando falla ≥ 50 % de al menos 4 llamadas en 10 s. Mientras está **abierto** (15 s), cada documento se deriva a la DLQ sin tocar Cosmos (`BrokenCircuitException`). |
| **DLQ del sink** | `tp.observability.application-log.processed.dlq.v1`. Recibe el JSON del documento fallido + cabeceras que describen el error. **Sin consumidor** en la demo (inspección manual). |
| ***At-least-once*** | El *commit* ocurre tras procesar el lote. Caída entre persistir y confirmar → el lote se reprocesa (posibles documentos duplicados). |
| **Semilla / Vault** | El endpoint y la *primary key* de Cosmos salen de la configuración (`SinkSettings`); el emulador de Key Vault **no se consulta**. |

---

## 🧱 Arquitectura hexagonal del servicio

```
LogSink.Worker  (Native AOT · Microsoft.NET.Sdk.Worker · Host + BackgroundService)
   │  Program.cs → Host.CreateApplicationBuilder + AddLogSinkInfrastructure + AddHostedService<BulkSinkWorkerService>
   │  BulkSinkWorkerService → arranca el pipeline con BatchSize / BatchTimeout de la config
   ▼
LogSink.Application
   └─ BulkSinkPipelineUseCase   → orquesta: consumir lote ➔ mapear ➔ bulk insert ➔ (commit)
   ▼
LogSink.Domain  (lógica pura + puertos + modelos)
   ├─ Services/TargetCollectionResolver     → decide la colección destino a partir de cabeceras
   ├─ Models/  BulkSinkResult · (KafkaBatchItem, LogSinkItem en Ports)
   ├─ ObservabilityHeaders                  → nombres canónicos de cabeceras x-*
   └─ Ports/   IBatchConsumerPort · IDocumentDbBulkSinkPort · IVaultTokenProviderPort · IDlqProducerPort
   ▲
LogSink.Infrastructure  (adaptadores)
   ├─ Adapters/KafkaBatchConsumerAdapter    (IConsumer<string,byte[]> · buffer 500 / ventana 250 ms · commit manual)
   ├─ Adapters/CosmosDbBulkSinkAdapter      (fan-out SemaphoreSlim(100) · resiliencia · métricas · DLQ por ítem)
   ├─ Adapters/KafkaDlqProducerAdapter      (IProducer<string,string> · Acks=All · idempotente → processed.dlq.v1)
   ├─ Adapters/AzureKeyVaultTokenAdapter    (caché RAM TTL 1 h de CosmosDbCredentials, tomadas de SinkSettings)
   ├─ Cosmos/CosmosDocumentClient           (POST REST /docs · authorization HMAC · clasifica 429/5xx vs 4xx)
   ├─ Cosmos/CosmosResourceTokenFactory     (HMAC-SHA256 del token maestro de Cosmos DB)
   ├─ Cosmos/CosmosDbResiliencePipelineFactory  (Polly.Core: Retry → CircuitBreaker)
   ├─ Messaging/KafkaHeaderMapper           (Headers ⇄ Dictionary<string,string>)
   └─ Configuration/SinkSettings + ConfigReader + DependencyInjection  (settings fail-fast + HttpClient)
```

---

## 🔄 Flujo de un lote

1. **Consumir** — `KafkaBatchConsumerAdapter` hace `Consume(50 ms)` en bucle y acumula en un buffer. Dispara el *handler* cuando `count >= 500` **o** `count > 0 && transcurrido >= 250 ms`.
2. **Mapear** — `BulkSinkPipelineUseCase.MapToSinkItems`: descarta ítems con `RawJson` en blanco; por cada uno resuelve `TargetCollection` (cabeceras) y `PartitionKey` (`item.Key ?? "default"`).
3. **Resolver credenciales** — `AzureKeyVaultTokenAdapter.ResolveCosmosCredentialsAsync(VaultTokenId)` — *cache hit* en RAM o construcción desde `SinkSettings` (TTL 1 h).
4. **_Bulk insert_** — `CosmosDbBulkSinkAdapter`: `SemaphoreSlim(100)` + `Task.WhenAll` sobre `InsertOneAsync`:
   - `resiliencePipeline.ExecuteAsync(() => documentClient.UpsertDocumentAsync(...))` → RUs consumidas → `metrics.RecordSuccess`.
   - Excepción → `metrics.RecordFailure()` + `SendFailedItemToDlqAsync` → si publica, `metrics.RecordDlqSent()`.
5. **Métricas** — `BulkSinkResult(TotalProcessed, TotalSuccessful, TotalFailed, TotalDlqSent, ElapsedMs, RequestUnitsConsumed)` → log `💾 [Bulk Sink Cosmos DB] Persistidos: X/Y | DLQ: Z | RUs: … | Latencia: … ms`.
6. **_Commit_** — `ProcessBatchAsync` devuelve **siempre `true`**; `KafkaBatchConsumerAdapter` confirma el `max(offset) + 1` de cada `TopicPartition` del lote.

---

## 📦 Micro-batching

`KafkaBatchConsumerAdapter.StartBatchConsumerAsync`:

| Parámetro | Valor | Nota |
| :--- | :--- | :--- |
| Disparador por tamaño | `BatchSize` = **500** | `LogSink:BatchSize` |
| Disparador por tiempo | `BatchTimeoutMs` = **250 ms** | se mide con un `Stopwatch` que arranca en el **primer** mensaje del lote |
| Poll interno | `Consume(50 ms)` | no bloquea el bucle |
| `AutoOffsetReset` | `Earliest` | reprocesa desde el último offset confirmado al reiniciar |
| `EnableAutoCommit` / `EnableAutoOffsetStore` | `false` / `false` | *commit* explícito tras persistir |
| `SessionTimeoutMs` / `MaxPollIntervalMs` | 15 000 / 300 000 | tolerancia de *rebalance* y de lote lento |
| *Commit* | `Commit(max(offset)+1 por TopicPartition)` | solo si el *handler* devuelve `true` |

`ConsumeException` se registra y continúa; una excepción inesperada del bucle registra y espera 50 ms.

---

## 🛡️ Resiliencia: reintentos y circuit breaker

`CosmosDbResiliencePipelineFactory.Create` construye un `ResiliencePipeline` (Polly.Core 8.7.0,
sin reflexión) que se aplica **por documento**:

```
Retry(2 intentos · 1 s constante)  ─►  CircuitBreaker(ratio 0.5 · ventana 10 s · mín. 4 · apertura 15 s)  ─►  UpsertDocumentAsync
```

**Excepciones que ambas estrategias manejan:** `HttpRequestException`, `TimeoutException`,
`CosmosTransientException` (429 / 5xx), `TaskCanceledException` (salvo cancelación real del token).

| Evento | Efecto | Log |
| :--- | :--- | :--- |
| Fallo transitorio | reintento tras 1 s (máx. 2) | `⚠️ [RETRY #n]` (Warning) |
| ≥ 50 % de fallos en ≥ 4 llamadas / 10 s | **circuito ABRE 15 s** | `🔴 [CIRCUIT BREAKER OPEN]` (Critical) |
| Circuito abierto | `BrokenCircuitException` inmediata (no toca Cosmos) → **documento a DLQ** con `x-circuit-state: OPEN` | — |
| Tras 15 s | prueba **HALF-OPEN**; éxito → **CLOSED** | `🟡 HALF-OPEN` / `🟢 CLOSED` |
| HTTP `4xx` ≠ `429` | `InvalidOperationException` → **no se reintenta**, va a DLQ | — |

> El *circuit breaker* es una **válvula de alivio**, no una pausa: con el circuito abierto los
> documentos **no se retienen**, se derivan a la DLQ para que el consumo de Kafka no se detenga.

---

## 🚨 DLQ individual por documento

`CosmosDbBulkSinkAdapter.SendFailedItemToDlqAsync` → `KafkaDlqProducerAdapter.SendToDlqAsync`
publica en `…processed.dlq.v1` (`IProducer<string, string>`, `Acks.All`, idempotente):

- **Key:** la *partition key* del documento.
- **Value:** el JSON original del documento, sin modificar.
- **Cabeceras:**

| Cabecera | Valor |
| :--- | :--- |
| `x-error-type` | nombre del tipo de excepción (`CosmosTransientException`, `BrokenCircuitException`, `InvalidOperationException`, …) |
| `x-error-message` | mensaje de la excepción |
| `x-error-timestamp` | ISO-8601 (`TimeProvider`) |
| `x-retry-attempts` | `Resilience:Retry:MaxRetryAttempts` configurado (**valor fijo**, no los intentos reales) |
| `x-target-collection` | colección resuelta, o `ContainerName` |
| `x-circuit-state` | `OPEN` si la causa fue `BrokenCircuitException`; `ACTIVE` en otro caso |
| `x-dlq-origin` | `LogSink.CosmosDbBulkSinkAdapter` |

Si el propio envío a la DLQ falla → `LogCritical` y se cuenta como fallo (no se relanza). El
*commit* del lote ocurre igual: **ningún documento bloquea la partición**.

---

## 🌐 Protocolo REST de Cosmos DB

`CosmosDocumentClient.UpsertDocumentAsync` — un `POST` por documento:

```
POST {endpoint}/dbs/{DatabaseName}/colls/{colección}/docs
  x-ms-date: <RFC1123>
  x-ms-version: 2018-12-31
  x-ms-documentdb-is-upsert: True
  x-ms-documentdb-partitionkey: ["<partitionKey>"]
  authorization: type%3Dmaster%26ver%3D1.0%26sig%3D<HMAC-SHA256(verb\ntype\nlink\ndate\n\n)>
  Content-Type: application/json
  <cuerpo = RawJson en bytes UTF-8>
```

- **Éxito** → devuelve `x-ms-request-charge` (RUs; `1.0` si ausente).
- **`429` o `≥ 500`** → `CosmosTransientException` (reintentable, cuenta para el *breaker*).
- **Otro `4xx`** → `InvalidOperationException` (no recuperable → DLQ).

**`HttpClient`** (`DependencyInjection.CreateCosmosHttpClient`): `SocketsHttpHandler` con
`PooledConnectionLifetime = 15 min`, `MaxConnectionsPerServer = 200`, `ConnectTimeout = 3 s`,
`HttpClient.Timeout = 3 s`, `DefaultRequestVersion = HTTP/1.1` y
`RemoteCertificateValidationCallback` que acepta el certificado autofirmado del emulador.

---

## 🎬 Escenarios de uso

### Escenario 1 — Lote lleno (ráfaga)

- Entran 500 documentos en < 250 ms → *flush* inmediato. `SemaphoreSlim(100)` los drena en ~5 oleadas paralelas. *Commit* del offset máximo.

### Escenario 2 — Goteo (ventana temporal)

- Llegan 12 documentos y luego silencio. A los 250 ms del primero se persiste el lote de 12.

### Escenario 3 — _Throttling_ puntual de Cosmos (429)

- Un documento recibe `429` → reintento a 1 s → `429` de nuevo → segundo reintento → éxito. Se registra `⚠️ [RETRY]`; el documento se persiste; RUs contadas.

### Escenario 4 — Cosmos DB caído

- 4 documentos fallan con `5xx` en 10 s (ratio 1,0 ≥ 0,5) → `🔴 CIRCUIT BREAKER OPEN` 15 s.
- Todos los documentos siguientes → DLQ con `x-error-type: BrokenCircuitException`, `x-circuit-state: OPEN`.
- A los 15 s, HALF-OPEN: si el primer intento va bien → `🟢 CLOSED` y se reanuda la persistencia.

### Escenario 5 — Documento sin `x-target-collection`

- `TargetCollectionResolver` no encuentra `x-service-name` + `x-telemetry-type` → `TargetCollection = null` → se usa `ContainerName` (`audit_logs`).

### Escenario 6 — Aislamiento por servicio y señal

- `x-service-name: Transfer.Mspx.Prometeus.Management` + `x-telemetry-type: Metric` → colección `Transfer_Mspx_Prometeus_Management_Metric` (la crea el emulador al primer *upsert*).

### Escenario 7 — Documento con JSON inválido

- El emulador responde `400` → `InvalidOperationException` → **sin reintento** → DLQ con `x-error-type: InvalidOperationException`, `x-circuit-state: ACTIVE`.

### Escenario 8 — Escalado horizontal

- `docker compose up -d --scale kafka-log-sink=2` (tras quitar `container_name`): las particiones de `…processed.v1` se reparten entre las 2 réplicas del *group* `log-sink-cosmosdb-group-v1`.

### Escenario 9 — Reinicio del _worker_

- `Earliest` + *commit* manual → se reanuda desde el último offset confirmado. Si cayó entre el *bulk insert* y el *commit*, el lote entero se reprocesa (documentos duplicados: el emulador genera clave de almacenamiento única por escritura).

---

## ⚙️ Consideraciones de implementación

- **El documento se reenvía tal cual, sin `System.Text.Json`.** El *value* de Kafka viaja a Cosmos como bytes UTF-8 exactos. `log_sink` no lo deserializa, valida ni transforma; **no existe un `JsonSerializerContext`** en este servicio. Un JSON mal formado lo rechaza el destino (`4xx`) → DLQ.
- **El "Cosmos DB" es el `documentdb_emulator`**: *shim* REST en memoria. La firma `authorization` HMAC se calcula correctamente pero **el emulador no la valida**; el `RemoteCertificateValidationCallback` acepta cualquier certificado.
- **El Vault no se consulta.** `AzureKeyVaultTokenAdapter` construye las `CosmosDbCredentials` desde `SinkSettings` y solo añade una caché con TTL 1 h. `KeyVaultEndpoint` / `VaultTokenId` son etiquetas.
- **`at-least-once`, no `exactly-once`.** `ProcessBatchAsync` devuelve `true` **siempre** —incluso si todos los documentos fueron a la DLQ—, así que el offset avanza. Caída entre persistir y confirmar → lote reprocesado → duplicados aguas abajo.
- **La DLQ garantiza el avance de la partición.** Cada documento se persiste o se deriva; nunca detiene el lote.
- **El _circuit breaker_ produce DLQ masiva, no *backpressure*.** Con el circuito abierto no se retiene nada: cada documento va a la DLQ con `x-circuit-state: OPEN`.
- **El _retry_ NO cubre `4xx`.** Solo `429` / `5xx` (`CosmosTransientException`), fallos de red y *timeouts*. Un `4xx` distinto de `429` va directo a la DLQ.
- **Paralelismo por lote acotado a 100** (`SemaphoreSlim`). Con 500 documentos, ~5 oleadas.
- **`x-retry-attempts` en la DLQ es el valor configurado (`2`), no los intentos reales** de ese documento.
- **La ventana de 250 ms se mide desde el primer mensaje del lote**, no es un *tick* fijo.
- **`container_name` en `docker-compose` bloquea `docker compose --scale`.** Hay que quitar esa línea del servicio `kafka-log-sink` para escalar.
- **Native AOT**: `OptimizationPreference=Speed`; `StackTraceSupport=true` (se conserva el *stack trace* para poder poblar `x-error-message`); `InvariantGlobalization`; `Confluent.Kafka` como `TrimmerRootAssembly`; los proyectos `Domain`/`Application`/`Infrastructure` marcan `IsAotCompatible`. Polly.Core 8 es AOT-safe. Contenedor final: `runtime-deps:10.0`, `DOTNET_EnableDiagnostics=0`, *entrypoint* `./LogSink.Worker`.
- **`config` *fail-fast*:** `BootstrapServers`, `SourceTopic`, `GroupId`, `CosmosEndpoint`, `DatabaseName`, `ContainerName`, `KeyVaultEndpoint` y `VaultTokenId` ausentes lanzan al arranque.
- **`log_sink` no crea tópicos.** `…processed.v1` y `…processed.dlq.v1` se autocrean con el valor por defecto del broker (`num.partitions=3`) si no existen.

---

## 🔧 Configuración

Orden de resolución (`ConfigReader`): `Section:Key` → `TECH-INT-…` → `TECH_INT_…`.
`docker-compose` pasa la forma con doble guion bajo (`LogSink__…`).

### Kafka

| `Section:Key` | Env (docker-compose) | Alterno `TECH-…` | Defecto | Uso |
| :--- | :--- | :--- | :--- | :--- |
| `LogSink:BootstrapServers` | `LogSink__BootstrapServers` | `TECH-INT-MSG-KAFKA_BROKERS` | — *(obligatorio)* | Brokers |
| `LogSink:SourceTopic` | `LogSink__SourceTopic` | `TECH-INT-MSG-LOGS_TOPIC` | — *(obligatorio)* | Tópico de entrada (`…processed.v1`) |
| `LogSink:GroupId` | `LogSink__GroupId` | `TECH-INT-MSG-LOGS_GROUP` | — *(obligatorio)* | Consumer group |
| `LogSink:DlqTopic` | `LogSink__DlqTopic` | `TECH-INT-MSG-DLQ_TOPIC` | `tp.observability.application-log.processed.dlq.v1` | DLQ |
| `LogSink:BatchSize` | `LogSink__BatchSize` | `TECH-INT-DB-BATCH_SIZE` | `500` | Documentos por lote |
| `LogSink:BatchTimeoutMs` | `LogSink__BatchTimeoutMs` | `TECH-INT-DB-BATCH_TIMEOUT_MS` | `250` | Ventana de acumulación |

### Cosmos DB

| `Section:Key` | Env (docker-compose) | Alterno `TECH-…` | Defecto | Uso |
| :--- | :--- | :--- | :--- | :--- |
| `LogSink:CosmosEndpoint` | `LogSink__CosmosEndpoint` | `TECH-INT-DB-AUDI_URL` | — *(obligatorio)* | `http://azure-documentdb:8081` |
| `LogSink:CosmosPrimaryKey` | `LogSink__CosmosPrimaryKey` | `TECH-INT-DB-AUDI_KEY` | `""` | *Master key* base64 (firma HMAC) |
| `LogSink:DatabaseName` | `LogSink__DatabaseName` | `TECH-INT-DB-AUDI_NAME` | — *(obligatorio)* | `ProdubancoObservability` |
| `LogSink:ContainerName` | `LogSink__ContainerName` | `TECH-INT-DB-AUDI_COLL` | — *(obligatorio)* | `audit_logs` (*fallback* si no hay `x-target-collection`) |
| `LogSink:PartitionKeyPath` | `LogSink__PartitionKeyPath` | `TECH-INT-DB-AUDI_PK_PATH` | `/partitionKey` | Ruta de la *partition key* |
| `LogSink:CosmosTimeoutSeconds` | `LogSink__CosmosTimeoutSeconds` | `COSMOS_TIMEOUT_SECONDS` | `3` | `ConnectTimeout` + `HttpClient.Timeout` |

### Azure Key Vault (etiquetas; no se consulta)

| `Section:Key` | Env (docker-compose) | Alterno `TECH-…` | Defecto |
| :--- | :--- | :--- | :--- |
| `LogSink:KeyVaultEndpoint` | `LogSink__KeyVaultEndpoint` | `TECH-INT-SECU-VAULT_URL` | — *(obligatorio)* |
| `LogSink:VaultTokenId` | `LogSink__VaultTokenId` | `TECH-INT-SECU-TOKEN_ID` | — *(obligatorio)* (`TKN-COSMOS-PRODUBANCO-V1`) |

### Resiliencia (Polly.Core)

| `Section:Key` | Env (docker-compose) | Defecto |
| :--- | :--- | :--- |
| `LogSink:Resilience:Retry:MaxRetryAttempts` | `LogSink__Resilience__Retry__MaxRetryAttempts` | `2` |
| `LogSink:Resilience:Retry:DelaySeconds` | `LogSink__Resilience__Retry__DelaySeconds` | `1` |
| `LogSink:Resilience:CircuitBreaker:FailureRatio` | `LogSink__Resilience__CircuitBreaker__FailureRatio` | `0.5` |
| `LogSink:Resilience:CircuitBreaker:SamplingDurationSeconds` | `…__SamplingDurationSeconds` | `10` |
| `LogSink:Resilience:CircuitBreaker:MinimumThroughput` | `…__MinimumThroughput` | `4` |
| `LogSink:Resilience:CircuitBreaker:BreakDurationSeconds` | `…__BreakDurationSeconds` | `15` |

---

## 📨 Cabeceras Kafka

### Entrada ← `…processed.v1` (se leen)

| Cabecera | Uso |
| :--- | :--- |
| `x-target-collection` | colección destino (prioritaria) |
| `x-service-name` + `x-telemetry-type` | componen la colección si falta `x-target-collection` |

El resto de cabeceras que emite `consumer_streams` (`x-risk-level`, `x-latency-ms`, …) se
ignoran: el documento persistido es el *value*, no las cabeceras.

### Salida → `…processed.dlq.v1` (se escriben)

`x-error-type`, `x-error-message`, `x-error-timestamp`, `x-retry-attempts`,
`x-target-collection`, `x-circuit-state` (`OPEN` / `ACTIVE`), `x-dlq-origin`
(`LogSink.CosmosDbBulkSinkAdapter`).

---

## 🛠️ Compilación, ejecución y pruebas

```powershell
# Compilar (incluye los proyectos de pruebas)
dotnet build log_sink/LogSink.slnx

# Ejecutar contra un broker local (localhost:9092 / Cosmos local, de appsettings.json)
dotnet run --project log_sink/src/LogSink.Worker

# Publicación Native AOT (como el Dockerfile; en Linux requiere clang zlib1g-dev libssl-dev)
dotnet publish log_sink/src/LogSink.Worker -c Release -r linux-x64 /p:PublishAot=true

# Dentro del stack docker
docker compose up -d --build kafka-log-sink

# Pruebas unitarias (xUnit + Moq + Microsoft.Extensions.TimeProvider.Testing) — ~52 pruebas
dotnet test log_sink/LogSink.slnx
```

Las pruebas cubren `TargetCollectionResolver`, `BulkSinkPipelineUseCase`, `ConfigReader`,
`KafkaHeaderMapper`, `CosmosResourceTokenFactory` (firma HMAC), `CosmosDocumentClient`
(clasificación 429/5xx/4xx con `HttpClient` simulado), `AzureKeyVaultTokenAdapter` (expiración
con `FakeTimeProvider`), `CosmosDbBulkSinkAdapter` (fan-out + DLQ) y `BulkSinkResult`.

Requiere el **SDK de .NET 10** (repo construido con 10.0.203).

---

## 📁 Estructura de carpetas

```text
log_sink/
├── Dockerfile                 # 2 etapas: SDK + clang (AOT publish) → runtime-deps:10.0
├── LogSink.slnx
├── src/
│   ├── LogSink.Domain/
│   │   ├── Services/          TargetCollectionResolver
│   │   ├── Models/            BulkSinkResult
│   │   ├── ObservabilityHeaders.cs
│   │   └── Ports/             IBatchConsumerPort (+ KafkaBatchItem, LogSinkItem) · IDocumentDbBulkSinkPort · IVaultTokenProviderPort (+ CosmosDbCredentials) · IDlqProducerPort
│   ├── LogSink.Application/
│   │   └── UseCases/          BulkSinkPipelineUseCase
│   ├── LogSink.Infrastructure/
│   │   ├── Adapters/          KafkaBatchConsumerAdapter · CosmosDbBulkSinkAdapter · KafkaDlqProducerAdapter · AzureKeyVaultTokenAdapter
│   │   ├── Cosmos/            ICosmosDocumentClient · CosmosDocumentClient · ICosmosResourceTokenFactory · CosmosResourceTokenFactory · CosmosDbResiliencePipelineFactory · CosmosTransientException
│   │   ├── Messaging/         KafkaHeaderMapper
│   │   ├── Configuration/     SinkSettings (+ ResilienceSettings) · ConfigReader
│   │   └── DependencyInjection.cs   (settings fail-fast + HttpClient de Cosmos)
│   └── LogSink.Worker/
│       ├── Program.cs         Host + AddHostedService<BulkSinkWorkerService>
│       ├── BulkSinkWorkerService.cs
│       └── appsettings.json
└── tests/                     LogSink.{Domain,Application,Infrastructure}.Tests  (xUnit)
```
