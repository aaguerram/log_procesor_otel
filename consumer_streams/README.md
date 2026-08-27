# ⚡ `consumer_streams` — Procesador de streams: descifrado, enmascarado y scoring

*Worker* central del pipeline de observabilidad. Consume los sobres **Protobuf cifrados** de
`…emitted.v1`, los **descifra** con AES-256-GCM, **enmascara** el JSON según el contrato OpenAPI
del servicio de origen (`x-log-data-protection`), lo **enriquece** con un *score* de riesgo/fraude
y **reenvía el JSON en claro** a `…processed.v1`. Los mensajes envenenados van a una DLQ dedicada.

Compila a un **binario Native AOT** para Linux (ELF autónomo, sin JIT, sin runtime .NET
instalado): arranque en milisegundos y contenedor mínimo (`runtime-deps:10.0`).

- **Solución:** `ConsumerStreams.slnx` · **.NET 10 (C# 14)** · Arquitectura Hexagonal · **Native AOT**
- **Consume:** `tp.observability.application-log.emitted.v1` (bytes Protobuf, `EncryptedPayloadEnvelope`)
- **Produce:** `tp.observability.application-log.processed.v1` (JSON en claro enmascarado)
- **DLQ:** `tp.observability.application-log.error.v1` (`EncryptedErrorPayloadEnvelope`)
- **Consumidor aguas abajo:** `log_sink`

> Contexto del pipeline completo y demás servicios: ver el [README raíz](../README.md).
> El emisor de los sobres es [`emisor_mensaje`](../emisor_mensaje/README.md).

---

## 📋 Contenido

1. [Funcionalidades](#-funcionalidades)
2. [Definiciones](#-definiciones)
3. [Arquitectura hexagonal del servicio](#-arquitectura-hexagonal-del-servicio)
4. [Flujo de procesamiento de un mensaje](#-flujo-de-procesamiento-de-un-mensaje)
5. [Enmascarado por contrato OpenAPI](#-enmascarado-por-contrato-openapi-el-corazón-del-servicio)
6. [Enriquecimiento y scoring de riesgo](#-enriquecimiento-y-scoring-de-riesgo)
7. [Manejo de errores y DLQ](#-manejo-de-errores-y-dlq)
8. [Escenarios de uso](#-escenarios-de-uso)
9. [Consideraciones de implementación](#-consideraciones-de-implementación)
10. [Configuración](#-configuración)
11. [Cabeceras Kafka](#-cabeceras-kafka)
12. [Compilación, ejecución y pruebas](#-compilación-ejecución-y-pruebas)
13. [Estructura de carpetas](#-estructura-de-carpetas)

---

## ✨ Funcionalidades

| # | Funcionalidad | Detalle |
| :- | :--- | :--- |
| 1 | **Consumo binario Protobuf** | `IConsumer<string, byte[]>` sobre `…emitted.v1`; decodifica el `EncryptedPayloadEnvelope` con `Google.Protobuf`. |
| 2 | **Descifrado AES-256-GCM (AES-NI)** | `AesGcm.Decrypt` con `transaction_id` como *Associated Data* y validación del *auth tag* (integridad). Clave resuelta del Vault con caché RAM (TTL 1 h). |
| 3 | **Validación estricta del sobre** | `EnvelopeValidator`: **todos los campos son obligatorios salvo `swagger`**; nonce 12 B, tag 16 B, `telemetry_type` ≠ `UNSPECIFIED`, etc. |
| 4 | **Enmascarado por contrato OpenAPI** | Compila el YAML `swagger` a un árbol congelado de reglas `x-log-data-protection` (`HashSha256`, `PartialLast4`, `Remove`, `Full`) y las aplica sobre bytes UTF-8 con **cero asignaciones de heap**. |
| 5 | **Enmascarado de parámetros de URL** | Con `MaskUrlPathAndQuery`, enmascara *path params* y valores de *query string* dentro de `url.path` / `url.query` / `http.target` / `url.full`, alineando la ruta real contra la plantilla compilada. |
| 6 | **Recursión en JSON embebido** | Si `http.{request,response}.body_preview` contiene JSON serializado como string, recurre y aplica las mismas reglas dentro. |
| 7 | **Caché de contratos con TTL deslizante** | `ThreadSafeContractRulesCacheAdapter`: `FrozenDictionary` por *fingerprint* SHA-256 del YAML, desalojo tras **10 min** de inactividad, evicción cada 1 min. |
| 8 | **Enriquecimiento y scoring** | `TransactionEnricher`: `FraudScore` (0-100), `RiskLevel` (LOW/MEDIUM/HIGH), `ProcessedStatus`, latencia de procesamiento y preservación de todas las etiquetas OTel como `otel.*`. |
| 9 | **Reenvío del JSON en claro** | Publica en `…processed.v1` **el JSON descifrado y enmascarado** (no un DTO); el resultado del scoring viaja en cabeceras. |
| 10 | **Colección Cosmos destino** | `TargetCollectionResolver`: cabecera `x-target-collection` = `{service_name «.»→«_»}_{Trace\|Metric\|Log}`; `log_sink` la usa para aislar por servicio y señal. |
| 11 | **DLQ / *poison-pill handling*** | Cualquier fallo (descifrado, JSON, validación) → `EncryptedErrorPayloadEnvelope` con `error_detail` a `…error.v1`, **confirmando el offset** para que la partición no se detenga. |
| 12 | **Offsets con *commit* manual** | `EnableAutoCommit=false`, `EnableAutoOffsetStore=false`; `Commit(consumeResult)` solo tras reenvío/enrutado exitoso (*at-least-once*). |
| 13 | **Compatibilidad legacy** | Los bytes que no son Protobuf se tratan como texto plano (traza JSON heredada) y se reenvían sin descifrar ni enmascarar. |
| 14 | **Native AOT** | `PublishAot`, `InvariantGlobalization`, `StripSymbols`; JSON 100 % *source-generated* (`StreamJsonContext`), sin reflexión. |
| 15 | **Escalado activo-activo** | Coordinación por *Consumer Group* de Kafka: *N* réplicas se reparten las particiones de `…emitted.v1` sin duplicar trabajo. |

---

## 📖 Definiciones

| Término | Significado en este servicio |
| :--- | :--- |
| **`EncryptedPayloadEnvelope`** | Sobre Protobuf de entrada (`Produbanco.Security.V1`): ciphertext (`data`), AEAD (`nonce`, `auth_tag`, `algorithm_version`), identificación de clave (`cert_thumbprint`, `vault_token_id`), trazabilidad (`transaction_id`, `timestamp_unix_ms`), `telemetry_type`, `service_name` y —único opcional— `swagger`. |
| **`EncryptedErrorPayloadEnvelope`** | Mismo sobre + campo `error_detail` (tipo + mensaje + *stack trace*). Se publica **solo** en `…error.v1`. Proto propio de este servicio. |
| **`x-log-data-protection`** | Directiva institucional en el contrato OpenAPI. Sintaxis: `@Log.Hash(SHA256)`, `@Log.Partial(LAST_4)`, `@Log.Remove`, `@Log.Full`. |
| **Regla de protección (`DataProtectionRuleType`)** | `Full` (0, intacto), `HashSha256` (1), `PartialLast4` (2), `Remove` (3). Cada tipo tiene un interruptor global en `DataProtectionRules:*`. |
| **`CompiledContractRules`** | Árbol inmutable (`FrozenDictionary`) producido por `OpenApiContractCompiler`: `"{MÉTODO} {ruta}"` → `"{ruta jerárquica de propiedad}"` → regla, más metadatos de *path/query params*. |
| **Ruta jerárquica de propiedad** | `padre.propiedad` (p. ej. `clientePrincipal.idCliente`). El compilador indexa por esta ruta con **alcance por operación**; **no hay *fallback* de propiedad global** (evita falsos positivos entre operaciones). |
| **Alcance por operación** | Una regla solo aplica dentro de su `MÉTODO /ruta`. Si no se puede extraer método/ruta del JSON, la regla efectiva es `Full` (nada se enmascara). |
| **Enriquecimiento** | Cálculo de `FraudScore` / `RiskLevel` / latencia. El `ProcessedTransactionEvent` resultante **no se serializa al tópico**: alimenta la clave de partición y las cabeceras `x-risk-level` / `x-processed-status` / `x-latency-ms`. |
| **Semilla determinista** | `SHA256("PRODUBANCO-SECRET-KEY-SEED-produbanco-encryption-cert-2026")` — la clave AES-256 efectiva, idéntica a la del emisor. `DeterministicSeedAesKeyMaterialFactory`. |
| **Colección destino** | `{service_name con «.»→«_»}_{Trace\|Metric\|Log}`, p. ej. `Transfer_Mspx_Prometeus_Management_Trace`. |
| ***Poison pill*** | Mensaje que no se puede procesar (sobre corrupto, *auth tag* inválido, campo obligatorio ausente, JSON no parseable). Se deriva a `…error.v1` y su offset **se confirma**. |
| ***At-least-once*** | El *commit* ocurre tras el reenvío. Si el proceso muere entre reenviar y confirmar, el mensaje se reprocesa → posible duplicado aguas abajo. |

---

## 🧱 Arquitectura hexagonal del servicio

```
ConsumerStreams.Worker  (Native AOT · Host + BackgroundService)
   │  Program.cs → Host.CreateApplicationBuilder + AddConsumerStreamsInfrastructure + AddHostedService<StreamWorkerService>
   │  StreamWorkerService → bucle que ejecuta el pipeline; reintenta cada 5 s si lanza
   ▼
ConsumerStreams.Application
   ├─ StreamProcessingPipelineUseCase   → orquesta consumo ➔ descifrado ➔ enmascarado ➔ enriquecido ➔ reenvío / DLQ
   ├─ Services/TransactionEnricher       → scoring de riesgo/fraude (ITransactionTransformerPort)
   └─ Serialization/StreamJsonContext    → JSON source-generated (AOT): RawTransactionEvent, ProcessedTransactionEvent
   ▼
ConsumerStreams.Domain  (lógica pura, puertos, .proto)
   ├─ Security/    EnvelopeParser · EnvelopeValidator · DlqEnvelopeFactory
   ├─ DataProtection/ PayloadMaskingService (decide si enmascarar)
   ├─ Utils/       OpenApiContractCompiler · JsonStreamDataProtectionMasker · UniformPartitionKeyGenerator
   ├─ Observability/ TelemetryTypeMapper · TargetCollectionResolver · StreamHeaderFactory · StreamHeaders
   ├─ Models/      RawTransactionEvent · ProcessedTransactionEvent · CompiledContractRules · DataProtectionRuleType
   ├─ Contracts/   IContractCompiler → OpenApiContractCompilerAdapter
   └─ Protos/      encrypted_envelope.proto  +  encrypted_error_envelope.proto
   ▲
ConsumerStreams.Infrastructure  (adaptadores)
   ├─ KafkaStreamConsumerAdapter      (IConsumer<string,byte[]> · commit manual · loop resiliente)
   ├─ KafkaStreamProducerAdapter      (IProducer<string,string> · Acks=All · idempotente → processed.v1)
   ├─ KafkaDlqProducerAdapter         (IProducer<string,byte[]> · EncryptedErrorPayloadEnvelope → error.v1)
   ├─ AesGcmPayloadCryptoAdapter      (solo Decrypt · AES-NI · AOT-safe)
   ├─ AzureKeyVaultTokenAdapter       (caché RAM TTL 1 h; deriva vía IAesKeyMaterialFactory)
   ├─ Security/DeterministicSeedAesKeyMaterialFactory  (semilla compartida con el emisor)
   ├─ ThreadSafeContractRulesCacheAdapter  (FrozenDictionary por fingerprint · TTL deslizante 10 min)
   ├─ Messaging/KafkaHeaderMapper     (Headers ⇄ Dictionary<string,string>)
   └─ Configuration/ConfigReader + DependencyInjection  (settings fail-fast)
```

---

## 🔄 Flujo de procesamiento de un mensaje

`StreamProcessingPipelineUseCase.ProcessMessageAsync` (por cada mensaje de `…emitted.v1`):

1. **Decodificar** — `EnvelopeParser.TryParse(rawBytes)` → `EncryptedPayloadEnvelope?` (`false` sin lanzar si no es Protobuf).
2. **Descifrar + enmascarar** — `DecryptAndMaskAsync`:
   - **Sin sobre** (bytes no-Protobuf) → se toma el texto UTF-8 tal cual, con `service_name = Transfer.Mspx.Prometeus.Management`, etiqueta `Trace`, sin descifrar ni enmascarar.
   - **Con sobre:**
     1. `EnvelopeValidator.Validate(envelope)` — lanza si falta cualquier campo obligatorio.
     2. `vaultTokenPort.ResolveKeyByTokenAsync(vault_token_id, cert_thumbprint)` — *cache hit* en RAM o derivación de la semilla (TTL 1 h).
     3. `cryptoPort.DecryptEnvelopeToJson(envelope, keyMaterial)` — AES-256-GCM, AAD = `transaction_id`; **lanza si el *auth tag* no valida**.
     4. `maskingService.ApplyIfApplicable(envelope, json)` — enmascara **solo si** `telemetry_type == Trace` **y** `swagger` no vacío **y** `DataProtectionRules:Enabled`.
3. **Enriquecer** — `EnrichPayload`:
   - Deserializa el JSON a `RawTransactionEvent` con `StreamJsonContext` (*source-gen*); si el resultado es `null` → lanza → DLQ.
   - `transformer.TransformAndEnrich(raw with { RawPayloadJson = json })` → `ProcessedTransactionEvent` (score, riesgo, latencia, *defaults* OTel).
4. **Clave de partición** — `UniformPartitionKeyGenerator.GenerateDispersedKey(processedEvent.OriginAccount ?? key)` → `PK-<hash 16 hex>`.
5. **Cabeceras de salida** — `StreamHeaderFactory.ForProcessedEvent(...)`: copia las entrantes y añade `x-service-name`, `x-telemetry-type`, `x-target-collection`, `x-risk-level`, `x-processed-status`, `x-latency-ms`, `x-stream-processor`, `x-decryption-algorithm`, `x-vault-token`.
6. **Reenviar** — `producerPort.ForwardEventAsync(targetTopic, partitionKey, decoded.Json, headers)` → **el JSON descifrado y enmascarado** se publica en `…processed.v1`.
7. **Confirmar** — si el reenvío devuelve `true`, `KafkaStreamConsumerAdapter` hace `Commit(consumeResult)`.
8. **En caso de excepción en cualquier paso** → `RouteToErrorTopicAsync` (ver [Manejo de errores](#-manejo-de-errores-y-dlq)) y **se devuelve `true`** para confirmar el offset (la partición sigue).

---

## 🛡️ Enmascarado por contrato OpenAPI (el corazón del servicio)

Solo se enmascara cuando `telemetry_type == Trace` **y** el sobre trae `swagger`
(`PayloadMaskingService.ShouldMask`). Métricas y logs pasan intactos aunque el contrato viaje.

### 1 · Compilación del contrato — `OpenApiContractCompiler.Compile(yaml)`

Un solo pase línea a línea del YAML (sin parser YAML completo):

- Extrae `title` y `version` por *regex* → `ContractKey = "{title}:{version}:{sha256[:16] hex}"`.
- Detecta **paths** (`  /contacts/...:`), **métodos** (`    get:` / `post:` / `put:` / `delete:` / `patch:`), `in: path` / `in: query`, `$ref:` y nombres de parámetro/propiedad (`- name:`, `clave:`).
- Al encontrar `x-log-data-protection: '@Log.Hash(SHA256)'` la asocia a la propiedad pendiente dentro de la operación `"{MÉTODO} {ruta}"`.
- `ParseRuleType`: contiene `Hash` → `HashSha256`; `Partial` → `PartialLast4`; `Remove` → `Remove`; en otro caso `Full`.
- **Resuelve `$ref` recursivamente**: vincula las propiedades de los *schemas* de `components/` referenciados por cada operación (para todos los métodos HTTP).
- Normaliza rutas: `/x/{idClient:int}/{channel}` → `/x/{idClient}/{channel}`.
- Salida: `FrozenDictionary` (lecturas concurrentes sin *locks*), más `RouteParameterRules` (índices de segmento de *path params* y reglas de *query params* por endpoint).

### 2 · Caché — `ThreadSafeContractRulesCacheAdapter`

| Aspecto | Valor |
| :--- | :--- |
| Clave | *fingerprint* SHA-256 (16 bytes hex) del YAML |
| Expiración | **TTL deslizante de 10 minutos** (`Touch` en cada acceso) |
| Barrido | timer cada 1 minuto (`TimeProvider.CreateTimer`) |
| Reloj | `TimeProvider` inyectado (expiración verificable en pruebas) |

### 3 · Aplicación — `JsonStreamDataProtectionMasker.MaskPayload`

1. **Pase preliminar** `ExtractMetadata`: obtiene `method` y `route` de `http.request.method` + `http.route`, o del tag `Name` (`"GET /ruta"`). Si no hay → `method = "GET"`, `route = ""` → nada se enmascara.
2. **Pase recursivo** `MaskAndCopy` (`Utf8JsonReader` → `Utf8JsonWriter`, 0 asignaciones), rastreando la ruta `padre.propiedad`:

| Situación | Acción |
| :--- | :--- |
| `rules.GetRule(method, route, "padre.prop", "prop")` → `HashSha256` | SHA-256 hex minúsculas (`stackalloc`) |
| → `PartialLast4` | `****` + últimos 4 caracteres |
| → `Remove` (y `settings.Remove`) | se **omite** la propiedad y su valor completo |
| → `Full` (o regla desactivada) | se copia intacto |
| Propiedad `http.{request,response}.body_preview` con string `{…}` / `[…]` | recursión: se enmascara el JSON embebido con las mismas reglas |
| `url.path` / `http.target` / `url.full` / `http.url` (con `MaskUrlPathAndQuery`) | separa `path?query`; enmascara los segmentos que son *path params* alineando la ruta real contra la plantilla, y los pares de la *query* según `QueryParamRules` |
| `url.query` / `http.query` | enmascara los pares `clave=valor` de la *query string* |

**Prioridad de coincidencia** (`CompiledContractRules.GetRule`): ruta jerárquica exacta (`clientePrincipal.idCliente`) → nombre simple (`idCliente`) → `Full`.

---

## 📊 Enriquecimiento y scoring de riesgo

`TransactionEnricher.TransformAndEnrich` (reloj inyectado como `TimeProvider`):

| Salida | Cómo se calcula |
| :--- | :--- |
| `ProcessingLatencyMs` | `DurationMs` si viene y es > 0; si no, `now − EmittedAt` (mínimo 0,1 ms) |
| `FraudScore` (0-100) | base **10**; `+50` si `Amount > 1500`, `+25` si `> 500`; `+10` si canal `ATM`/`MOBILE_APP`; `+15` si tipo `WITHDRAWAL`/`QR_PAYMENT`; `Clamp(0,100)` |
| `RiskLevel` | `≥ 60` → `HIGH` · `≥ 30` → `MEDIUM` · resto → `LOW` |
| `ProcessedStatus` | `HIGH` → `FLAGGED_FOR_AUDIT` · resto → `VERIFIED_AND_AUDITED` |
| `AuditMetadata` | `processor.engine/runtime/node`, `audit.score`, `audit.risk`, y **todas** las etiquetas OTel como `otel.<clave>` |
| *Defaults* OTel | sin `TransactionId` → usa `TraceId`; canal ← `url.path` o `"OTEL_TRACE"`; tipo ← `Name` o `"OBSERVABILITY_LOG"`; `OriginAccount` ← `TRACE-<traceId[:8]>` |

> ⚠️ **El `ProcessedTransactionEvent` NO se publica en `…processed.v1`.** El *value* del mensaje
> es el JSON original enmascarado. Del scoring solo salen del servicio `x-risk-level`,
> `x-processed-status` y `x-latency-ms` (cabeceras); el `FraudScore` numérico solo se registra en
> el log del *worker*.

---

## 🚨 Manejo de errores y DLQ

`RouteToErrorTopicAsync` se dispara ante **cualquier** excepción del pipeline:

1. `DlqEnvelopeFactory.Create(envelopeOriginal, rawBytes, key, failure, now, "ERR-<guid>")`:
   - Reutiliza los metadatos del sobre original si existen; si el mensaje no era un sobre válido, rellena valores seguros (`NONE`, nonce/tag en ceros, `TelemetryType.Log`, `Unknown.Service`).
   - `error_detail` = `"{FullName}: {Message}\n{StackTrace}"`.
2. Cabeceras `StreamHeaderFactory.ForError`: `x-error-type`, `x-error-message`, `x-error-timestamp` (ISO-8601), `x-source-topic`, `x-error-handler = ConsumerStreams.DLQ` (+ las entrantes).
3. `KafkaDlqProducerAdapter.PublishErrorEnvelopeAsync` publica el `EncryptedErrorPayloadEnvelope` binario en `…error.v1`.
4. **El offset del mensaje original se confirma** (retorno `true`) → *poison-pill handling*: la partición nunca se atasca.
5. Si el propio envío a la DLQ falla → `LogCritical` y se traga la excepción (no se relanza).

**Causas típicas de DLQ:** *auth tag* inválido (mensaje alterado o clave incorrecta), sobre con
un campo obligatorio ausente, `data` que descifra a un JSON no parseable como `RawTransactionEvent`.

---

## 🎬 Escenarios de uso

### Escenario 1 — Traza `GET` con contrato (se descifra y enmascara)

- Entra un sobre con `telemetry_type = TRACE` y el YAML de `Transfer.Mspx.Prometeus.Management`.
- Se descifra; `MaskPayload` extrae `GET` + `/…/contacts-by-idClient/{idClient}/{channel}`.
- Aplica: `@Log.Hash(SHA256)` sobre `idClient` en `url.path`, hash de cédulas y *partial last 4* de cuentas dentro del array de `http.response.body_preview`.
- Reenvía el JSON enmascarado a `…processed.v1` con `x-target-collection: Transfer_Mspx_Prometeus_Management_Trace`.

### Escenario 2 — Traza `POST` de creación de contacto

- El compilador resolvió el `$ref` del *requestBody* de la operación `POST /contacts/local-contact`.
- Se enmascaran las propiedades del *schema* (cédula → hash, cuenta destino → últimos 4) tanto en el body como en `http.request.body_preview`.

### Escenario 3 — Métrica del runtime .NET

- Sobre con `telemetry_type = METRIC`. `ShouldMask` → `false` (no es `Trace`): **el JSON pasa intacto**.
- Se enriquece con *defaults* (`OBSERVABILITY_LOG`), se reenvía a `…processed.v1` con `x-target-collection: …_Metric`.

### Escenario 4 — Log de aplicación

- `telemetry_type = LOG`. Igual que la métrica: sin enmascarar, colección `…_Log`.

### Escenario 5 — Mensaje envenenado

- Un sobre cuyo `data` fue alterado en tránsito → `AesGcm.Decrypt` lanza (*auth tag*).
- Va a `…error.v1` como `EncryptedErrorPayloadEnvelope` con `error_detail`; el offset en `…emitted.v1` **se confirma**.

### Escenario 6 — Traza JSON legacy (sin Protobuf)

- Bytes que no parsean como `EncryptedPayloadEnvelope` → se tratan como texto plano, se enriquecen y se reenvían **sin descifrar ni enmascarar** (compatibilidad hacia atrás). No van a la DLQ.

### Escenario 7 — Rotación del contrato OpenAPI

- El emisor empieza a mandar un YAML nuevo → *fingerprint* distinto → se compila y cachea.
- El contrato anterior se **desaloja tras 10 min** sin usarse.

### Escenario 8 — Escalado horizontal

- `docker compose up -d --scale kafka-consumer-streams=2` (tras quitar `container_name` del `docker-compose.yml`): las particiones de `…emitted.v1` se reparten entre las 2 réplicas del *group* `consumer-streams-produbanco-v1`.

### Escenario 9 — Reinicio del *worker*

- `AutoOffsetReset = Earliest` + *commit* manual → se reanuda desde el último offset confirmado (*at-least-once*; puede reprocesar el mensaje en vuelo si cayó entre reenviar y confirmar).

---

## ⚙️ Consideraciones de implementación

- **Native AOT, sin reflexión JSON.** `PublishAot`, `InvariantGlobalization`, `StripSymbols`; `Confluent.Kafka` y `Google.Protobuf` como `TrimmerRootAssembly`. Todo DTO nuevo debe añadirse a `StreamJsonContext` (`[JsonSerializable]`) o fallará en *runtime*. Contenedor final: `runtime-deps:10.0`, `DOTNET_EnableDiagnostics=0`, *entrypoint* `./ConsumerStreams.Worker` (ELF nativo).
- **El cuerpo reenviado es el JSON original enmascarado**, no un `ProcessedTransactionEvent`. El scoring es informativo: solo `x-risk-level` / `x-processed-status` / `x-latency-ms` salen en cabeceras. `log_sink` persiste el body tal cual.
- **`at-least-once`, no `exactly-once`.** El `Commit` ocurre tras el reenvío. Caída entre reenviar y confirmar → el mensaje se reprocesa y `log_sink` puede crear un documento duplicado (el emulador Cosmos genera clave de almacenamiento única por escritura).
- **Sin *fallback* de propiedad global** en el compilador de contratos: una regla `x-log-data-protection` solo aplica dentro de su operación `MÉTODO /ruta`. Es deliberado (evita enmascarar de más entre endpoints con nombres de campo iguales).
- **Extracción de método/ruta *best-effort*.** Si la traza no trae `http.request.method` + `http.route` ni un `Name` con formato `"VERBO /ruta"`, se asume `GET` + ruta vacía y **nada se enmascara** (regla `Full`).
- **Métricas y logs nunca se enmascaran**, aunque el sobre incluya el contrato.
- **Los `.proto` están duplicados.** `Protos/encrypted_envelope.proto` debe ser **byte a byte idéntico** al de `emisor_mensaje`. `encrypted_error_envelope.proto` solo existe aquí.
- **La clave AES es una semilla determinista.** `DeterministicSeedAesKeyMaterialFactory` la deriva de `SHA256("PRODUBANCO-SECRET-KEY-SEED-produbanco-encryption-cert-2026")`. `AzureKeyVaultTokenAdapter` **no lee configuración** — las variables `KeyVault__VaultUri` / `KeyVault__VaultName` del `docker-compose` no se usan; lowkey-vault no se consulta.
- **Dos cachés con TTL distinto:** material criptográfico (`ConcurrentDictionary`, TTL fijo **1 h**) y contratos compilados (TTL deslizante **10 min**).
- **Un solo hilo de consumo por instancia.** El bucle `while` procesa un mensaje a la vez (`await` por mensaje). El paralelismo real proviene de escalar réplicas / particiones.
- **Bucle resiliente en dos niveles:** `KafkaStreamConsumerAdapter` traga `ConsumeException` con espera de 1 s; `StreamWorkerService` reejecuta el pipeline completo tras 5 s si `ExecutePipelineAsync` lanza.
- **`EnrichPayload` deserializa *todo* como `RawTransactionEvent`** (campos opcionales). Una métrica o un log se mapean a un evento casi vacío y el *enricher* rellena valores por defecto.
- **`config` *fail-fast*:** `BootstrapServers`, `GroupId`, `SourceTopic` y `TargetTopic` ausentes lanzan al arranque (`ConfigReader.Required`). El resto tiene valores por defecto.

---

## 🔧 Configuración

Orden de resolución (`ConfigReader` en `Infrastructure`): `Section:Key` → `TECH-INT-…` → `TECH_INT_…`.

### `KafkaStream`

| `Section:Key` | Env (docker-compose) | Alterno `TECH-…` | Defecto | Uso |
| :--- | :--- | :--- | :--- | :--- |
| `KafkaStream:BootstrapServers` | `KafkaStream__BootstrapServers` | `TECH-INT-MSG-KAFKA_BROKERS` | — *(obligatorio)* | Brokers |
| `KafkaStream:GroupId` | `KafkaStream__GroupId` | `TECH-INT-MSG-STREAM_GROUP` | — *(obligatorio)* | Consumer group |
| `KafkaStream:SourceTopic` | `KafkaStream__SourceTopic` | `TECH-INT-MSG-SOURCE_TOPIC` | — *(obligatorio)* | Tópico de entrada |
| `KafkaStream:TargetTopic` | `KafkaStream__TargetTopic` | `TECH-INT-MSG-TARGET_TOPIC` | — *(obligatorio)* | Tópico de salida |
| `KafkaStream:ErrorTopic` | `KafkaStream__ErrorTopic` | — | `tp.observability.application-log.error.v1` | DLQ |
| `KafkaStream:AutoOffsetReset` | `KafkaStream__AutoOffsetReset` | — | `Earliest` | Posición inicial |
| `KafkaStream:EnableAutoCommit` | — | — | `false` | *Commit* manual |
| `KafkaStream:PollTimeoutMs` | — | — | `1000` | Timeout de `Consume` |

### `DataProtectionRules` (interruptores del enmascarado — *activado salvo `false` explícito*)

| `Section:Key` | Env (docker-compose) | Alterno | Defecto | Efecto |
| :--- | :--- | :--- | :--- | :--- |
| `DataProtectionRules:Enabled` | `DataProtectionRules__Enabled` | `DATA_PROTECTION_ENABLED` | `true` | Motor de enmascarado global |
| `DataProtectionRules:HashSha256` | `DataProtectionRules__HashSha256` | `DATA_PROTECTION_HASH_SHA256` | `true` | Regla `@Log.Hash(SHA256)` |
| `DataProtectionRules:PartialLast4` | `DataProtectionRules__PartialLast4` | `DATA_PROTECTION_PARTIAL_LAST4` | `true` | Regla `@Log.Partial(LAST_4)` |
| `DataProtectionRules:Remove` | `DataProtectionRules__Remove` | `DATA_PROTECTION_REMOVE` | `true` | Regla `@Log.Remove` |
| `DataProtectionRules:Full` | `DataProtectionRules__Full` | `DATA_PROTECTION_FULL` | `true` | Regla `@Log.Full` |
| `DataProtectionRules:MaskUrlPathAndQuery` | `DataProtectionRules__MaskUrlPathAndQuery` | `DATA_PROTECTION_MASK_URL` | `true` | Enmascara `url.path` / `url.query` |

> `KeyVault__VaultUri` / `KeyVault__VaultName` aparecen en el `docker-compose` pero **este servicio
> no los lee** (la clave es la semilla determinista).

---

## 📨 Cabeceras Kafka

### Salida → `…processed.v1` (`StreamHeaderFactory.ForProcessedEvent`)

| Cabecera | Valor | Origen |
| :--- | :--- | :--- |
| `x-stream-processor` | `ConsumerStreams.NativeAOT` | constante |
| `x-decryption-algorithm` | `AES-256-GCM` | constante |
| `x-vault-token` | `vault_token_id` o `NONE` | sobre |
| `x-service-name` | p. ej. `Transfer.Mspx.Prometeus.Management` | sobre |
| `x-telemetry-type` | `Trace` / `Metric` / `Log` | `TelemetryTypeMapper` |
| `x-target-collection` | `{Service}_{Signal}` | `TargetCollectionResolver` |
| `x-processed-status` | `VERIFIED_AND_AUDITED` / `FLAGGED_FOR_AUDIT` | `TransactionEnricher` |
| `x-risk-level` | `LOW` / `MEDIUM` / `HIGH` | `TransactionEnricher` |
| `x-latency-ms` | p. ej. `3.14` | `TransactionEnricher` |
| *(entrantes)* | — | se copian todas las del mensaje original |

### Salida → `…error.v1` (`StreamHeaderFactory.ForError`)

`x-error-type`, `x-error-message`, `x-error-timestamp` (ISO-8601), `x-source-topic`
(`tp.observability.application-log.emitted.v1`), `x-error-handler` (`ConsumerStreams.DLQ`), + las entrantes.

---

## 🛠️ Compilación, ejecución y pruebas

```powershell
# Compilar (incluye los proyectos de pruebas)
dotnet build consumer_streams/ConsumerStreams.slnx

# Ejecutar contra un broker local (localhost:9092, de appsettings.json)
dotnet run --project consumer_streams/src/ConsumerStreams.Worker

# Publicación Native AOT (como el Dockerfile; en Linux requiere clang zlib1g-dev libssl-dev)
dotnet publish consumer_streams/src/ConsumerStreams.Worker -c Release -r linux-x64 /p:PublishAot=true

# Dentro del stack docker
docker compose up -d --build kafka-consumer-streams

# Pruebas unitarias (xUnit + Moq + Microsoft.Extensions.TimeProvider.Testing) — ~121 pruebas
dotnet test consumer_streams/ConsumerStreams.slnx
```

Las pruebas cubren el compilador de contratos, el enmascarador de streaming, `PayloadMaskingService`,
`EnvelopeValidator` / `EnvelopeParser` / `DlqEnvelopeFactory`, `TransactionEnricher`,
`StreamProcessingPipelineUseCase`, el descifrado AES-GCM, la caché de contratos (expiración con
`FakeTimeProvider`), `AzureKeyVaultTokenAdapter`, `ConfigReader` y `KafkaHeaderMapper`.

Requiere el **SDK de .NET 10** (repo construido con 10.0.203).

---

## 📁 Estructura de carpetas

```text
consumer_streams/
├── Dockerfile                 # 2 etapas: SDK + clang (AOT publish) → runtime-deps:10.0
├── ConsumerStreams.slnx
├── src/
│   ├── ConsumerStreams.Domain/
│   │   ├── Protos/            encrypted_envelope.proto · encrypted_error_envelope.proto  (→ Grpc.Tools)
│   │   ├── Security/          EnvelopeParser · EnvelopeValidator · DlqEnvelopeFactory
│   │   ├── DataProtection/    PayloadMaskingService
│   │   ├── Utils/             OpenApiContractCompiler · JsonStreamDataProtectionMasker · UniformPartitionKeyGenerator
│   │   ├── Observability/     TelemetryTypeMapper · TargetCollectionResolver · StreamHeaderFactory · StreamHeaders
│   │   ├── Models/            RawTransactionEvent · ProcessedTransactionEvent · DataProtectionModels
│   │   ├── Contracts/         IContractCompiler · OpenApiContractCompilerAdapter
│   │   ├── Configuration/     DataProtectionRulesSettings
│   │   └── Ports/             IStreamConsumerPort · IStreamProducerPort · IDlqProducerPort · ITransactionTransformerPort · ICryptoPorts · IContractRulesCachePort
│   ├── ConsumerStreams.Application/
│   │   ├── UseCases/          StreamProcessingPipelineUseCase
│   │   ├── Services/          TransactionEnricher
│   │   └── Serialization/     StreamJsonContext  (JSON source-generated para AOT)
│   ├── ConsumerStreams.Infrastructure/
│   │   ├── Adapters/          KafkaStream{Consumer,Producer}Adapter · KafkaDlqProducerAdapter · AesGcmPayloadCryptoAdapter · AzureKeyVaultTokenAdapter · ThreadSafeContractRulesCacheAdapter
│   │   ├── Security/          IAesKeyMaterialFactory · DeterministicSeedAesKeyMaterialFactory
│   │   ├── Messaging/         KafkaHeaderMapper
│   │   ├── Configuration/     KafkaStreamSettings · ConfigReader
│   │   └── DependencyInjection.cs
│   └── ConsumerStreams.Worker/
│       ├── Program.cs         Host + AddHostedService<StreamWorkerService>
│       ├── StreamWorkerService.cs
│       └── appsettings.json
└── tests/                     ConsumerStreams.{Domain,Application,Infrastructure}.Tests  (xUnit)
```
