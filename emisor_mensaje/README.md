# 📤 `emisor_mensaje` — Emisor de telemetría y consola de gestión de tópicos

Servicio **productor** del pipeline de observabilidad. Construye señales de OpenTelemetry
(trazas, métricas y logs), las **poda**, las **cifra a nivel de carga útil** con AES-256-GCM,
las envuelve en un **sobre Protobuf autosuficiente** con el contrato OpenAPI del servicio de
origen y las publica en Kafka. Además expone una **consola web + API REST** para administrar
tópicos y observar el clúster.

Es el **único servicio JIT** del repositorio (ASP.NET Core 10, sin Native AOT): es un panel de
control, no tiene requisito de arranque en microsegundos.

- **Proyecto activo:** `KafkaDemo.Web` → `http://localhost:5000`
- **Tópico de salida:** `tp.observability.application-log.emitted.v1` (bytes Protobuf cifrados)
- **Consumidor aguas abajo:** `consumer_streams`
- **Solución:** `KafkaDemoHexagonal.slnx` · **.NET 10 (C# 14)** · Arquitectura Hexagonal

> Contexto del pipeline completo, tópicos y demás servicios: ver el [README raíz](../README.md).

---

## 📋 Contenido

1. [Funcionalidades](#-funcionalidades)
2. [Definiciones](#-definiciones)
3. [Arquitectura hexagonal del servicio](#-arquitectura-hexagonal-del-servicio)
4. [Flujo de publicación de un evento](#-flujo-de-publicación-de-un-evento)
5. [OTelTracePruner — podado de trazas GET](#-oteltracepruner--podado-de-trazas-get)
6. [Escenarios de uso](#-escenarios-de-uso)
7. [Consideraciones de implementación](#-consideraciones-de-implementación)
8. [Configuración](#-configuración)
9. [API REST](#-api-rest)
10. [Compilación y ejecución](#-compilación-y-ejecución)
11. [Estructura de carpetas](#-estructura-de-carpetas)

---

## ✨ Funcionalidades

| # | Funcionalidad | Detalle |
| :- | :--- | :--- |
| 1 | **Emisión de 3 señales OpenTelemetry** | Selector `Trace` / `Metric` / `Log` en el dashboard; el tipo viaja en el campo `telemetry_type` del Protobuf. |
| 2 | **Catálogo dinámico de ejemplos** | **4 trazas** (`GET` + 3 `POST` a `/contacts/*`), **20 tipos de métricas** .NET runtime (`otel_metrics_catalog.json`: `LongSum`, `LongSumNonMonotonic`, `DoubleSum`, `Histogram`) y **3 logs** (INFO / WARN / ERROR). El desplegable se repuebla según la señal elegida. |
| 3 | **Cifrado AES-256-GCM (AES-NI)** | El JSON completo se cifra antes de tocar Kafka; `transaction_id` se usa como *Associated Data* (AAD), atando integridad y trazabilidad. |
| 4 | **Sobre Protobuf autosuficiente** | `EncryptedPayloadEnvelope` (namespace `Produbanco.Security.V1`): ciphertext + nonce + auth tag + metadatos de Key Vault + `telemetry_type` + `service_name` + `swagger` (contrato OpenAPI en YAML). |
| 5 | **Contrato OpenAPI embebido** | El YAML `transfer-mspx-prometeus.management.standard.yaml` (con directivas `x-log-data-protection`) se adjunta a cada sobre para que `consumer_streams` enmascare según el `operationId` de origen. |
| 6 | **`OTelTracePruner`** | Recorta los arreglos grandes embebidos en `http.response.body_preview` de las trazas **`GET`**, en un solo pase de streaming y **cero asignaciones de heap**. Configurable (`TracePruning:*`). |
| 7 | **Claves de partición de alta dispersión** | `UniformPartitionKeyGenerator` (SplitMix64 / avalancha Murmur3), en cliente y servidor, para repartir uniformemente entre las 40 particiones. |
| 8 | **Colecciones Cosmos DB aisladas** | El `service_name` define la colección destino aguas abajo: `{ServiceName con «.»→«_»}_{TelemetryType}`, p. ej. `Transfer_Mspx_Prometeus_Management_Trace`. |
| 9 | **Validación estricta del sobre** | `ValidateMandatoryEnvelopeFields`: **todos los campos son obligatorios salvo `swagger`**; se rechaza cualquier sobre incompleto antes de publicar. |
| 10 | **Detección automática de tipo de señal** | Si el portal no especifica el tipo, `DetectTelemetryType` lo infiere del contenido (`TraceId`/`SpanId` → Trace, `resourceMetrics` → Metric, `resourceLogs`/`log_level` → Log; por defecto Trace). |
| 11 | **Administración de tópicos** | REST + UI: listar (con/sin internos), ver detalle (particiones, líder, réplicas, ISR), crear, eliminar y *health* del clúster vía `AdminClient`. |
| 12 | **Envío por lote sintético** | Genera *N* transacciones bancarias aleatorias (`TRANSFER`, `PAYMENT`, `DEPOSIT`, `WITHDRAWAL`, `QR_PAYMENT` × canales × monedas) y las publica; crea el tópico con **40 particiones** si no existe. |
| 13 | **Caché de material criptográfico (TTL 1 h)** | `ConcurrentDictionary` en RAM; al vencer, se re-deriva de forma transparente. |
| 14 | **Cabeceras Kafka de trazabilidad** | Cada mensaje lleva `content-type: application/x-protobuf`, `x-encryption-algorithm`, `x-vault-token`, `x-cert-thumbprint`, `correlation-id`, `message-index`. |
| 15 | **Consola de flujo en vivo** | El dashboard muestra offset, partición y latencia de cada publicación; contador de sesión. |

---

## 📖 Definiciones

| Término | Significado en este servicio |
| :--- | :--- |
| **Sobre cifrado (`EncryptedPayloadEnvelope`)** | Mensaje **Protobuf** autosuficiente: transporta el JSON cifrado (`data`), los parámetros AEAD (`nonce` 12 B, `auth_tag` 16 B, `algorithm_version`), la identificación de la clave (`cert_thumbprint`, `vault_token_id`), la trazabilidad (`transaction_id`, `timestamp_unix_ms`), el `telemetry_type`, el `service_name` y —único opcional— el `swagger`. |
| **Tipo de señal (`TelemetryType`)** | Enum del proto: `TRACE (1)`, `METRIC (2)`, `LOG (3)`. `UNSPECIFIED (0)` se rechaza. Solo `TRACE` dispara enmascaramiento por contrato aguas abajo. |
| **Contrato OpenAPI / `swagger`** | El YAML del microservicio de origen con metadatos `x-log-data-protection` (`@Log.Hash(SHA256)`, `@Log.Partial(LAST_4)`, `@Log.Full`, `@Log.Remove`). El emisor **solo lo transporta**; quien lo aplica es `consumer_streams`. |
| **`service_name`** | Identificador del microservicio emisor (por defecto `Transfer.Mspx.Prometeus.Management`). Define la colección Cosmos DB aislada por servicio y señal. |
| **Clave de partición dispersa (`PK-…`)** | Cadena `PK-<hash 16 hex>[-<sufijo>]` generada con SplitMix64. Garantiza reparto uniforme. Una clave que ya llega con forma `PK-…` y longitud > 20 se respeta **verbatim**; en otro caso se re-deriva del identificador de negocio. |
| **AAD (*Associated Data*)** | El `transaction_id` se pasa como dato asociado del cifrado GCM: si se altera en tránsito, la validación del *auth tag* falla y el mensaje termina en la DLQ. |
| **Podado de trazas (*trace pruning*)** | Truncado de los arreglos JSON grandes dentro de `http.response.body_preview` de las trazas `GET`, para no archivar cientos de filas de respuesta en el log de auditoría. |
| **Semilla determinista** | `SHA256("PRODUBANCO-SECRET-KEY-SEED-produbanco-encryption-cert-2026")` — la clave AES-256 efectiva, idéntica en el emisor y en `consumer_streams`. La ruta `DefaultAzureCredential` + lowkey-vault existe pero la semilla es lo que realmente cifra. |
| ***Fail-fast*** | La configuración esencial ausente (`Kafka:BootstrapServers`, `Kafka:ClientId`, `KeyVault:VaultUri`) lanza excepción **al arranque**, no en la primera petición. |

---

## 🧱 Arquitectura hexagonal del servicio

```
KafkaDemo.Web  (JIT · ASP.NET Core Minimal APIs + dashboard wwwroot/)
   │  Program.cs → endpoints REST + StaticFiles + fallback a index.html
   ▼
KafkaDemo.Application  (casos de uso · sin dependencias de infra)
   ├─ SendMessagesUseCase      → SendCustomMessageAsync / GenerateAndSendBatchAsync
   └─ ManageTopicsUseCase      → listar / crear / borrar / detalle / health
   ▼
KafkaDemo.Domain  (modelos, puertos, utilidades puras, .proto)
   ├─ Ports: IMessageProducerPort · ITopicManagementPort · IVaultTokenProviderPort · IPayloadCryptoPort
   ├─ Utils: UniformPartitionKeyGenerator (SplitMix64) · OTelTracePruner
   ├─ Configuration: TracePruningSettings
   └─ Protos/encrypted_envelope.proto  (compilado con Grpc.Tools → clases C#)
   ▲
KafkaDemo.Infrastructure  (adaptadores · implementan los puertos)
   ├─ KafkaProducerAdapter        (Confluent.Kafka · productor dual string/byte[], idempotente, Acks=All)
   ├─ KafkaAdminAdapter           (AdminClient · metadata, CreateTopics, DeleteTopics, ping)
   ├─ AesGcmPayloadCryptoAdapter  (AES-256-GCM AES-NI · arma y valida el envelope)
   ├─ AzureKeyVaultTokenAdapter   (DefaultAzureCredential + caché RAM TTL 1 h; semilla determinista)
   └─ DependencyInjection.cs      (AddKafkaInfrastructure: settings fail-fast + registro de puertos)
```

También está `KafkaDemo.ConsoleApp` (en la `.slnx`, con su `Dockerfile`): entrada alternativa que
espera al broker (15 reintentos × 3 s) y publica un lote de `MessageCount` (20 por defecto) al
arrancar. **Desactivada en `docker-compose`.**

---

## 🔄 Flujo de publicación de un evento

`POST /api/messages/send` → `SendMessagesUseCase.SendCustomMessageAsync`:

1. **Material de clave** — `IVaultTokenProviderPort.GetOrCreateEncryptionKeyAsync("produbanco-encryption-cert")`; *cache hit* en RAM o re-derivación de la semilla (TTL 1 h).
2. **Identificadores** — `eventId` = GUID nuevo; `transaction_id` = `request.Key` o `TXN-yyyyMMdd-XXXXXX`.
3. **Clave de partición** — si `request.Key` empieza por `PK-` y mide > 20 → se usa tal cual; si no → `UniformPartitionKeyGenerator.GenerateDispersedKey(...)`.
4. **Podado** — `OTelTracePruner.PruneIfGetTrace(value, pruningSettings)` (ver sección siguiente).
5. **Tipo de señal** — se toma `request.TelemetryType` o se infiere con `DetectTelemetryType`.
6. **Cifrado + sobre** — `AesGcmPayloadCryptoAdapter.EncryptJsonToEnvelope(...)`: nonce aleatorio de 12 B, `Encrypt` con AAD = `transaction_id`, se arma el `EncryptedPayloadEnvelope` (con `swagger` YAML y `service_name`) y se ejecuta `ValidateMandatoryEnvelopeFields`.
7. **Publicación** — `KafkaProducerAdapter.SendMessageAsync`: `ProduceAsync` del `byte[]` Protobuf en `…emitted.v1` con las cabeceras `x-*`.
8. **Respuesta** — `{ topic, partition, offset, status, timestamp, key }`; el dashboard lo pinta en la consola de flujo y regenera una nueva clave para el siguiente envío.

> **El emisor no valida el *esquema* del payload**: cifra los bytes tal cual. Un JSON mal
> formado se detecta en `consumer_streams` (parseo / validación) y se enruta a la DLQ.

---

## ✂️ `OTelTracePruner` — podado de trazas GET

### Qué poda, exactamente

Una traza `GET` que lista, por ejemplo, **120 contactos** produce un tag
`http.response.body_preview` cuyo valor es un **array JSON de 120 objetos serializado como
string**. Archivar las 120 filas en el log de auditoría es ruido: para observabilidad basta una
muestra representativa. `OTelTracePruner` recorta ese array.

`PruneIfGetTrace(rawJson, settings)` hace lo siguiente:

| Paso | Acción | Si no aplica |
| :- | :--- | :--- |
| 1 | Si `TracePruning:Enabled = false` o el JSON está vacío | devuelve el original **sin tocar** |
| 2 | **Filtro preliminar ultrarrápido** (`String.Contains`): el texto debe contener `"GET"` y `"http.response.body_preview"` | devuelve el original |
| 3 | Valida que el contenido sea JSON estructurado (`{` o `[` tras espacios) — comprobación de 0 asignaciones | devuelve el original |
| 4 | **Pase de streaming** (`Utf8JsonReader` → `Utf8JsonWriter`): copia **todo el JSON verbatim**, excepto la propiedad `http.response.body_preview` | — |
| 5 | Si el valor de `http.response.body_preview` es un **string que a su vez es JSON estructurado**, lo poda con `PruneInnerJsonString`; si es texto plano / HTML / `null`, lo preserva | preserva el valor |
| 6 | En el JSON interno: por cada **arreglo**, conserva como máximo **`MaxArrayItems`** elementos (los sobrantes —objetos, arreglos o escalares— se descartan) **mientras la profundidad ≤ `MaxDepth`**. Más abajo de `MaxDepth`, los arreglos se copian completos | — |
| 7 | Cualquier excepción de sintaxis → **fallback seguro**: devuelve el `rawJson` original | — |

### Qué NO hace

- **No toca las trazas `POST`** (el filtro del paso 2 exige `GET`). El body de un `POST` representa una entidad única y es relevante completo.
- **No toca `http.request.body_preview`** ni ningún otro tag — solo `http.response.body_preview`.
- **No recorta la profundidad** del JSON: `MaxDepth` limita *hasta qué nivel llega el recorte de arreglos*, no trunca la anidación.
- **No enmascara nada** — eso es responsabilidad de `consumer_streams` con el contrato OpenAPI.

### Parámetros

| Clave | Env (compose) | Defecto | Efecto |
| :--- | :--- | :--- | :--- |
| `TracePruning:Enabled` | `TracePruning__Enabled` | `true` | Activa/desactiva el podado |
| `TracePruning:MaxArrayItems` | `TracePruning__MaxArrayItems` | `10` | Elementos máximos que se conservan por arreglo |
| `TracePruning:MaxDepth` | `TracePruning__MaxDepth` | `5` | Nivel de anidación hasta el que se aplica el recorte |

---

## 🎬 Escenarios de uso

### Escenario 1 — Traza `GET` con respuesta voluminosa (se poda)

- Dashboard → tipo **Trace** → preset *"GET /contacts-by-idClient/8172201/IN (Lista 120 Contactos)"*.
- El JSON tiene `TraceId`, `SpanId`, `Tags.http.response.body_preview` con 120 contactos.
- `OTelTracePruner` deja **10** contactos; el resto del span (tiempos, `url.path`, `http.route`, …) se copia intacto.
- Se cifra, se arma el sobre (`telemetry_type = TRACE`, `service_name`, `swagger`) y se publica en `…emitted.v1`.
- **Aguas abajo:** `consumer_streams` descifra, aplica el contrato (`@Log.Hash(SHA256)` sobre cédulas, `@Log.Partial(LAST_4)` sobre cuentas, enmascarado de `url.path`) y persiste en `Transfer_Mspx_Prometeus_Management_Trace`.

### Escenario 2 — Traza `POST` de creación de contacto (no se poda)

- Preset *"POST /contacts/local-contact (Cédula: 1702756766)"*.
- El body con datos personales viaja **completo** (el pruner ignora los `POST`).
- `consumer_streams` aplica las reglas del `operationId` `POST` correspondiente (hash de cédula, últimos 4 de la cuenta destino, etc.).

### Escenario 3 — Métrica del runtime .NET

- Tipo **Metric** → preset *"[LongSum] dotnet.gc.collections"* (uno de los 20 del `otel_metrics_catalog.json`).
- `telemetry_type = METRIC`. El `swagger` viaja en el sobre pero `consumer_streams` **lo ignora** (las métricas no se enmascaran).
- Colección destino: `Transfer_Mspx_Prometeus_Management_Metric`.

### Escenario 4 — Log de aplicación

- Tipo **Log** → preset *"[ERROR] DatabaseTimeout"* (incluye `exception.type` / `stackTrace`).
- `telemetry_type = LOG`. Ruta directa a Cosmos DB, sin enmascarar.
- Colección: `Transfer_Mspx_Prometeus_Management_Log`.

### Escenario 5 — Prueba de carga / dispersión de particiones

- Botón **"Enviar Lote de 20 Transacciones"** o `POST /api/messages/send-batch { "count": 1000 }`.
- `GenerateAndSendBatchAsync`: crea `…emitted.v1` con **40 particiones** si falta, genera transacciones bancarias aleatorias (cada una con `PK-<hash>-<cuentaOrigen>`), las cifra y las publica **secuencialmente** con `Flush` final.
- Útil para verificar el reparto uniforme entre particiones en la Kafka UI.

### Escenario 6 — Gestión de tópicos y monitoreo

- Tab **"Gestión de Tópicos"**: listar (checkbox para incluir internos como `__consumer_offsets`), ver detalle (partición, líder, réplicas, ISR), crear (nombre validado `[a-zA-Z0-9._\-]+`, particiones, replicación), eliminar (con confirmación).
- `GET /api/health` se sondea cada 10 s para el badge de estado del clúster.

### Escenario 7 — Vencimiento del TTL de la clave

- El material del Vault vive 1 h en RAM. Al vencer, el siguiente envío entra por *cache miss*, re-deriva la clave de la semilla y renueva el TTL — **sin pérdida de mensajes ni intervención**.

---

## ⚙️ Consideraciones de implementación

- **Servicio JIT, no AOT.** A diferencia de `consumer_streams` y `log_sink`, el emisor corre sobre `mcr.microsoft.com/dotnet/aspnet:10.0`. Usa `System.Text.Json` por reflexión (serializa objetos anónimos en el lote); **no** hay `JsonSerializerContext` *source-generated*.
- **Los `.proto` están duplicados.** `emisor_mensaje/src/KafkaDemo.Domain/Protos/encrypted_envelope.proto` debe permanecer **byte a byte idéntico** al de `consumer_streams`. Cada proyecto compila el suyo con `Grpc.Tools`.
- **La clave AES es una semilla, no un secreto del Vault.** `AzureKeyVaultTokenAdapter` construye un `CertificateClient` con `DefaultAzureCredential` (aceptando el certificado autofirmado de lowkey-vault), pero la clave efectiva se deriva de `SHA256("PRODUBANCO-SECRET-KEY-SEED-produbanco-encryption-cert-2026")`. Cambiar esa cadena **rompe el descifrado** en todo el pipeline. El `catch` con clave *fallback* existe pero, en la práctica, la ruta feliz no consulta el Vault y no lanza.
- **El envío individual NO crea el tópico.** Solo `GenerateAndSendBatchAsync` asegura `…emitted.v1` con 40 particiones. Un `POST /api/messages/send` a un tópico inexistente depende de la autocreación del broker (`num.partitions=3`).
- **El `swagger` se adjunta a *todos* los sobres** (Trace, Metric y Log). Es `consumer_streams` quien decide usarlo solo cuando `telemetry_type == Trace`.
- **El contrato se cachea en memoria estática** (`_cachedSwaggerYaml`) tras la primera lectura de disco. Para recargarlo hay que reiniciar el servicio. Se busca en `Contracts/`, `wwwroot/data/` y `data_guia/` (en ese orden).
- **`transaction_id` = AAD.** Va como *Associated Data* del cifrado GCM. En la ruta de envío individual con `request.Key = "PK-…"`, el `transaction_id`, el AAD y la clave de partición coinciden con esa misma cadena.
- **El lote es secuencial**, no un pipeline paralelo: `foreach … await SendMessageAsync` y `Flush(5 s)` al final.
- **Productor idempotente:** `EnableIdempotence = true`, `Acks = All`, `MessageSendMaxRetries = 3`, `MessageTimeoutMs = 10000`.
- **La clave del cliente y la del servidor NO son la misma cadena.** `app.js` y `UniformPartitionKeyGenerator` comparten el algoritmo SplitMix64 pero siembran distinto (el cliente añade `Date.now()` y sufijo `-TRACE/-METRIC/-LOG`; el servidor usa `TickCount64` y el `businessId`). El cliente **propone**; el servidor la **respeta** si empieza por `PK-` y mide > 20 caracteres.
- **CORS abierto** (`AllowAnyOrigin/Header/Method`) para permitir acceso desde otros hosts en la demo.
- **Puerto 5000 fijo** vía `ASPNETCORE_URLS`. Dashboard 100 % estático (`wwwroot/`, JS *vanilla*), servido con `UseStaticFiles` + `MapFallbackToFile("index.html")`.

---

## 🔧 Configuración

Orden de resolución (en `Infrastructure/DependencyInjection.cs`):
`Section:Key` → variable `TECH-INT-…` → variable `TECH_INT_…`. Lo esencial ausente **lanza al arranque**.

| `Section:Key` | Env (docker-compose) | Alterno `TECH-…` | Defecto | Uso |
| :--- | :--- | :--- | :--- | :--- |
| `Kafka:BootstrapServers` | `Kafka__BootstrapServers` | `TECH-INT-MSG-KAFKA_BROKERS` | — *(obligatorio)* | Brokers del clúster |
| `Kafka:ClientId` | `Kafka__ClientId` | `TECH-INT-MSG-CLIENT_ID` | — *(obligatorio)* | Client ID del productor/admin |
| `Kafka:TargetTopic` | `Kafka__TargetTopic` | `TECH-INT-MSG-LOGS_TOPIC` | `tp.observability.application-log.emitted.v1` | Tópico destino del lote |
| `Kafka:Acks` | — | — | `all` | Confirmaciones del broker |
| `Kafka:EnableIdempotence` | — | — | `true` | Productor idempotente |
| `KeyVault:VaultUri` | `KeyVault__VaultUri` | `TECH-INT-SECU-VAULT_URL` | — *(obligatorio)* | URI del emulador de Key Vault |
| `KeyVault:VaultName` | `KeyVault__VaultName` | — | — | Nombre lógico del vault |
| `TracePruning:Enabled` | `TracePruning__Enabled` | `TRACE_PRUNING_ENABLED` | `true` | Activa el podado de trazas GET |
| `TracePruning:MaxArrayItems` | `TracePruning__MaxArrayItems` | `TRACE_PRUNING_MAX_ARRAY_ITEMS` | `10` | Elementos máximos por arreglo |
| `TracePruning:MaxDepth` | `TracePruning__MaxDepth` | `TRACE_PRUNING_MAX_DEPTH` | `5` | Profundidad hasta la que se poda |
| `Kafka:MessageCount` *(ConsoleApp)* | — | `TECH-INT-MSG-COUNT` | `20` | Tamaño del lote al arrancar la consola |

---

## 🌐 API REST

Base: `http://localhost:5000`

| Método | Ruta | Descripción |
| :--- | :--- | :--- |
| `GET` | `/api/health` | Estado del clúster: `{ isConnected, totalTopics, status, checkedAt }` |
| `GET` | `/api/topics?includeInternal=false` | Lista de tópicos con particiones, réplicas e ISR |
| `GET` | `/api/topics/{topicName}` | Detalle de un tópico |
| `POST` | `/api/topics` | Crea un tópico — body `{ topicName, partitions, replicationFactor, configs? }` |
| `DELETE` | `/api/topics/{topicName}` | Elimina un tópico |
| `POST` | `/api/messages/send` | Publica un evento — body `{ topic, key?, value, telemetryType?, serviceName?, headers? }` |
| `POST` | `/api/messages/send-batch` | Publica un lote — body `{ topic?, count? }` (defecto: `TargetTopic`, 20) |
| `GET` | `/api/traces/otel-get` | Devuelve la traza `GET` de muestra (`wwwroot/data/otel_get_trace.json`) |
| `GET` | `/api/contracts/swagger` | Devuelve el contrato OpenAPI YAML que se embebe en el sobre |

---

## 🛠️ Compilación y ejecución

```powershell
# Compilar la solución del emisor
dotnet build emisor_mensaje/KafkaDemoHexagonal.slnx

# Dashboard contra un broker local (localhost:9092, de appsettings.json)
dotnet run --project emisor_mensaje/src/KafkaDemo.Web        # http://localhost:5000

# Entrada de consola alternativa (lote único al arrancar)
dotnet run --project emisor_mensaje/src/KafkaDemo.ConsoleApp

# Solo este servicio dentro del stack docker
docker compose up -d --build kafka-web
```

Requiere el **SDK de .NET 10** (repo construido con 10.0.203). El contenedor `kafka-web` se
construye desde `emisor_mensaje/Dockerfile.web` (runtime `aspnet:10.0`).

---

## 📁 Estructura de carpetas

```text
emisor_mensaje/
├── Dockerfile              # KafkaDemo.ConsoleApp (runtime:10.0) — desactivado en compose
├── Dockerfile.web          # KafkaDemo.Web (aspnet:10.0) — contenedor kafka-web
├── KafkaDemoHexagonal.slnx
└── src/
    ├── KafkaDemo.Domain/
    │   ├── Models/            KafkaMessage · MessageResult · TopicInfo · TopicCreationRequest
    │   ├── Ports/             IMessageProducerPort · ITopicManagementPort · ICryptoPorts (IVaultTokenProviderPort, IPayloadCryptoPort)
    │   ├── Utils/             UniformPartitionKeyGenerator (SplitMix64) · OTelTracePruner
    │   ├── Configuration/     TracePruningSettings
    │   └── Protos/            encrypted_envelope.proto  (→ Grpc.Tools)
    ├── KafkaDemo.Application/
    │   ├── UseCases/          SendMessagesUseCase · ManageTopicsUseCase
    │   └── DTOs/              SendMessageRequestDto · BatchSendResultDto · CreateTopicDto · ClusterHealthDto
    ├── KafkaDemo.Infrastructure/
    │   ├── Adapters/          KafkaProducerAdapter · KafkaAdminAdapter · AesGcmPayloadCryptoAdapter · AzureKeyVaultTokenAdapter
    │   ├── Configuration/     KafkaSettings
    │   └── DependencyInjection.cs
    ├── KafkaDemo.Web/         Program.cs (Minimal APIs) + appsettings.json
    │   └── wwwroot/
    │       ├── index.html · css/style.css · js/app.js
    │       └── data/          otel_get_trace.json · otel_post_trace_{1,2,3}.json
    │                          otel_metrics_catalog.json · transfer-mspx-prometeus.management.standard.yaml
    └── KafkaDemo.ConsoleApp/  Program.cs + appsettings.json  (entrada alternativa, batch al arranque)
```
