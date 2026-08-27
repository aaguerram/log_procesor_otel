# Estándar para nombrado y diseño de tópicos

| Criterio | Descripción |
|---|---|
| Código | EST-INT-0001 |
| Política relacionada | |
| Deriva de | Arquitectura Flexible |
| Dominio | Infraestructura, Seguridad, Aplicación, Integración, DevOps |
| Versión | v1.0|
| Fecha de Emisión | 2026-06-02 |
| Fecha de Actualización | 2026-06-04 |
| Estado | Propuesto |
| Responsable técnico | Arquitectura |
| Aprobado por | Comité de Gobierno de Arquitectura |

## Revisión y aprobación del documento

| Versión | Fecha de revisión | Revisado por | Fecha de aprobación | Aprobado por | Comité, Directorio y N° de acta |
|---|---|---|---|---|---|
| v1.0 | 2026-06-02 | Arquitectura | 2026-06-02 | Comité de Gobierno de Arquitectura | [PENDIENTE-VALIDACION-DUEÑO] |

### Propósito 

Establecer un marco normativo, estandarizado y de cumplimiento para la creación, nombrado y configuración de topicos de eventos en KAFKA, con el propósito de:
   - Facilitar su gobierno, los nombres deben ser fáciles de entender y reflejar claramente los datos y el propósito del tópico
   - Prevenir problemas de infraestructura mediante un diseño correcto de particionado y retención de los datos
   - Evitar colisiones de nombres 

### Alcance 

Este lineamiento es de uso obligatiorio y aplica para los equipos que diseñen, implementen o consuman eventos dentro de la plataforma KAFKA, a nivel de los tópicos creados por las aplicaciones inernas como tambien aquellos creados por herramientas de integración.

### Definiciones 

**KAFKA:**  Apache Kafka es una plataforma distribuida de transmisión de eventos (Event Streaming Platform) que permite publicar, almacenar, procesar y consumir eventos en tiempo real de manera escalable, resiliente y desacoplada. Su principal objetivo es facilitar la integración entre sistemas mediante el intercambio de eventos de forma asíncrona.

**Tópico (Topic)** Es un canal lógico donde se publican y almacenan eventos relacionados con una misma capacidad de negocio o proceso. Los productores publican eventos en un tópico y los consumidores los leen según sus necesidades.

**Broker** Es un servidor que forma parte del clúster Kafka y es responsable de recibir, almacenar y distribuir eventos. Un clúster Kafka puede estar compuesto por uno o varios brokers trabajando de forma conjunta para proporcionar alta disponibilidad y escalabilidad.

**Producer** Es una aplicación o componente que genera y publica eventos en uno o varios tópicos Kafka. Es el responsable de enviar la información al clúster para que pueda ser consumida por otros sistemas.

**Consumer** Es una aplicación o componente que se suscribe a uno o varios tópicos Kafka para leer y procesar los eventos publicados por los productores.

**Partición** Es una subdivisión física de un tópico que permite distribuir los eventos entre múltiples brokers y consumidores, mejorando la escalabilidad y el rendimiento del sistema.
Las particiones permiten que varios consumidores procesen eventos en paralelo, manteniendo el orden de los mensajes dentro de una misma partición

**Retención** Es la política que determina cuánto tiempo o cuánto volumen de datos permanecerán almacenados en un tópico antes de ser eliminados automáticamente por Kafka.

**Offset** Es un identificador secuencial asignado a cada evento dentro de una partición. Permite a Kafka conocer la posición exacta de un mensaje y a los consumidores continuar la lectura desde el último evento procesado.

**DLQ o Dead Letter Queue** Es un tópico especial utilizado para almacenar eventos que no pudieron ser procesados correctamente por un consumidor, luego de aplicar los reintentos definidos.

### Casos de Uso
Casos de uso en los que se debe utilizar Kafka (streaming/eventos) frente a sistemas de colas (mensajería punto a punto).

#### 1. Procesamiento de eventos de negocio
Publicación de eventos de negocio relevantes.
Ejemplos: pagos autorizados, transacciones ejecutadas, notificaciones generadas

#### 2. Arquitectura basada en eventos (EDA)
Kafka está optimizado para la ingesta de alto volumen de datos y permite que múltiples consumidores procesen los mismos eventos de forma paralela.
Adecuado para arquitecturas event-driven, flujos continuos de datos (por ejemplo, tracking de clics o dispositivos IoT) y escenarios de procesamiento en tiempo real.

#### 3. Persistencia y relectura de mensajes. 
Para almacenar eventos durante un periodo configurable, habilitando su relectura para reprocesamiento, reproducción o reconstrucción de estados históricos sin necesidad de reinyectar datos.
Este comportamiento es esencial en arquitecturas de event sourcing, CQRS o para auditorías.

#### 4. Fan-out (broadcast) 
Un evento necesita ser consumido por múltiples sistemas de manera independiente, garantizando desacoplamiento entre productores y consumidores

#### 5. Datos y analítica
Para alimentar ecosistemas de datos, incluyendo data lakes, soluciones de analítica avanzada, machine learning, motores de correlación y prevención de fraude, permitiendo el procesamiento continuo de información.

### Estándar de nombrado

#### Consideraciones generales 

- Nombrado en **inglés** 
- Usar solo minúsculas.
- Usar como separador de niveles el punto (.)
- Puede contener **letras** (a-z)
- Uso de **guiones medios** (-) para palabras compuestas, no usar otros caracteres especiales 
- No incluir el **ambiente:** dev, qa y prod 

#### Nombrado de Tópicos

Todo tópico Kafka publicado desde la Capa Media deberá nombrarse bajo una convención funcional, evitando referencias a microservicios, aplicaciones, tecnologías o componentes físicos.
La estructura para el nombre del tópico deberá ser jerarquico y separada por puntos (.)

**Formato:**   

tp.<dominio>.<recurso>.<evento>.<version>

Donde:

|Nombre|Detalle|Mandatorio|Ejemplo|
|--|--|--|--|
|tp|Acronimo de Tópico. |Si|tp|
|dominio|Para dominios de negocio utilizar los dominios de negocio de BIAN. En el caso de dominios técnicos que no se alineen con BIAN, se empleará un dominio técnico que describa claramente la función o capacidad correspondiente. |Si|cards|
|recurso|Identifica la entidad o recurso al que está asociado el evento. Cuando esa entidad corresponda a un objeto de negocio de BIAN, debe usarse el nombre de la entidad tal como aparece en el Business Object Model (BOM). |Si|credit-card,customer-survey|
|evento|Evento que representa el tópico|No|blocked|
|version|Indica la versión del tópico cuando es necesario|No|v1|

Ejemplos: 

tp.payments.transaction.authorized.v1
Tópico donde se almacenan eventos relacionados con pagos autorizados.

tp.cards.credit-card.blocked
Tópico donde se almacenan eventos relacionados con tarjetas de credito bloquedas

tp.marketing.customer-survey.response
Tópico donde se almacenan eventos relacionados con el resultado de que si el cliente lleno o no la encuesta de satisfaccion

### Diseño Técnico 
#### 1. Formatos de datos (payload)

Queda probido el uso de XML, texto plano, CSV o estructuras libres; los formatos permitodos son:

a. Para eventos corporativos y de integración entre dominios, se recomienda usar Avro binario con Schema Registry, debido a que este formato serializa los mensajes en formato binario compacto que reduce el consumo de red y almacenamiento.

**Casos de Uso**
- Eventos críticos de negocio.
- Alta volumetría.
- Integración entre múltiples consumidores.
- Validación estricta de esquemas.
- Gobierno de compatibilidad.

b. JSON Schema puede usarse cuando se requiera mayor legibilidad o facilidad de integración.

**Casos de Uso**
- Integraciones con proveedores.
- Consumo por aplicaciones que no soportan Avro fácilmente.
- Casos de menor volumetría.
- Eventos donde la lectura humana facilite soporte.

####2. Estructura del mensaje
Todo mensaje publicado en Kafka deberá seguir un patrón de tipo envelope, en el cual los metadatos (como identificadores, timestamps, entre otros) estén claramente separados del payload o contenido de datos del mensaje

Ejemplo:
{

  "eventId": "uuid",
  "correlationId": "uuid",
  "eventTime": "date-time",
  "specVersion": "1.0",
  "dataContentType": "avro/binary",
  "data": {}
}

|Campo|Descripción|Mandatorio|Ejemplo|
|--|--|--|--|
|eventId|Identificador único del evento|Si|uuid|
|correlationId|Identificador único del evento|No|uuid|
|eventTime|Fecha y hora de generación|Si|date-time|
|specVersion|Versión del estándar |Si|v 1.0|
|dataContentType|Formato usado|Si|avro/binary|
|data|Payload del mensaje|Si||

####3. Retención de datos
Cada tópico debe tener política de retención definida.
El tiempo de retención por defecto en producción sera de al menos 3 días.

####3. DLQ y manejo de errores
Todo consumidor crítico debe tener tópico DLQ.
<dominio>.<recurso>.<evento>.dlq.<version>

**Casos de Uso**
- Eventos con errores de formato.
- Eventos con datos incompletos.
- Fallas de validación de negocio.
- Errores temporales no resueltos luego de varios reintentos.
- Fallas en sistemas destino.

####4. Particionado y Paralelismo
Número de particiones: Ningun tópico en producción se creará con una sola partición, al menos seran 4.
Tópicos de alto rendimiento deben calcularse con base al rendimiento y consumo esperado.

####5. Disponibilidad y Resiliencia
Para ambientes productivos el factor de replicación deberá ser siempre 3, con el fin de evitar pérdida de datos en casos de caida de un broker. 

####6. Seguridad
Cada tópico debe definir:

- Productor autorizado.
- Consumidores autorizados.
- ACLs por tópico.
- Cifrado en tránsito.
- Cifrado en reposo si aplica.
- No publicar datos sensibles innecesarios.
- Enmascarar o tokenizar datos sensibles.
- Trazabilidad mediante correlationId.

## Checklist de aceptación

- [ ] El nombre sigue la convención de nombrado definida.
- [ ] No se incluyen nombres de aplicaciones o microservicios.
- [ ] El caso de uso y propósito del tópico están documentados.
- [ ] El evento posee identificador único (eventId).
- [ ] Se utiliza Avro como formato preferente para eventos
- [ ] Se definió una clave de partición (Partition Key).
- [ ] El tópico tiene la política de retención definida.
- [ ] Se encuentran definidas las ACLs correspondientes.
- [ ] Existe tópico DLQ cuando el caso de uso lo requiere.
- [ ] Los datos sensibles están tokenizados o enmascarados cuando aplica.

## Excepciones

Las excepciones al cumplimiento del estándar deberán estar justificadas técnicamente, documentadas y aprobadas por Arquitectura o el Comité de Gobierno correspondiente.  
Toda excepción deberá incluir alcance, vigencia, riesgo aceptado, controles compensatorios y plan de regularización o mitigación.

## Responsabilidades

| Rol | Responsabilidad |
|---|---|
| Arquitectura | Definir las convenciones de nombrado y configuración (particiones, retención) de Tópicos |
| Equipo Dev | Aplicar convenciones nombrado y configuración e implementar productores y consumidores |
| Seguridad | Definir y aplicar políticas de acceso (ACLs) a nivel de tópicos |
| DevOps / Plataforma | Automatiza creación y configuración de tópicos. Monitorear el estado de los tópicos|
| QA | Validar que los tópicos estén creados siguiendo el estándar de nombrado y configuración (particiones, retención). Ejecutar pruebas de integración y contratos |

## Referencias

- [Kafka Topic Naming Convention](https://www.confluent.io/learn/kafka-topic-naming-convention/)
- [When to use RabbitMQ or Apache Kafka](https://www.cloudamqp.com/blog/when-to-use-rabbitmq-or-apache-kafka.html#:~:text=Ideal%20for%3A%20Apache%20Kafka%20%26,possible%20through%20RabbitMQ%20Streams)
- [Message Queue vs. Apache Kafka](https://blog.iron.io/message-queue-vs-apache-kafka/#:~:text=Pros%20and%20Cons%20of%20Apache,Kafka)