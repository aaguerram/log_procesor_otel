# Lineamiento para nombrado de variables

|Criterio|Descripción|
|---|---|
|Código|LIN-INT-0010|
|Política relacionada|04|
|Deriva de|Clean Architecture|
|Dominio|Business, Application, Technology|
|Versión|v1.0|
|Fecha de Emisión|2026-06-22|
|Fecha de Actualización|2026-07-31|
|Estado|Publicado|
|Responsable técnico|Arquitectura Integración|
|Aprobado por|Comité de Gobierno de Arquitectura|

## Revisión y aprobación del documento

| Versión | Fecha de revisión | Revisado por | Fecha de aprobación | Aprobado por | Comité, Directorio y N° de acta |
|---|---|---|---|---|---|
| v1.0 | 2026-06-22 | Arquitectura Integración | 2026-06-22 | Comité de Gobierno de Arquitectura | [PENDIENTE-VALIDACION-DUEÑO] |

## Tabla de contenido

- [Propósito](#propósito)
- [Alcance](#alcance)
- [Audiencia](#audiencia)
- [1. Consideraciones generales](#1-consideraciones-generales)
- [2. Convención](#2-convención)
  - [2.1. Tipo(Type)](#21-tipotype)
  - [2.2. Alcance(Scope)](#22-alcancescope)
  - [2.3. Fuente](#23-fuentesource)
  - [2.4. Recurso](#24-recurso)
  - [2.5 Atributo](#25-atributo)
  - [2.6 Ejemplos](#26-ejemplo-de-lineamiento)
- [Checklist de aceptación](#checklist-de-aceptación)
- [Referencias](#referencias)

## Propósito

Establecer la estructura para el nombrado de variables del lado de una entidad que lo establece como propiedad, uso en tiempo de ejecución, caché, al asignar un objeto con un patrón de diseño.

## Alcance

El uso es obligatiorio tanto interno como externo, este último se debe aprobar con arquitectura para su uso en caso de no apegarse al mismo.

## Audiencia

Desarrollo, Seguridad, Gobierno de datos, Arquitectura de datos.

### 1. Consideraciones generales

- Uso exclusivo de mayúsculas (UPPERCASE) en cada sección.
- Use inglés
- Longitud máxima de cada sección es de cuatro caracteres excepto en RECURSO_ATRIBUTO que tiene doce, no considere el caracter de separación.
- Los únicos carácteres permitidos son
  - \- guión medio para separar secciones
  - \_ guión bajo para separar características.
- No use espacios
- No emplee palabras reservadas como null, string, empty, etc.
- No use números
- No use secuencias
- El nombre no puede iniciar ni terminar con caracteres especiales, números.

### 2. Convención

Formato:

```yml
<ACRÓNIMO_TIPO>-<ALCANCE>-<FUENTE>-<RECURSO>_<ATRIBUTO>
```

Donde:

#### 2.1 Tipo(Type)

Define la capa, use la referencia **ArchiMate**:

|Nombre|Objetivo|Acrónimo|
|--|--|--|
|BUSINESS|Negocio|**BUSI**|
|APPLICATION|Aplicación|**APPL**|
|TECHNOLOGY|Tecnología|**TECH**|

#### 2.2 Alcance(Scope)

La frontera de consumo

|Nombre|Acrónimo|
|--|--|
|Interno|INT|
|Externo|EXT|

#### 2.3 Fuente(Source)

Especifica el repositorio, interfaz, registro. En si el registro del objeto en la integración.

##### &ensp; 2.3.1 Ejemplos(Samples)

Technology:

- DB (Base de Datos)
- CACH (Cache)
- MSG (Mensajería)
- API (API)
- SECU (Security)
- LOGS: LOGS
- SFTP (Files)

#### 2.4 Recurso

Identifica el objeto específico dentro de la fuente.

Ejemplos:

- CUST: CUSTOMER
- PAYM: PAYMENT
- AUDI: AUDITORIA

#### 2.5 Atributo

Indica la propiedad concreta del recurso asi como la característica.

Ejemplos:

- URL
- NAME
- USER
- PASS
- TIMEOUT
- CONN (conexión)
- KEY, etc...

### 2.6 Ejemplo de lineamiento

```yml
- Nombre: TECH-INT-API-PAYM_URL

Donde:

TECH → Capa
INT→ Alcance Interno
API→ Fuente API
PAYM → Recurso pagos
URL → propiedad o característica
```

```yml
otros:

- TECH-INT-DB-CUST_PORT
- TECH-INT-DB-CUST_CONN
- TECH-INT-MSG-PAYM_TOPIC
- TECH-INT-MSG-NOTI_QUEUE
- TECH-EXT-API-REGCIV_URL
- TECH-INT-CACH-SESS_TTL
- TECH-INT-API-FRAUD_KEY
- TECH-INT-LOGS-AUDI_LEVEL
- TECH-INT-SFTP-FILENAME_PATH
```

## Checklist de aceptación

- [ ] La integración cuenta con aprobación de Arquitectura.
- [ ] La integración cuenta con aprobación de Seguridad.
- [ ] El canal de comunicación fue definido y aprobado por el Banco.
- [ ] Los capas estan segregadas.
- [ ] No se usan nombres a percepción.
- [ ] No se emplean caracteres no permitidos.
- [ ] Se cumple el lineamiento

## Excepciones

Cualquier excepción a este lineamiento deberá ser formalmente solicitada, justificada, evaluada y aprobada por las áreas responsables del Banco.

## Referencias

- [Archimate](https://archimate.visual-paradigm.com/what-is-layers-and-aspects-in-archimate-core-framework)
