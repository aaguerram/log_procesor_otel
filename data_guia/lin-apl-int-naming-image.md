# Lineamiento para nombrado y despliegue de imágenes

|Criterio|Descripción|
|---|---|
|Código|LIN-INT-0013|
|Política relacionada|04|
|Deriva de|Clean Architecture|
|Dominio|Business, Application, Technology|
|Versión|v1.0|
|Fecha de Emisión|2026-06-23|
|Fecha de Actualización|2026-07-29|
|Estado|Publicado|
|Responsable técnico|Arquitectura Integración|
|Aprobado por|Comité de Gobierno de Arquitectura|

## Revisión y aprobación del documento

| Versión | Fecha de revisión | Revisado por | Fecha de aprobación | Aprobado por | Comité, Directorio y N° de acta |
|---|---|---|---|---|---|
| v1.0 | 2026-06-23 | Arquitectura Integración | 2026-06-23 | Comité de Gobierno de Arquitectura | [PENDIENTE-VALIDACION-DUEÑO] |

## Tabla de contenido

- [Propósito](#proposito)
- [Ruta de acceso](#ruta-de-acceso)
    1. [Estructura de carpetas](#type-media-folder)
    2. [Nombre de elemento](#element-name)

## Propósito

Determinar la estructura de carpetas y la clasificación de los elementos multimedia, estos será importante para el despliegue.

## Ruta de Acceso

Considere este formato de enlace: **https://<server-images\>/media/<type-media-folder\>/<element-name\>**

Donde:

### **<type-media-folder\>**

Puede tomar uno de la siguiente estructura

|Nombre|Descripción|
|:--:|:--|
|image|Para objetos de gran volumen como imagen|
|logo|Identifica identidad son de poco tamaño|
|icon|Identifica a elementos de acción para html|
|data|Para objetos que requieran información estática que no cambia como QR|
|element|Asociado a elementos html como barras, separadores, slidebar, etc|

#### **Consideraciones**

1. Únicamente la carpeta ***image*** se debe considerar para objetos de gran volumen, los demás no deben superar los `5KB`
2. De la carpeta **data**, las propiedades de los objetos especificamente los datos en cualquiera de sus dimensiones no deben ser sensibles, estar cifrados, este último, seguridad de la información debe permitir su valor en claro.

### **<element-name\>**

#### Consideraciones

- Use inglés
- El nombre admite únicamente letras, no puede contener números.
- Solo se permite el caracter guión medio.
  Se cita un ejemplo: **family-smile-body**.jpeg
- Use Kebab-Case para citar las propiedades
- El tamaño máximo no debe superar los 30 caracteres, *no considere la extensión*
- No se admiten secuenciales pero si jerarquias por ejemplo **family-smile-first-body.jpeg**, solo para los casos que necesariamente apliquen.
- Se recomienda para el nombrado identificar la parte del documento (header, body, footer), esto para iconos, barras, separadores en si elementos html.

## Checklist de aceptación

- [ ] la clasificación es la correcta
- [ ] se aloja el objeto en el lugar adecuado y designado
- [ ] cumple con el lineamiento de nombrado