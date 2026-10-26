# Fase 1 — Criterios de cierre y declaración de salida

| | |
|---|---|
| **Alcance** | Cierre de la Fase 1 y paso a la implementación nativa |
| **Estado** | Sin completar |
| **Hito** | R0 |
| **Documentos relacionados** | [`dependencies.md`](dependencies.md) · [`contract.md`](../contract.md) · [`requirements.md`](../requirements.md) |

Este documento recoge las condiciones que deben cumplirse para cerrar la
Fase 1 y la declaración que lo acredita. Se completa al final de la fase,
sobre los resultados observados; no contiene respuestas anticipadas.

## 1. Criterios de cierre

| # | Criterio | Evidencia | Estado |
|---|---|---|---|
| C-01 | El prototipo se ejecuta sobre varios procesos worker y atiende `Produce`, `Fetch`, `ListOffsets` y las operaciones de grupo | `prototype/`, suite de `contracttest/` contra el gateway | Pendiente |
| C-02 | El diagrama de arquitectura corresponde al código ejecutado | [`dependencies.md`](dependencies.md) §2 | Pendiente |
| C-03 | Cada fila del mapa de dependencias tiene verificación asociada | [`dependencies.md`](dependencies.md) §5 | Pendiente |
| C-04 | Se ha ejecutado al menos un experimento de fallo con la predicción versionada antes de ejecutar, y el estado de cada invariante evaluado (`verified`, `violated` o `not_evaluable`) | `evidence/phase-1-ray/` | Pendiente |
| C-05 | El contrato está completo para las operaciones del prototipo y sin cuestiones abiertas | [`contract.md`](../contract.md) | Pendiente |
| C-06 | La declaración de salida (§2) está respondida y revisada por las dos integrantes | Este documento | Pendiente |

## 2. Declaración de salida

### 2.1 Problema que resuelve la aplicación

*Pendiente.* Resumen en pocas líneas, con enlace a los requisitos que cubre.

### 2.2 Comportamiento externo que se preserva

*Pendiente.* Operaciones y reglas del contrato que se mantienen en la versión
nativa, e indicación de las que solo existirán en ella (identidad de productor,
autenticación, `acks=all` basado en el conjunto sincronizado).

### 2.3 Comportamiento que aporta el código de Bitácora

*Pendiente.* Filas del mapa de dependencias cuyo proveedor es la aplicación.

### 2.4 Comportamiento que aporta Ray

*Pendiente.* Filas del mapa de dependencias cuyo proveedor es el runtime, con
la pregunta de diseño que cada una deja abierta.

### 2.5 Semántica de fallo observada

*Pendiente.* Para cada experimento: fallo provocado, resultado, estado de los
invariantes evaluados (`I-xx`), estado que sobrevivió y límites de la
conclusión. Enlaces a `evidence/phase-1-ray/`.

## 3. Congelación del contrato

Con la declaración completada, el contrato se marca como versión estable.

| Versión | Fecha | Referencia (commit o etiqueta) | Motivo |
|---|---|---|---|
| 1.0 | Pendiente | Pendiente | Cierre de la Fase 1 |

Los cambios posteriores se tramitan mediante un ADR que describa el motivo, el
efecto sobre los clientes existentes y las pruebas de conformidad afectadas.
Una prueba de `contracttest/` no se relaja para acomodar una implementación.

## 4. Retirada de Ray

A partir del cierre:

- Ray no forma parte de las dependencias de ejecución de la implementación
  nativa, ni directa ni a través de un componente que delegue el comportamiento
  distribuido central.
- `prototype/` se conserva como referencia para la comparación Ray / nativo y
  no se modifica salvo para corregir defectos que afecten a esa comparación.
- Cada pregunta de la última columna del mapa de dependencias queda asociada a
  una decisión en [`docs/decisions/`](../decisions/README.md) o a una omisión
  justificada.
