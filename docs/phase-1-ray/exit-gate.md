# Fase 1 — Criterios de cierre y declaración de salida

| | |
|---|---|
| **Alcance** | Cierre de la Fase 1 y paso a la implementación nativa |
| **Estado** | Declaración redactada; pendiente de la revisión de las dos integrantes (C-06) |
| **Hito** | R0 |
| **Documentos relacionados** | [`dependencies.md`](dependencies.md) · [`contract.md`](../contract.md) · [`requirements.md`](../requirements.md) |

Este documento recoge las condiciones que deben cumplirse para cerrar la
Fase 1 y la declaración que lo acredita. Se completa al final de la fase,
sobre los resultados observados; no contiene respuestas anticipadas.

## 1. Criterios de cierre

| # | Criterio | Evidencia | Estado |
|---|---|---|---|
| C-01 | El prototipo se ejecuta sobre varios procesos worker y atiende `Produce`, `Fetch`, `ListOffsets` y las operaciones de grupo | `prototype/`, suite de `contracttest/` contra el gateway | Cumplido: `make contract`, 13 de 13 en el perfil básico; `test_three_nodes.py` y `test_multinode.py` sobre tres nodos Ray |
| C-02 | El diagrama de arquitectura corresponde al código ejecutado | [`dependencies.md`](dependencies.md) §2 | Cumplido: cada componente del diagrama es un módulo de `prototype/` (gateway, `Controller`, `PartitionReplica`, `GroupCoordinator`), y los experimentos ejercitan ese camino completo |
| C-03 | Cada fila del mapa de dependencias tiene verificación asociada | [`dependencies.md`](dependencies.md) §5 | Cumplido: 14 de 14 filas |
| C-04 | Se ha ejecutado al menos un experimento de fallo con la predicción versionada antes de ejecutar, y el estado de cada invariante evaluado (`verified`, `violated` o `not_evaluable`) | `evidence/phase-1-ray/` | Cumplido: `EXP-001` a `EXP-003`, predicción en `dddd910` |
| C-05 | El contrato está completo para las operaciones del prototipo y sin cuestiones abiertas | [`contract.md`](../contract.md) | Cumplido: sin cuestiones abiertas; los experimentos no piden cambios en el contrato (L-14 es un defecto del prototipo) |
| C-06 | La declaración de salida (§2) está respondida y revisada por las dos integrantes | Este documento | Pendiente: revisión cruzada |

## 2. Declaración de salida

### 2.1 Problema que resuelve la aplicación

Bitácora es un registro distribuido de eventos: los productores escriben
registros en temas particionados y los consumidores los leen de forma
independiente, en el orden de cada partición. El sistema persiste y replica
los registros, mantiene el progreso de los grupos de consumidores y debe poder
demostrar qué conserva ante fallos
([`requirements.md`](../requirements.md#propósito), RF-01 a RF-11). El
prototipo implementa el perfil básico de RF-01 a RF-08 y RF-11, no los
requisitos completos. Quedan fuera: las 100 terminaciones abruptas de RF-04;
el mínimo de réplicas sincronizadas y el rechazo de un líder obsoleto de RF-05
(L-01; el failover observado, ~1,9 s, sí cumple el límite de 10 s); la CLI de
reprocesamiento de RF-08, y la falta de autorización de RF-11. RF-09
(reintentos seguros) y RF-10 (autenticación) son del perfil completo y solo
existirán en la versión nativa.

### 2.2 Comportamiento externo que se preserva

El contrato `bitacora.v1` ([`contract.md`](../contract.md)) es la frontera: la
versión nativa debe superar la misma suite `contracttest/` que hoy supera el
gateway, sin relajar ninguna prueba.

- **Se mantiene tal cual (perfil básico):** `Produce`, `Fetch` y
  `ListOffsets`; `Heartbeat`, `CommitOffsets` y `FetchCommittedOffsets`;
  `GetMetadata`, `CreateTopic` (con `request_id`) y `ResetGroupOffsets`. El
  formato de lote ([ADR-001](../decisions/ADR-001-formato-de-lote-en-el-contrato.md)),
  la reconciliación incremental de grupos
  ([ADR-002](../decisions/ADR-002-reconciliacion-incremental-de-grupos.md)),
  las reglas de nombres, la lista cerrada de errores y los tres resultados de
  una escritura: correcta, fallida o indeterminada.
- **Solo en la versión nativa (perfil completo):** identidad de productor con
  deduplicación (`InitProducerId`), rechazo de `ACKS_ALL` por debajo del
  mínimo de réplicas sincronizadas, rechazo de épocas de líder obsoletas y
  autenticación por token. El prototipo responde `UNIMPLEMENTED` a
  `InitProducerId` y no presenta esas propiedades como garantizadas.

### 2.3 Comportamiento que aporta el código de Bitácora

Filas del [mapa](dependencies.md#5-mapa-de-dependencias-del-middleware) cuyo
proveedor es la aplicación, sola o junto con Ray. Son las que la versión
nativa puede reutilizar como diseño, no como código:

- **Asignación de offsets** en la líder (orden dentro de una partición).
- **Estado durable de cada réplica**: archivo por réplica, reconstrucción al
  reiniciar y truncado de un lote incompleto.
- **Replicación y conjunto sincronizado**: la líder envía los lotes, el HW es
  el menor LEO del ISR y una réplica solo sale del ISR con permiso del
  `Controller`.
- **Promoción de líder**: el `Controller` comprueba la líder y promueve la
  réplica viva del ISR con mayor LEO.
- **Coordinación de grupos**: épocas, asignación `range`, offsets confirmados
  en archivo y expulsión por sesión vencida.
- **Observabilidad**: eventos JSON de cambios de líder e ISR y transiciones y
  métricas de los coordinadores.

### 2.4 Comportamiento que aporta Ray

Filas cuyo proveedor es el runtime, con la pregunta que cada una deja abierta
para la versión nativa:

| Lo que aporta Ray | Pregunta de diseño |
|---|---|
| Ejecución de los métodos de un actor de uno en uno (orden) | Qué proceso serializa las escrituras de una partición y cómo |
| Invocación remota entre gateway y actores | Protocolo explícito con plazos e identificadores de petición |
| Ubicación de réplicas por recursos lógicos | Asignación determinista o planificación dinámica |
| Descubrimiento por nombre de actor | Arranque desde semillas y metadatos servidos por el clúster |
| Detección de la muerte de un proceso | Latidos, sesiones y criterio de sospecha (L-06) |
| Reinicio de actores y reintento de tareas | Identidad de productor y deduplicación; ningún reintento de operaciones con efecto en el runtime (L-02, L-14) |
| Resultado síncrono de una llamada | Respuesta síncrona con el offset, sin almacén de objetos |
| Serialización entre procesos | Protobuf en la red y formato binario propio en disco |
| Cola de llamadas sin límite | Colas acotadas, plazos y presupuesto de reintentos |

### 2.5 Semántica de fallo observada

- **[`EXP-001`](../../evidence/phase-1-ray/EXP-001-baseline/record.md),
  sin fallos.** I-01, I-02, I-03, I-04 e I-08 `verified` (I-03 e I-04
  `not_evaluable` con `acks=leader`). ~720 ok/s con `acks=all` y ~1 200 con
  `acks=leader`; ningún intervalo sin escrituras confirmadas supera 33 ms.
- **[`EXP-002`](../../evidence/phase-1-ray/EXP-002-leader-worker-loss/record.md),
  `kill -9` del proceso de la líder (F1 / F15), sin reintentos.** Todas las
  propiedades `verified` en 8 ejecuciones. Sobrevive todo lo confirmado con
  `acks=all`, en su offset; las escrituras en curso quedan `indeterminate`, ni
  confirmadas ni rechazadas en falso. El cliente pasa 1,93–1,97 s sin
  escrituras, lo que tarda Ray en dar el proceso por muerto.
- **[`EXP-003`](../../evidence/phase-1-ray/EXP-003-leader-worker-loss-retries/record.md),
  el mismo fallo con `max_task_retries=3`.** I-08 `violated` en 4 de 5
  ejecuciones: tres escrituras comunicadas como rechazadas que están en el
  log y un lote escrito dos veces y confirmado, que también rompe I-02. I-03 e
  I-04 `verified`. El reintento del runtime no distingue operaciones con
  efecto.
- **Límites de estas conclusiones:** un único host (L-04), fallos de proceso y
  no de red ni de disco, una caída por ejecución y sin `fsync` por lote. L-01
  (épocas de líder) sigue sin ponerse a prueba: requiere una líder pausada o
  aislada que siga aceptando escrituras.

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
