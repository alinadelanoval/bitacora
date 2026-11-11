# Contrato observable de Bitácora

## Propósito

Este documento define el comportamiento que observan los clientes de Bitácora:
las operaciones de la API, sus resultados, sus errores y el formato de los
lotes de registros. No determina cómo se implementan el almacenamiento, la
coordinación ni la replicación; esos mecanismos se describen en
[`architecture.md`](architecture.md).

La API de red se define en `proto/bitacora/v1/bitacora.proto`. Este documento
especifica la semántica de sus operaciones.

## Convenciones

- **Debe**, **no debe** y **puede** tienen el significado normativo de
  [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119): obligación,
  prohibición y opción.
- **LEO** (*log end offset*): offset que se asignaría a la siguiente escritura
  en una partición.
- **High watermark**: límite de visibilidad de una partición. Los consumidores
  solo leen registros por debajo de él.

## Versión

- **Versión:** `1.0`, en revisión.
- **Congelada en:** _por completar (commit / etiqueta)_. Tras la congelación,
  cualquier cambio requiere un ADR que justifique el motivo, el efecto en los
  clientes y las pruebas afectadas.
- **Decisiones relacionadas:**
  [ADR-001](decisions/ADR-001-formato-de-lote-en-el-contrato.md), formato de
  lote;
  [ADR-002](decisions/ADR-002-reconciliacion-incremental-de-grupos.md),
  reconciliación incremental de grupos.

## Perfiles de conformidad

El contrato define dos perfiles. Cada implementación declara el perfil que
cumple.

- **Básico:** temas y particiones, escritura y lectura de lotes, offsets,
  grupos de consumidores, progreso confirmado y administración de temas.
- **Completo:** el perfil básico y, además, deduplicación por identidad de
  productor, `InitProducerId`, rechazo de escrituras `ACKS_ALL` por debajo del
  mínimo de réplicas sincronizadas, rechazo de épocas de líder obsoletas y
  autenticación y autorización por token.

Una implementación del perfil básico debe responder el estado gRPC
`UNIMPLEMENTED` a `InitProducerId` y no debe presentar como garantizadas las
propiedades exclusivas del perfil completo. Las pruebas de conformidad de una
regla del perfil completo se informan como «no aplica» contra una
implementación del perfil básico; nunca como superadas.

### Estado de conformidad

| Operación | Perfil | `prototype/` (básico) | `bitacorad` (completo) | Caso en `contracttest/` |
|---|---|---|---|---|
| `Produce` | Básico; deduplicación y mínimo de réplicas: completo | pendiente | pendiente | pendiente |
| `Fetch` | Básico | pendiente | pendiente | pendiente |
| `ListOffsets` | Básico | pendiente | pendiente | pendiente |
| `Heartbeat` | Básico | pendiente | pendiente | pendiente |
| `CommitOffsets`, `FetchCommittedOffsets` | Básico | pendiente | pendiente | pendiente |
| `GetMetadata`, `CreateTopic` | Básico; épocas e ISR: completo | pendiente | pendiente | pendiente |
| `InitProducerId` | Completo | `UNIMPLEMENTED` | pendiente | pendiente |
| `ResetGroupOffsets` | Básico | pendiente | pendiente | pendiente |

«Pendiente» indica que la regla todavía no se ha comprobado, no que sea
opcional.

## Operaciones

El servicio público se divide en `Log`, `Groups` y `Admin`. Los errores de
dominio se devuelven en el campo `ErrorCode` de la respuesta. Los estados de
gRPC se reservan para fallos del canal o del servidor y para operaciones no
implementadas.

| Operación | Precondiciones | Resultado | Errores de dominio | Reintento | Visible después |
|---|---|---|---|---|---|
| `Produce` | Tema y partición existentes; `partition` presente; `acks` especificado; `timeout_ms > 0`; lote válido | Offset del primer registro del lote; `-1` con `ACKS_NONE` | `UNKNOWN_TOPIC_OR_PARTITION`, `INVALID_REQUEST`, `CORRUPT_MESSAGE`, `NOT_LEADER`, `REQUEST_TIMED_OUT`; perfil completo: `NOT_ENOUGH_REPLICAS`, `FENCED_LEADER_EPOCH`, `OUT_OF_ORDER_SEQUENCE`, `DUPLICATE_SEQUENCE` | Con identidad de productor (perfil completo), sin duplicar. Sin identidad, puede duplicar | Cuando el high watermark cubre el lote |
| `Fetch` | Tema y partición existentes; `partition` presente; `max_bytes > 0` | Lotes desde el que contiene `offset`, por debajo del high watermark | `UNKNOWN_TOPIC_OR_PARTITION`, `INVALID_REQUEST`, `OFFSET_OUT_OF_RANGE`, `NOT_LEADER` | Sí | — |
| `ListOffsets` | Partición existente; posición especificada | Primer offset disponible, high watermark o primer offset con timestamp igual o posterior al pedido; `-1` si no existe | `UNKNOWN_TOPIC_OR_PARTITION`, `INVALID_REQUEST` | Sí | — |
| `Heartbeat` | `group_id` y `member_id` válidos; `member_epoch ≥ -1`; al entrar, al menos un tema válido | Época del miembro, intervalo de latido, asignación utilizable y pendiente, época del grupo | `UNKNOWN_MEMBER`, `ILLEGAL_GENERATION`, `INVALID_REQUEST` | Sí, con el mismo `member_id` | En la asignación del miembro |
| `CommitOffsets` | Miembro y época de miembro vigentes; particiones asignadas al miembro | Por partición, el mayor entre el offset confirmado y el pedido | `UNKNOWN_MEMBER`, `ILLEGAL_GENERATION`, `INVALID_REQUEST` | Sí | En `FetchCommittedOffsets` |
| `FetchCommittedOffsets` | — | Offset confirmado por partición; las particiones sin commit no aparecen | `INVALID_REQUEST` | Sí | — |
| `GetMetadata` | — | Líder, época, réplicas e ISR por partición | `UNKNOWN_TOPIC_OR_PARTITION` | Sí | — |
| `CreateTopic` | Nombre válido (ver «Nombres»); `partitions ≥ 1`; factor de replicación válido | Metadatos del tema | `TOPIC_ALREADY_EXISTS`, `INVALID_REPLICATION_FACTOR`, `INVALID_REQUEST` | Sí, con el mismo `request_id` | En `GetMetadata` |
| `InitProducerId` | Perfil completo | Identidad y época del productor | — | Sí, con el mismo `request_id` | — |
| `ResetGroupOffsets` | Grupo vacío | Nuevas posiciones de lectura | `GROUP_NOT_EMPTY`, `INVALID_REQUEST` | Sí, con el mismo `request_id` y posiciones | En `FetchCommittedOffsets` |

Con autenticación (perfil completo), cualquier operación puede devolver además
`ERROR_CODE_PERMISSION_DENIED`.

El servicio `Replication` es interno entre nodos y no forma parte de este
contrato. Sus peticiones requieren credenciales de nodo y no deben exponerse a
los clientes.

## Formato del lote

Los registros viajan en lotes: una cabecera fija de 61 bytes seguida de los
registros. El servidor valida la cabecera y almacena los bytes sin decodificar
los registros; la lectura devuelve los mismos bytes. Una petición `Produce`
contiene un único lote. Los enteros de la cabecera son big-endian.

| Bytes | Campo | Tipo | Valor |
|---|---|---|---|
| 0–7 | `baseOffset` | int64 | El cliente escribe `0`; el líder escribe el offset del primer registro |
| 8–11 | `batchLength` | int32 | Bytes que siguen a este campo |
| 12–15 | `partitionLeaderEpoch` | int32 | El cliente escribe `-1`; el líder escribe su época |
| 16 | `magic` | int8 | Versión del formato: `2` |
| 17–20 | `crc` | uint32 | CRC-32C (Castagnoli) de los bytes desde `attributes` hasta el final |
| 21–22 | `attributes` | int16 | `0` |
| 23–26 | `lastOffsetDelta` | int32 | `recordCount - 1` |
| 27–34 | `baseTimestamp` | int64 | Milisegundos Unix del primer registro |
| 35–42 | `maxTimestamp` | int64 | Mayor timestamp del lote |
| 43–50 | `producerId` | int64 | Identidad del productor; `-1` sin identidad |
| 51–52 | `producerEpoch` | int16 | `-1` sin identidad |
| 53–56 | `baseSequence` | int32 | Secuencia del primer registro; `-1` sin identidad |
| 57–60 | `recordCount` | int32 | Número de registros, `≥ 1` |

`baseOffset` y `partitionLeaderEpoch` quedan fuera del CRC: los escribe el
servidor después de que el cliente lo calcule.

Cada registro tiene esta forma, donde `varint` es un entero de longitud
variable con codificación zigzag:

| Campo | Tipo |
|---|---|
| `length` | varint: bytes restantes del registro |
| `attributes` | int8: `0` |
| `timestampDelta` | varint: timestamp − `baseTimestamp` |
| `offsetDelta` | varint: posición en el lote, desde `0` |
| `keyLength`, `key` | varint y bytes; `-1` sin clave |
| `valueLength`, `value` | varint y bytes; `-1` sin valor |
| `headersCount`, `headers` | varint y, por cabecera: `keyLength`, `key`, `valueLength`, `value` |

El servidor debe rechazar el lote sin escribir ningún registro cuando:

- `magic ≠ 2`, la longitud no coincide con los bytes recibidos o el CRC no
  coincide: `ERROR_CODE_CORRUPT_MESSAGE`;
- `recordCount < 1`, `lastOffsetDelta ≠ recordCount - 1`, `attributes ≠ 0`, o
  la identidad de productor es parcial (unos campos a `-1` y otros no):
  `ERROR_CODE_INVALID_REQUEST`.

## Modelo de datos y posiciones

- Un tema contiene una o más particiones.
- Cada partición es un log ordenado de forma independiente. No existe orden
  global entre particiones.
- Cada registro aceptado recibe un offset dentro de su partición. Los offsets
  empiezan en cero, son contiguos y crecientes, y no se reutilizan. Los
  registros de un lote reciben offsets consecutivos desde `baseOffset`.
- Una lectura desde el offset `n` incluye el registro `n` si sigue disponible.
- El offset confirmado de un grupo es la siguiente posición que el grupo debe
  leer. Confirmarlo no modifica el log.

### Nombres

Los nombres de tema y los identificadores de grupo solo pueden contener letras
ASCII, dígitos, `.`, `_` y `-`, y no pueden ser `.` ni `..`. Un nombre de tema
tiene como máximo 249 caracteres; un `group_id`, como máximo 64. Un nombre que
no cumpla estas reglas se rechaza con `ERROR_CODE_INVALID_REQUEST`. Ninguno
puede contener un separador de rutas, de modo que una implementación puede
usarlos para nombrar archivos o directorios sin riesgo de salir de su
directorio de datos.

## Escritura

| `acks` | Respuesta |
|---|---|
| `ACKS_NONE` | Al recibir el lote, con `base_offset = -1`. El productor no puede concluir que el lote se haya almacenado. |
| `ACKS_LEADER` | Cuando el líder ha escrito el lote. Si el líder falla antes de replicarlo, el lote puede perderse. |
| `ACKS_ALL` | Cuando las réplicas sincronizadas han escrito el lote. En el perfil completo, si las réplicas sincronizadas no alcanzan el mínimo configurado, la escritura falla con `ERROR_CODE_NOT_ENOUGH_REPLICAS`. |
| `ACKS_UNSPECIFIED` | Se rechaza con `ERROR_CODE_INVALID_REQUEST`. |

Ninguna confirmación garantiza la supervivencia ante fallos ajenos al modelo
configurado.

La identidad del productor va en la cabecera del lote. En el perfil completo,
un lote con una secuencia ya aceptada no crea registros nuevos. Un lote sin
identidad no tiene protección contra duplicados.

## Lectura

- Rango válido: `logStartOffset ≤ offset ≤ LEO`.
- Fuera de ese rango, la respuesta es `ERROR_CODE_OFFSET_OUT_OF_RANGE`. La
  posición no se sustituye por otra.
- Si `offset` es igual al LEO o está entre el high watermark y el LEO, el
  servidor espera hasta `max_wait_ms` a que haya registros visibles y, si no
  los hay, responde vacío. Con `max_wait_ms = 0` responde sin esperar.
- La respuesta contiene lotes completos. El primero puede incluir registros
  anteriores a `offset`, que el cliente debe descartar.

Una respuesta vacía indica que no hay registros disponibles en ese momento; no
indica el final del tema.

## Grupos de consumidores

En cada instante, cada partición tiene como máximo un miembro del grupo que
puede usarla. Cada cambio de membresía incrementa la época del grupo y produce
un reparto objetivo con la estrategia `range`. Cada miembro converge a ese
reparto mediante sus latidos: primero deja las particiones que ya no le
corresponden y después recibe las nuevas, a medida que sus miembros anteriores
las dejan. El reparto converge en menos de 15 s en la configuración de
referencia.

- **Entrar, latir y salir.** Las tres acciones usan `Heartbeat` y se
  distinguen por `member_epoch`: `0` para entrar, o volver a entrar sin
  particiones; `-1` para salir; un valor positivo, igual a la época vigente del
  miembro, para latir. El servidor fija el intervalo de latido, que comunica en
  cada respuesta, y el plazo de sesión.
- **Identidad.** El cliente genera su `member_id` antes de entrar y lo reutiliza
  en todos los latidos y reintentos. Repetir una entrada cuya respuesta se
  perdió no crea un segundo miembro.
- **Reconciliación.** En cada latido, el miembro envía todas las particiones
  que tiene. La respuesta indica las que puede usar (`assignment`) y las que le
  corresponden pero otro miembro aún no ha dejado (`pending`). El miembro debe
  dejar de usar cualquier partición que no figure en `assignment`, confirmar su
  progreso en ella y no incluirla en el siguiente latido. Su época de miembro
  avanza a la del grupo cuando ya no tiene particiones que dejar. Un miembro que
  no deja una partición dentro del plazo de reequilibrio sale del grupo.
- **Sesión.** Un miembro sin latidos durante el plazo de sesión sale del grupo y
  sus particiones se reasignan.
- **Épocas obsoletas.** Un latido con una época de miembro distinta de la
  vigente se rechaza con `ERROR_CODE_ILLEGAL_GENERATION`. El miembro debe dejar
  todas sus particiones y volver a entrar con `member_epoch = 0` y el mismo
  `member_id`.
- **Commits.** Un commit se acepta si el miembro pertenece al grupo, su época
  de miembro es la vigente y la partición está en su asignación actual,
  incluidas las que está dejando; si no, se rechaza con
  `ERROR_CODE_UNKNOWN_MEMBER` o `ERROR_CODE_ILLEGAL_GENERATION`. Para cada
  partición se conserva el mayor offset confirmado, de modo que un commit que
  llega tarde no hace retroceder el progreso. El retroceso explícito se hace con
  `ResetGroupOffsets`. El miembro debe confirmar su progreso antes de salir.
- **Progreso.** `FetchCommittedOffsets` omite las particiones sin commit; el
  cliente aplica entonces su política de posición inicial.
- **Salida.** Salir es idempotente: si el miembro ya no pertenece al grupo, la
  respuesta es `ERROR_CODE_NONE`.

## Resultado indeterminado

Una escritura termina de una de tres formas: **correcta** (se conoce su
offset), **fallida** (se sabe que no tuvo efecto) o **indeterminada** (timeout,
conexión interrumpida o respuesta perdida). En el último caso el cliente no
puede deducir si la escritura tuvo efecto.

- Una escritura indeterminada no genera la obligación de durabilidad de
  [I-03](requirements.md#invariantes-del-problema).
- El reintento debe reenviar el lote original. Con identidad de productor
  (perfil completo), el lote aparece a lo sumo una vez (I-08); sin identidad,
  puede aparecer duplicado.

Las propiedades que el sistema debe conservar se recogen en
[`requirements.md`](requirements.md#invariantes-del-problema).

## Errores

La lista de códigos es cerrada:

| Código | Significado | Política del cliente |
|---|---|---|
| `ERROR_CODE_NONE` | Operación correcta. | — |
| `ERROR_CODE_NOT_LEADER` | El nodo no es líder de la partición. | Refrescar metadatos y reintentar. |
| `ERROR_CODE_FENCED_LEADER_EPOCH` | La época de líder indicada es obsoleta. | Refrescar metadatos y reintentar. |
| `ERROR_CODE_NOT_ENOUGH_REPLICAS` | Las réplicas sincronizadas no alcanzan el mínimo. | Reintentar con la misma identidad de productor. |
| `ERROR_CODE_OFFSET_OUT_OF_RANGE` | El offset está fuera de `[logStartOffset, LEO]`. | Corregir la posición. |
| `ERROR_CODE_OUT_OF_ORDER_SEQUENCE` | La secuencia del productor no es la esperada. | Reconciliar el estado del productor. |
| `ERROR_CODE_DUPLICATE_SEQUENCE` | La secuencia ya fue aceptada. | Tratar como éxito con el offset original. |
| `ERROR_CODE_ILLEGAL_GENERATION` | La época de miembro no es la vigente o la partición ya no está asignada al miembro. | Dejar todas las particiones y volver a entrar con `member_epoch = 0`. |
| `ERROR_CODE_UNKNOWN_MEMBER` | El miembro no pertenece al grupo. | Dejar todas las particiones y volver a entrar con `member_epoch = 0`. |
| `ERROR_CODE_REBALANCE_IN_PROGRESS` | Reservado. Ninguna operación lo devuelve: el reequilibrio es incremental. | — |
| `ERROR_CODE_CORRUPT_MESSAGE` | El lote no supera la validación de versión, longitud o CRC. | Corregir el lote. |
| `ERROR_CODE_NOT_CONTROLLER` | El nodo no es el controlador. | Refrescar metadatos y reintentar. |
| `ERROR_CODE_REQUEST_TIMED_OUT` | La operación superó su plazo; puede haber tenido efecto. | Reintentar con la misma identidad. |
| `ERROR_CODE_INVALID_REPLICATION_FACTOR` | El factor de replicación no es válido. | Corregir la petición. |
| `ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION` | El tema o la partición no existen. | Corregir el nombre o refrescar metadatos. |
| `ERROR_CODE_GROUP_NOT_EMPTY` | El grupo tiene miembros activos. | Reintentar cuando el grupo esté vacío. |
| `ERROR_CODE_PERMISSION_DENIED` | El principal no tiene permiso. | Usar credenciales con el rol adecuado. |
| `ERROR_CODE_TOPIC_ALREADY_EXISTS` | Existe un tema con ese nombre, creado con otro `request_id`. | Usar el tema existente o elegir otro nombre. |
| `ERROR_CODE_INVALID_REQUEST` | Falta un campo obligatorio, un valor está fuera de rango o el lote incumple una regla de formato. | Corregir la petición. |

Ante errores temporales, el cliente debe aplicar retroceso exponencial, jitter
y un presupuesto de reintentos. Un timeout no debe interpretarse como rechazo
de la escritura.

## Administración y autenticación

En el perfil completo, cada token identifica un principal con uno de los roles
`producer`, `consumer`, `admin` o `node`. El servidor autentica el token y
autoriza la operación antes de modificar estado o entregar datos.

- El token viaja como metadato de autenticación de la llamada gRPC.
- Un token ausente o inválido, o un rol insuficiente, produce
  `ERROR_CODE_PERMISSION_DENIED` antes de ejecutar la operación.
- Los errores no revelan el token ni información sensible, y las acciones
  administrativas quedan registradas para auditoría.

| Rol | Permisos |
|---|---|
| `producer` | Publicar |
| `consumer` | Leer y operar con grupos |
| `admin` | Consultar y modificar temas y particiones |
| `node` | Comunicación interna entre nodos |

Ningún rol concede las capacidades de otro. La gestión avanzada de
identidades, TLS mutuo y ACL por tema quedan fuera del alcance.

## Conformidad

Cada regla observable debe tener un caso en `contracttest/`, etiquetado con su
perfil. La suite se ejecuta sin modificar sus expectativas contra todas las
implementaciones. Una diferencia entre implementaciones se corrige o se
registra con su alcance y justificación; una prueba no se debilita para
ocultarla.

| # | Comprobación | Perfil |
|---|---|---|
| 1 | Escritura y lectura ordenada dentro de una partición. | Básico |
| 2 | Lectura desde un offset explícito, en el LEO y fuera de rango. | Básico |
| 3 | Búsqueda por fecha y reprocesamiento desde la CLI. | Básico |
| 4 | Reinicio y reanudación desde el progreso confirmado de un grupo. | Básico |
| 5 | Asignación exclusiva, tiempo de reequilibrio, rechazo de commits de generaciones antiguas y progreso que no retrocede. | Básico |
| 6 | Rechazo de lotes con CRC incorrecto y de peticiones con campos obligatorios ausentes. | Básico |
| 7 | Reintento de una escritura cuya respuesta se perdió, sin duplicados. | Completo |
| 8 | Rechazo de escrituras `ACKS_ALL` por debajo del mínimo de réplicas. | Completo |
| 9 | Cambio de líder, recuperación y lectura limitada por el high watermark. | Completo |
| 10 | Rechazo de tokens inválidos o roles insuficientes, sin exponer credenciales en logs. | Completo |

Los cambios que alteren el comportamiento observable deben actualizar la API,
este contrato y las pruebas de conformidad.
