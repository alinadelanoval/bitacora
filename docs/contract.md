# Contrato observable de Bitácora

## Propósito

Este documento define el comportamiento que pueden observar los clientes de
Bitácora. Se aplica al prototipo Ray y al servidor nativo Go. Las dos
implementaciones deben ofrecer las mismas reglas para las operaciones que
comparten.

La API de red se versiona en `proto/bitacora/v1/`. Este contrato especifica el
significado de sus operaciones; no determina cómo se implementan el
almacenamiento, la coordinación ni la replicación. Esos mecanismos se
describen en [`architecture.md`](architecture.md).

## Operaciones de la API

El servicio público se divide en `Log`, `Groups` y `Admin`. Los errores de
dominio se devuelven mediante el `ErrorCode` de la respuesta; los errores de
transporte de gRPC quedan reservados para fallos del canal o del servidor.
`Replication` es un servicio interno entre nodos y no forma parte de la API
estable del cliente.

| Operación | Servicio | Resultado visible | Reintento |
|---|---|---|---|
| `Produce` | `Log` | Offset base del lote aceptado y posición inicial del log. | Sí cuando se conserva la identidad y secuencia del productor; la respuesta perdida no implica rechazo. |
| `Fetch` | `Log` | Registros desde el offset pedido, en orden y hasta el high watermark. Puede esperar hasta `max_wait_ms`. | Sí; no modifica el log. |
| `ListOffsets` | `Log` | Offset earliest, latest o del primer registro con timestamp igual o posterior al solicitado. | Sí; no modifica el estado. |
| `JoinGroup` | `Groups` | Identidad del miembro, generación y asignación vigente. | Sí; devuelve la generación vigente. |
| `Heartbeat` | `Groups` | Aceptación del latido o error de miembro/generación. | Sí mientras el miembro y la generación sigan vigentes. |
| `LeaveGroup` | `Groups` | Confirmación de salida del miembro. | Sí; repetir la salida no debe duplicar miembros. |
| `CommitOffsets` | `Groups` | Confirmación de los siguientes offsets de lectura. | Sí; confirmar los mismos valores es idempotente. |
| `FetchCommittedOffsets` | `Groups` | Último offset confirmado para cada partición solicitada. | Sí; no modifica el estado. |
| `GetMetadata` | `Admin` | Líder, época, réplicas e ISR por partición. | Sí; es una consulta. |
| `CreateTopic` | `Admin` | Metadatos del tema creado. `request_id` permite reconocer el reintento de la misma mutación. | Sí con el mismo `request_id`. |
| `InitProducerId` | `Admin` | Identidad y época asignadas al productor. | Sí con el mismo `request_id`. |
| `ResetGroupOffsets` | `Admin` | Confirmación de las nuevas posiciones de lectura solicitadas. | Sí con el mismo `request_id` y posiciones. |

El servicio interno `Replication` contiene `ReplicaFetch`,
`OffsetForLeaderEpoch`, `BrokerHeartbeat` y `AlterPartition`. Sus operaciones,
mensajes y reglas pueden cambiar sin romper el contrato de cliente. Las
peticiones de ese servicio requieren credenciales de nodo; no deben exponerse
como operaciones de cliente.

La identidad de productor (`producer_id`, época y secuencia) forma parte de
`Produce`. La deduplicación persistente es una garantía de las fases nativas;
el prototipo Ray de Fase 1 no la presupone y debe medir/documentar si un
reintento produce duplicados. Del mismo modo, en Ray `acks=all` espera a las
réplicas vivas del prototipo y no equivale aún a la garantía nativa basada en
ISR, high watermark y fencing por época de líder.

## Modelo de datos y posiciones

- Un tema contiene una o más particiones.
- Cada partición es un log ordenado de forma independiente. No se garantiza un
  orden global entre particiones.
- Cada registro aceptado recibe un offset entero dentro de su partición. Los
  offsets empiezan en cero, son contiguos y crecientes, y no se reutilizan.
- El offset identifica un registro. Una petición de lectura desde el offset
  `n` incluye el registro `n` si todavía está disponible.
- El offset de fin representa la siguiente posición que se asignaría a una
  escritura. Una lectura desde ese offset no devuelve registros hasta que
  haya nuevas escrituras visibles.
- Los offsets confirmados por un grupo identifican la siguiente posición que
  ese grupo debe leer. Confirmar un offset no elimina ni modifica registros.
- La lectura de registros replicados no avanza más allá del límite de
  visibilidad comunicado por el líder vigente.

## Escritura

La operación de escritura añade registros a una partición y devuelve su
posición cuando la API puede confirmar el resultado según el nivel de
confirmación solicitado.

| Nivel | Resultado |
|---|---|
| `acks=0` | El productor no espera una confirmación del servidor y no puede concluir que el registro se haya almacenado. |
| `acks=1` | La escritura se confirma cuando el líder vigente la ha aceptado localmente. Una pérdida del líder antes de replicarla puede hacer que el registro no sobreviva. |
| `acks=all` | La escritura se confirma cuando la cumplen las réplicas sincronizadas requeridas por la configuración. Si el número de réplicas disponibles cae por debajo del mínimo configurado, la escritura falla en lugar de confirmar una garantía que no puede cumplir. |

El nivel de confirmación y la configuración de replicación determinan la
durabilidad; ninguna confirmación garantiza supervivencia ante fallos ajenos al
modelo configurado.

Las escrituras del productor se identifican mediante una identidad y una
secuencia por partición. Repetir una secuencia ya aceptada no crea un segundo
registro. Un timeout o una conexión interrumpida no permite al productor
deducir si la escritura tuvo efecto; el reintento debe conservar la identidad
y la secuencia originales.

## Lectura

Una petición de lectura identifica tema, partición, offset inicial y límites
de cantidad o tamaño. Los registros se devuelven en orden creciente de offset
y no superan el límite de visibilidad de la partición.

Una respuesta vacía indica que no hay registros disponibles en ese momento
dentro del límite solicitado; no indica el final permanente del tema. Una
petición con una posición inválida o con un recurso inexistente devuelve el
error correspondiente.

El consumidor puede comenzar desde el primer registro disponible, desde el
offset de fin o desde un offset explícito. Si el offset solicitado ya no está
disponible, la operación comunica el error o posición válida definida por la
API; no lo sustituye silenciosamente por otra posición.

Una lectura por fecha devuelve el primer offset cuyo timestamp sea igual o
posterior a la fecha solicitada. Si no existe tal registro, la respuesta indica
que no hay una posición disponible para esa fecha.

## Grupos de consumidores

Los miembros de un grupo comparten el progreso confirmado y reciben una
asignación de particiones. En una misma generación activa, cada partición se
asigna como máximo a un miembro del grupo.

Los cambios de membresía generan una nueva asignación y una nueva generación.
La asignación se completa en menos de 15 s en la configuración de referencia.
Un `JoinGroup` sin `member_id` solicita una identidad nueva; uno con identidad
vigente solicita reincorporar ese miembro. Las operaciones de commit asociadas
a una generación obsoleta se rechazan. Un miembro que retoma el consumo puede
solicitar la posición confirmada del grupo o una posición explícita para
reprocesar registros.

El commit guarda la siguiente posición que se leerá, no modifica el log y no
impide que el mismo grupo o un consumidor independiente vuelva a leer
posiciones anteriores.

## Errores y reintentos

La lista de códigos de dominio es cerrada y se define en
`proto/bitacora/v1/bitacora.proto`:

| Código | Significado | Política general |
|---|---|---|
| `ERROR_CODE_NONE` | Operación correcta. | No aplica. |
| `ERROR_CODE_NOT_LEADER` | El nodo no es líder de la partición. | Reintentar tras refrescar metadatos. |
| `ERROR_CODE_FENCED_LEADER_EPOCH` | La época del líder solicitante es obsoleta. | Refrescar metadatos antes de reintentar. |
| `ERROR_CODE_NOT_ENOUGH_REPLICAS` | No se cumple el mínimo de réplicas sincronizadas. | Reintentable si el cliente conserva la identidad de escritura. |
| `ERROR_CODE_OFFSET_OUT_OF_RANGE` | El offset solicitado no es válido o ya no está disponible. | No reintentar sin corregir la posición. |
| `ERROR_CODE_OUT_OF_ORDER_SEQUENCE` | La secuencia del productor no es la esperada. | No reintentar sin reconciliar el estado del productor. |
| `ERROR_CODE_DUPLICATE_SEQUENCE` | La secuencia ya fue aceptada. | Tratar como éxito con el offset original. |
| `ERROR_CODE_ILLEGAL_GENERATION` | La generación del grupo ya no es vigente. | Reunirse al grupo y usar la generación nueva. |
| `ERROR_CODE_UNKNOWN_MEMBER` | El miembro no pertenece al grupo. | Volver a unirse al grupo. |
| `ERROR_CODE_REBALANCE_IN_PROGRESS` | El grupo está cambiando su asignación. | Reintentable después de completar el rebalanceo. |
| `ERROR_CODE_CORRUPT_MESSAGE` | El lote no supera la validación de integridad. | No reintentar sin corregir el lote. |
| `ERROR_CODE_NOT_CONTROLLER` | El nodo no es el controlador. | Refrescar metadatos y reintentar. |
| `ERROR_CODE_REQUEST_TIMED_OUT` | La operación excedió su plazo; puede haber tenido efecto. | Reintentar con la misma identidad de petición/escritura. |
| `ERROR_CODE_INVALID_REPLICATION_FACTOR` | El factor de replicación solicitado no es válido. | No reintentar sin corregir la petición. |
| `ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION` | No existe el tema o la partición solicitada. | No reintentar sin corregir metadatos o nombre. |
| `ERROR_CODE_GROUP_NOT_EMPTY` | No se puede aplicar la operación porque el grupo tiene miembros activos. | Reintentable después de vaciar el grupo. |
| `ERROR_CODE_PERMISSION_DENIED` | El principal no tiene permiso para la operación. | No reintentar sin credenciales/rol autorizados. |

Los errores temporales pueden permitir un reintento. El cliente debe aplicar
retroceso exponencial, jitter y un presupuesto de reintentos. Nunca debe
interpretar un timeout como prueba de que una escritura fue rechazada.

## Administración y autenticación

Cada token corresponde a un principal y a uno de los roles `producer`,
`consumer`, `admin` o `node`. El servidor autentica el token y autoriza la
operación según el rol antes de modificar estado o entregar datos. Los errores
de autenticación y autorización no revelan el token ni información sensible.
Las acciones administrativas quedan registradas para su auditoría. La gestión
avanzada de identidades, TLS mutuo y ACL por tema no forman parte del alcance.

El cliente presenta su token como metadato de autenticación en las llamadas
gRPC. Un token ausente o inválido, o un rol insuficiente, produce
`ERROR_CODE_PERMISSION_DENIED` antes de ejecutar la operación. Esta capacidad
se implementa en BIT-86 de la fase nativa; el prototipo inicial no debe
presentar como demostrada una garantía de autenticación que aún no implementa.

`producer` puede publicar; `consumer` puede leer y operar con grupos;
`admin` puede consultar y modificar la configuración de temas y particiones;
`node` se reserva a la comunicación interna entre procesos. Ningún rol concede
por sí solo las capacidades de otro.

Las operaciones administrativas permiten crear y consultar los temas y
particiones incluidos en el alcance. La CLI permite solicitar el reprocesamiento
desde el inicio, el final, un offset o una fecha, sin modificar el log.

## Conformidad

Cada regla observable debe contar con un caso en `contracttest/`. La suite se
ejecuta sin alterar sus expectativas contra Ray y Go para las operaciones
implementadas en ambas fases.

Como mínimo, las pruebas comprueban:

1. Escritura y lectura ordenada dentro de una partición.
2. Lectura desde offset explícito y comportamiento en el offset de fin.
3. Búsqueda por fecha y reprocesamiento desde la CLI.
4. Reinicio y reanudación desde el progreso confirmado de un grupo.
5. Exclusividad de asignación, tiempo de rebalanceo y rechazo de commits de
   generaciones antiguas.
6. Reintento de una escritura cuya respuesta se perdió, sin crear duplicados.
7. Rechazo de escrituras `acks=all` cuando no se alcanza el mínimo de réplicas.
8. Cambio de líder, recuperación y lectura limitada por el límite de visibilidad.
9. Rechazo de solicitudes con tokens inválidos o roles insuficientes, sin
   exponer credenciales en logs.

Una diferencia entre implementaciones debe corregirse o registrarse con su
alcance y justificación; no se debe debilitar una prueba para ocultarla. Los
cambios que alteren el comportamiento del cliente deben actualizar la API,
este contrato y las pruebas de conformidad.
