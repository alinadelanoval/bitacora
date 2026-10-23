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
Las operaciones de commit asociadas a una generación obsoleta se rechazan. Un
miembro que retoma el consumo puede solicitar la posición confirmada del grupo
o una posición explícita para reprocesar registros.

El commit guarda la siguiente posición que se leerá, no modifica el log y no
impide que el mismo grupo o un consumidor independiente vuelva a leer
posiciones anteriores.

## Errores y reintentos

Los errores observables distinguen, como mínimo:

- petición o configuración inválida;
- tema o partición inexistente;
- credenciales ausentes o no válidas;
- productor o miembro de grupo no autorizado;
- generación obsoleta o conflicto de asignación;
- falta de réplicas suficientes;
- timeout o indisponibilidad del servicio.

Los errores temporales pueden permitir un reintento. Cuando una operación de
escritura use identidad y secuencia, el reintento debe repetir ambas para
evitar duplicados. Un cliente no debe interpretar un timeout como prueba de que
una escritura fue rechazada.

## Administración y autenticación

Cada token corresponde a un principal y a uno de los roles `producer`,
`consumer`, `admin` o `node`. El servidor autentica el token y autoriza la
operación según el rol antes de modificar estado o entregar datos. Los errores
de autenticación y autorización no revelan el token ni información sensible.
Las acciones administrativas quedan registradas para su auditoría. La gestión
avanzada de identidades, TLS mutuo y ACL por tema no forman parte del alcance.

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
