# Checker

Verificador de invariantes sobre el historial de una ejecución. Comprueba las
propiedades de [`docs/requirements.md`](../../docs/requirements.md#invariantes-del-problema)
contra lo que el sistema confirmó y devolvió; no declara garantías que no pueda
verificar.

## Entrada: historial

El productor registra, por cada escritura (NDJSON):

| Campo | Contenido |
|---|---|
| `id` | Identificador lógico único del mensaje |
| `key`, `partition` | Clave y partición |
| `offset` | Offset devuelto por `Produce` |
| `acks` | Nivel de confirmación solicitado |
| `result` | `ok`, `fail` o `indeterminate` (timeout o conexión interrumpida) |
| `ts` | Marca de tiempo local |

Un grupo nuevo realiza la **lectura final** desde el inicio hasta el límite de
visibilidad **después de sanar el fallo**. Los consumidores registran también
las lecturas realizadas durante el experimento y los commits de offsets.

Solo las escrituras `ok` generan obligación. Una escritura `indeterminate` puede
aparecer en el log, a lo sumo una vez.

### Formato (versión mínima, BIT-16)

Una línea JSON por evento, con `op`, `ts` (RFC 3339), `topic` y `partition`.
El paquete [`tools/history`](../history/history.go) lo escribe y lo lee, y
rechaza las líneas mal formadas o con campos desconocidos.

| `op` | Campos propios | Significado |
|---|---|---|
| `produce` | `id`, `key`, `acks` (`none`, `leader`, `all`), `result`, `offset` (`-1` si no es `ok`), `error`, `latency_us` | Una escritura y su resultado. El valor del registro es su `id`. |
| `read` | `reader`, `offset`, `value` | Un registro leído. La lectura final usa `reader: "final"`; las lecturas durante el experimento usan otro nombre, por ejemplo `probe`. |
| `final_read_done` | `start_offset`, `high_watermark` | La lectura final de la partición llegó a su HW: todo offset de `[start_offset, high_watermark)` tuvo que leerse. |

Reglas que aplica la versión mínima:

- Solo una escritura `ok` con `acks: "all"` genera la obligación de I-03 y
  cuenta como reconocida en I-04. Con `ACKS_LEADER` el lote puede perderse si
  el líder cae antes de replicarlo ([contrato](../../docs/contract.md)).
- I-01 se evalúa cuando un mismo lector relee un offset; I-02, I-03 e I-08
  necesitan la lectura final completa (`final_read_done`). Sin ellas, la
  propiedad queda `not_evaluable`.
- I-08: el generador mínimo envía cada `id` una sola vez y no reintenta, así que
  un `id` en dos offsets es un segundo efecto del sistema (por ejemplo, un
  reintento interno de Ray). Una escritura `fail` presente en el log también
  viola I-08: el cliente que la reintentase la duplicaría.

## Anomalías

| Anomalía | Detecta | Propiedad |
|---|---|---|
| `log_mutation` | Distinto valor para el mismo (partición, offset) en lecturas repetidas | I-01 |
| `offset_gap`, `offset_duplicate` | Huecos o repeticiones de offsets en una partición | I-02 |
| `value_divergence` | Un mismo (partición, offset) con dos valores en lecturas distintas | I-02 |
| `offset_mismatch` | El offset devuelto por `Produce` no es aquel en que se lee el registro | I-02 |
| `lost_write` | Registro `ok` con confirmación durable ausente en la lectura final, o con otro offset o valor | I-03 |
| `divergent_ack` | Dos escrituras reconocidas distintas en el mismo (partición, offset); confirmaciones aceptadas durante una partición por un lado sin autoridad | I-04 |
| `position_lost`, `silent_reset` | Posición confirmada no recuperada tras un reinicio, o sustituida sin una operación explícita | I-06 |
| `dual_owner` | Dos propietarios de la misma partición en un mismo instante (requiere logs del coordinador) | I-07 |
| `duplicate` | Identificador lógico con más de un registro bajo identidad vigente | I-08 |
| `rejected_write_present` | Escritura con resultado `fail` (el sistema dijo que no tuvo efecto) presente en la lectura final; un reintento del cliente la duplicaría | I-08 |
| `unauthorized_effect` | Cambio de estado tras una operación rechazada por rol | I-09 |

I-05 (integridad) no se evalúa con el historial: se comprueba con las pruebas
del almacenamiento (bytes basura, terminación abrupta durante la escritura).

## Salida

Un informe en JSON y en texto con **el estado de cada propiedad**:

- `verified`: comprobada, con el escenario y la ejecución.
- `violated`: con la anomalía, los registros implicados y su ubicación en el historial.
- `not_evaluable`: la propiedad no se decide con este historial; se indica qué prueba la cubre.

Nunca se emite un resultado global sin el desglose. El código de salida es
distinto de cero si alguna propiedad evaluada está `violated`.

```sh
go run ./tools/checker -history history.ndjson -json checker.json
```

Sale con `0` si ninguna propiedad está violada, `1` si alguna lo está y `2` si
el historial no se puede leer. La versión mínima evalúa I-01, I-02, I-03, I-04
e I-08; BIT-90 añade el resto.

Las propiedades de arquitectura (`AI-*`) que se verifican con logs de nodos
(por ejemplo, `AI-02`) se comprueban con el mismo informe cuando se aportan
esos logs.
