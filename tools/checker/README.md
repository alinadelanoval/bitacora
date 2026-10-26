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

Las propiedades de arquitectura (`AI-*`) que se verifican con logs de nodos
(por ejemplo, `AI-02`) se comprueban con el mismo informe cuando se aportan
esos logs.
