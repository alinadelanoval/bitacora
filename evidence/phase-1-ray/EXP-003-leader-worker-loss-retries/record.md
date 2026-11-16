# EXP-003: pérdida del worker de la líder, con reintentos de Ray

- **Fase / implementación:** Ray
- **Issue y requisitos:** BIT-16 (#8); RF-09, RNF-09
- **Propiedades que pone a prueba:** `I-01`, `I-02`, `I-03`, `I-04`, `I-08`
- **Commit / versión:** se anota en cada ejecución (`summary.json`, campo `commit`)
- **Escenario (ID) y workload:** `F15` con `max_task_retries=3` en las
  réplicas, `W5`
- **Afirmación bajo prueba:** el reintento automático de Ray (L-02) no
  distingue una llamada que ya tuvo efecto: ¿un `append` reintentado escribe
  dos veces el mismo lote?

## Predicción (escrita antes de ejecutar)

Ray reenvía las llamadas interrumpidas al actor reiniciado. Ese actor vuelve
como seguidora, así que un `append` reintentado falla con `NotLeader` sin
escribir nada. Un duplicado solo es posible si el reintento se ejecuta
**después** de que el `Controller` haya vuelto a promover a esa misma réplica.
Eso puede pasar: la réplica reiniciada suele tener el mayor LEO (escribió el
lote antes de enviarlo), y el gateway deja de esperar una llamada a los 5 s
(`timeout_ms`) sin cancelarla en Ray, así que el aviso al `Controller` y la
promoción pueden adelantarse a un reintento todavía en cola.

- **I-08:** esperamos `verified` en la mayoría de ejecuciones y no
  descartamos `violated` (`duplicate`) en alguna. Si aparece un duplicado,
  predecimos su forma: un `id` con resultado `indeterminate`, en dos offsets,
  con la réplica reiniciada como nueva líder en el evento `leader_change`.
  Con 5 ejecuciones no podremos dar una frecuencia; solo si el mecanismo
  existe en la práctica.
- **I-03, I-04:** `verified`, por las mismas razones que en EXP-002: el
  reintento no cambia qué tienen las réplicas del ISR.
- **I-02:** `verified`. Un duplicado ocupa offsets nuevos y contiguos; no
  rompe el orden de la partición, aunque viola I-08.
- **I-01:** `verified`.
- **Escrituras afectadas e indisponibilidad:** como EXP-002. Puede haber
  algún `REQUEST_TIMED_OUT` más, porque el gateway espera mientras Ray
  reintenta.

- **Fecha y commit de la predicción:** 2026-11-15, sobre `c08efbf`; se versiona
  en el commit que añade este registro, antes de la primera ejecución.

## Configuración y procedimiento

- **Entorno, versiones y nodos:** como EXP-001.
- **Configuración y semilla:** `config.yaml` de cada ejecución;
  `max_task_retries=3` en `PartitionReplica` (opción del experimento que se
  añade con su script; el prototipo usa 0).
- **Workload:** `W5` con `acks=all`, 30 s.
- **Disparador y momento del fallo:** como EXP-002, `kill -9` al worker de la
  líder a los 10 s.
- **Ejecuciones:** 5, cada una con una carpeta de datos nueva.
- **Comando de reproducción y limpieza:**
  `python scripts/experiments/leader_loss.py --out <carpeta> --acks all --kill-after 10 --task-retries 3`.

## Evidencia

Cada ejecución está en `runs/<etiqueta>/`:

- `config.yaml`: parámetros, commit (`ad27772`, sin cambios locales), versiones
  y entorno.
- `history.ndjson.gz`: historial del LoadGen. Se descomprime con
  `gunzip -k` y se verifica con
  `go run ./tools/checker -history history.ndjson`, que reproduce
  `checker.json`.
- `summary.json` y `loadgen.txt`: rendimiento, latencias y errores.
- `checker.json` y `checker.txt`: estado de cada propiedad.
- `fault.json`: caída inyectada, si la hay, y mayor intervalo sin escrituras
  `ok`, medido con el reloj monotónico del LoadGen (`elapsed_us`).
- `nodes/`: `events.ndjson` y `controller.json` del `Controller`, y el log del
  gateway.

**Tanda descartada.** Una primera tanda de las mismas ejecuciones, sobre
`6aa363e`, se descartó porque medía la indisponibilidad con la hora de pared:
contaba como primera escritura posterior a la caída una respuesta que ya iba en
camino, y el reloj de WSL2 retrocede ~1 s cada ~35 s, así que ningún intervalo
calculado con `ts` era fiable. Se corrigió en `ad27772` y se repitieron todas
las ejecuciones. Los resultados del Checker de la tanda descartada coinciden
con estos en EXP-001 y EXP-002; en EXP-003 hubo 1 violación de I-08 en 5
ejecuciones, frente a 4 en esta tanda.

## Resultado

| Ejecución | `ok` | `fail` (`NOT_LEADER`) | Cambio de líder tras la caída (s) | Sin `ok` (s) | Nueva líder (LEO n1/n2/n3) | I-02 | I-08 | Anomalías |
|---|---|---|---|---|---|---|---|---|
| run-1 | 20 709 | 0 | 0,31 | 0,918 | n1 (7040/7039/7039) | `violated` | `violated` | `duplicate` y `offset_mismatch` de `w-0007040` |
| run-2 | 20 339 | 4 | 0,31 | 0,958 | n2 (6853/6853/6853) | `verified` | `violated` | `rejected_write_present` de `w-0006853` |
| run-3 | 20 622 | 4 | 0,30 | 0,958 | n2 (6923/6923/6923) | `verified` | `violated` | `rejected_write_present` de `w-0006923` |
| run-4 | 20 472 | 4 | 0,31 | 0,952 | n2 (6828/6828/6828) | `verified` | `verified` | — |
| run-5 | 20 471 | 4 | 0,30 | 0,956 | n2 (6956/6956/6956) | `verified` | `violated` | `rejected_write_present` de `w-0006956` |

I-01, I-03 e I-04 están `verified` en las 5 ejecuciones. Ninguna escritura
quedó `indeterminate`.

- **Resultado observado:** Ray repite las llamadas interrumpidas en la réplica
  reiniciada. Aparecen dos formas de segundo efecto, ambas violaciones de I-08:
  - **Rechazada pero escrita** (run-2, run-3, run-5): la líder había escrito y
    replicado el lote antes de morir (LEO iguales); el reintento llega a la
    réplica reiniciada, que ya es seguidora, y responde `NotLeader`. El gateway
    lo traduce a `NOT_LEADER`, que para el cliente significa «sin efecto», pero
    el lote está en el log (L-14). Un cliente que reintentase lo duplicaría.
  - **Duplicado confirmado** (run-1): la líder `n1` había escrito
    `w-0007040` en el offset 7039 sin replicarlo (LEO 7040 frente a 7039). Al
    reiniciarse, el `Controller` la vuelve a elegir por tener el mayor LEO, y
    el reintento de Ray llega cuando ya es líder: escribe el lote otra vez en
    el 7043 y el cliente recibe `ok` con ese offset. El registro aparece dos
    veces. El `offset_mismatch` de I-02 es la misma causa vista desde el
    offset: el registro está en el offset devuelto, pero también en otro
    anterior.
- **Propiedades:** I-08 `violated` en 4 de 5 ejecuciones; I-02 `violated` en
  1 (como consecuencia del duplicado); I-01, I-03 e I-04 `verified` en todas.
- **Diferencias con la predicción:**
  - Predijimos I-08 `verified` en la mayoría de ejecuciones; fue `violated` en
    4 de 5.
  - No predijimos la forma «rechazada pero escrita», que fue la más frecuente
    (3 de 5).
  - El duplicado apareció con el mecanismo predicho (la réplica reiniciada
    vuelve a ser líder antes del reintento), pero el cliente lo recibió como
    `ok`, no como `indeterminate`: no hay ninguna señal de que exista una copia
    de más.
  - Predijimos I-02 `verified`; el duplicado lo rompe por `offset_mismatch`.
  - Predijimos una indisponibilidad como la de EXP-002; fue la mitad
    (0,92–0,96 s). El reintento hace que la réplica reiniciada responda pronto,
    y el `Controller` promueve a los ~0,3 s en lugar de ~1,9 s. No hemos
    determinado por qué las escrituras afectadas tardan aun así ~0,95 s en
    responder.

## Conclusión e incertidumbre

- **Conclusión:** el reintento automático de Ray (`max_task_retries` > 0) no
  es seguro para una operación con efecto como `append`: en 4 de 5 caídas
  produjo un segundo efecto que el cliente no puede detectar, bien un lote
  escrito dos veces y confirmado, bien un lote escrito pero comunicado como
  rechazado. Confirma la decisión del prototipo de usar
  `max_task_retries=0` (L-02) y muestra que la traducción de `NotLeader` del
  gateway depende de ella (L-14). Lo confirmado no se pierde (I-03) ni se
  contradice (I-04).
- **Límites:** como EXP-002 (L-04, una caída por ejecución, siempre de `n1` a
  los 10 s). 5 ejecuciones bastan para mostrar que el defecto existe y no es
  raro, no para estimar su frecuencia (la tanda descartada dio 1 de 5).
- **Incertidumbre restante:** el orden en que Ray ejecuta los reintentos
  frente a `become_leader` no está garantizado; con otro tiempo de reinicio
  podrían aparecer duplicados en los que el cliente reciba `REQUEST_TIMED_OUT`.
  No se ha medido con `acks=leader`.
- **Acciones:** el prototipo mantiene `max_task_retries=0`. Corrección
  candidata para una rama aparte: que el gateway responda `REQUEST_TIMED_OUT`
  ante `NotLeader` en `Produce` cuando las réplicas reintentan, o que no
  arranque con reintentos. Para el diseño nativo: identidad de productor con
  secuencia y deduplicación en la líder (perfil completo del contrato), y
  ningún reintento de operaciones con efecto en el runtime.
