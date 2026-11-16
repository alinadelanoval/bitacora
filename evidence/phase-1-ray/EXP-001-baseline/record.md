# EXP-001: línea base sin fallos

- **Fase / implementación:** Ray
- **Issue y requisitos:** BIT-16 (#8); RNF-09
- **Propiedades que pone a prueba:** `I-01`, `I-02`, `I-03`, `I-04`, `I-08`
- **Commit / versión:** se anota en cada ejecución (`summary.json`, campo `commit`)
- **Escenario (ID) y workload:** `F0` (sin fallos), `W5`
- **Afirmación bajo prueba:** sin fallos, el prototipo confirma cada escritura
  `acks=all` en un offset único y contiguo, y la lectura final devuelve
  exactamente lo confirmado. Las cifras de rendimiento y latencia son la
  referencia para EXP-002, EXP-003 y la comparación de la Fase 3.

## Predicción (escrita antes de ejecutar)

- **Propiedades:** las cinco `verified`, en todas las ejecuciones. Sin fallos
  no hay escrituras `fail` ni `indeterminate`: la única fuente de error sería
  que una seguidora tardase más que el plazo de replicación (2 s), algo que no
  esperamos con tres réplicas en una máquina sin otra carga.
- **Rendimiento con `acks=all`:** entre 500 y 800 escrituras `ok` por segundo;
  p50 entre 4 y 8 ms y p99 por debajo de 20 ms. Es el orden de la prueba de
  humo de 10 s hecha al terminar el LoadGen (649 ok/s, p50 5,9 ms, p99 10,0 ms),
  no un dato independiente. Cada escritura es un lote de un registro y la
  líder atiende una llamada cada vez (L-07), así que 4 escritores comparten
  una sola cola.
- **Con `acks=leader`:** más rendimiento y menos latencia que con `acks=all`,
  porque la líder no espera a las seguidoras; pero no el doble, porque la
  llamada a la líder y el paso por el gateway siguen en el camino. Esperamos
  entre 1,2 y 2 veces el rendimiento de `acks=all`. I-03 e I-04 quedarán
  `not_evaluable`: ninguna escritura `acks=leader` genera obligación.

- **Fecha y commit de la predicción:** 2026-11-15, sobre `c08efbf`; se versiona
  en el commit que añade este registro, antes de la primera ejecución.

## Configuración y procedimiento

- **Entorno, versiones y nodos:** WSL2 Ubuntu 24.04, un nodo Ray con los tres
  nodos lógicos `n1`–`n3` en la misma máquina (L-04); gateway del prototipo.
- **Configuración y semilla:** `config.yaml` de cada ejecución. Plazo de
  replicación 2000 ms (por defecto), sin `fsync` por lote. El LoadGen no usa
  azar: los `id` son secuenciales.
- **Workload:** `W5` (`tools/loadgen/workloads/w5.json`): 1 partición, RF=3,
  4 escritores, un registro por lote, 30 s, `timeout_ms` 5000; `probe` cada
  segundo con ventana de 500 offsets.
- **Ejecuciones:** 3 con `acks=all` y 3 con `acks=leader`, cada una con una
  carpeta de datos nueva.
- **Disparador y momento del fallo:** ninguno.
- **Comando de reproducción y limpieza:**
  `python scripts/experiments/leader_loss.py --out <carpeta> --acks all|leader`
  (`scripts/experiments/README.md`). Cada ejecución arranca un nodo Ray propio
  con una carpeta de datos temporal, que se borra al terminar.

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

| Ejecución | `acks` | Escrituras `ok` | ok/s | p50 (ms) | p95 (ms) | p99 (ms) | Mayor intervalo sin `ok` (ms) | I-01 | I-02 | I-03 | I-04 | I-08 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| all-1 | all | 21 584 | 719 | 5,3 | 6,8 | 9,3 | 21 | `verified` | `verified` | `verified` | `verified` | `verified` |
| all-2 | all | 21 514 | 717 | 5,3 | 6,6 | 9,1 | 33 | `verified` | `verified` | `verified` | `verified` | `verified` |
| all-3 | all | 21 729 | 724 | 5,3 | 6,5 | 8,9 | 20 | `verified` | `verified` | `verified` | `verified` | `verified` |
| leader-1 | leader | 36 214 | 1 207 | 3,1 | 4,2 | 6,0 | 27 | `verified` | `verified` | `not_evaluable` | `not_evaluable` | `verified` |
| leader-2 | leader | 35 813 | 1 194 | 3,2 | 4,3 | 6,0 | 29 | `verified` | `verified` | `not_evaluable` | `not_evaluable` | `verified` |
| leader-3 | leader | 36 501 | 1 217 | 3,1 | 4,2 | 5,8 | 20 | `verified` | `verified` | `not_evaluable` | `not_evaluable` | `verified` |

- **Resultado observado:** ninguna escritura `fail` ni `indeterminate`. La
  lectura final contiene exactamente las escrituras confirmadas, cada una en el
  offset devuelto. El `probe` releyó 14 000 offsets por ejecución (28
  pasadas con ventana de 500) sin ver ningún cambio.
- **Propiedades:** I-01, I-02 e I-08 `verified` en las 6 ejecuciones; I-03 e
  I-04 `verified` con `acks=all` y `not_evaluable` con `acks=leader`, como se
  predijo.
- **Diferencias con la predicción:** ninguna. Con `acks=all`, 717–724 ok/s
  (predicción: 500–800), p50 5,3 ms (4–8) y p99 8,9–9,3 ms (< 20). Con
  `acks=leader`, 1,67 veces el rendimiento de `acks=all` (predicción: 1,2–2).

## Conclusión e incertidumbre

- **Conclusión:** sin fallos, el prototipo cumple las cinco propiedades
  evaluadas y da la línea base para EXP-002, EXP-003 y la Fase 3: ~720 ok/s
  con `acks=all` y ~1 200 ok/s con `acks=leader`, con lotes de un registro y 4
  escritores. Sin fallos, ningún intervalo sin escrituras confirmadas supera
  33 ms: es la referencia para medir la indisponibilidad en EXP-002 y EXP-003.
- **Límites:** un único host con los tres nodos lógicos (L-04); lotes de un
  registro; una sola partición; 30 s por ejecución; 3 ejecuciones por nivel de
  confirmación. Las cifras de rendimiento solo valen para comparar en esta
  máquina.
- **Incertidumbre restante:** el `probe` registró 0 o 1 pasadas fallidas por
  ejecución también sin fallos (`probe_errors` en `summary.json`); la pasada se
  repite y no afecta al veredicto, pero la causa no se ha investigado.
- **Acciones:** ninguna sobre el prototipo.
