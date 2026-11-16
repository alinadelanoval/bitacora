# EXP-002: pérdida del worker de la líder, sin reintentos

- **Fase / implementación:** Ray
- **Issue y requisitos:** BIT-16 (#8); RF-05, RF-09, RNF-09
- **Propiedades que pone a prueba:** `I-01`, `I-02`, `I-03`, `I-04`, `I-08`
- **Commit / versión:** se anota en cada ejecución (`summary.json`, campo `commit`)
- **Escenario (ID) y workload:** `F1` / `F15` (terminación abrupta del proceso
  worker que aloja la réplica líder), `W5`
- **Afirmación bajo prueba:** si el proceso de la líder muere en mitad de la
  carga, el `Controller` promueve a otra réplica del ISR y ninguna escritura
  confirmada con `acks=all` se pierde, cambia de offset o queda contradicha.
  Las escrituras en curso quedan indeterminadas, no falsamente confirmadas ni
  falsamente rechazadas.

## Predicción (escrita antes de ejecutar)

Mecanismo esperado (docs/phase-1-ray/dependencies.md, 3.2): Ray reinicia el
actor (`max_restarts=-1`) como seguidora; la siguiente llamada del gateway
recibe `RayActorError` o `NotLeader`, avisa al `Controller`, y este pregunta
el estado a las réplicas del ISR y promueve la de mayor LEO. Con
`max_task_retries=0` la llamada interrumpida no se repite.

- **I-03 `verified`.** Una escritura `acks=all` solo se confirma cuando todas
  las réplicas del ISR la tienen (HW = menor LEO del ISR), y cada réplica la
  ha escrito en su archivo con `flush`, que sobrevive a la muerte del proceso.
  La réplica promovida es del ISR, así que tiene todo lo confirmado.
- **I-02 `verified`.** Sin huecos ni duplicados en la lectura final. Si la
  líder caída tenía al final de su archivo un lote sin replicar, hay dos casos
  y ninguno rompe el orden: o la promovida es otra réplica y la antigua, al
  volver como seguidora, descarta ese lote al recibir otro distinto en el
  mismo offset (`replicate`); o la promovida es la antigua, porque vuelve con
  el mayor LEO, y ese lote pasa a ser visible.
- **I-04 `verified`.** Con terminación del proceso no hay líder antigua viva:
  el actor reiniciado vuelve siempre como seguidora. L-01 (sin épocas) no se
  pone a prueba con este fallo; necesitaría una líder pausada o aislada que
  siga aceptando escrituras, que queda fuera de este experimento.
- **I-08 `verified`.** Ni el cliente ni Ray reintentan; una escritura en curso
  aparece a lo sumo una vez.
- **I-01 `verified`.** Lo leído por el `probe` antes de la caída se lee igual
  después.
- **Escrituras afectadas por caída:** entre 1 y 4 `indeterminate` (las que
  estaban en curso o en cola en la líder; hay 4 escritores) y algunas `fail`
  con `NOT_LEADER` mientras se promueve. Algunas indeterminadas aparecerán en
  la lectura final y otras no: las dos cosas son correctas.
- **Indisponibilidad:** entre 1 y 6 s sin escrituras `ok`. El límite superior
  sale de los plazos: el `Controller` espera hasta 2 s el estado de las
  réplicas y el gateway hasta 4 s una llamada a un actor.
- **Con `acks=leader`:** I-03 e I-04 `not_evaluable`. Puede perderse alguna
  escritura `ok` que la líder no hubiera replicado; si ocurre, se registra
  como dato, no como violación, porque el contrato lo admite.

- **Fecha y commit de la predicción:** 2026-11-15, sobre `c08efbf`; se versiona
  en el commit que añade este registro, antes de la primera ejecución.

## Configuración y procedimiento

- **Entorno, versiones y nodos:** como EXP-001 (un nodo Ray, tres nodos
  lógicos, L-04).
- **Configuración y semilla:** `config.yaml` de cada ejecución; plazo de
  replicación 2000 ms; `max_task_retries=0` (valor del prototipo).
- **Workload:** `W5`, 30 s.
- **Disparador y momento del fallo:** a los 10 s de carga, `kill -9` al PID
  del worker de la réplica líder de la partición (`runtime_info` del actor,
  líder según `describe_topic`). Una caída por ejecución.
- **Ejecuciones:** 5 con `acks=all` y 3 con `acks=leader`, cada una con una
  carpeta de datos nueva.
- **Lectura final:** al terminar la carga, cuando el gateway vuelve a
  responder; la espera es por condición (el LoadGen la reintenta hasta 60 s).
- **Comando de reproducción y limpieza:**
  `python scripts/experiments/leader_loss.py --out <carpeta> --acks all|leader --kill-after 10`.
  El script espera por condición a que haya escrituras confirmadas antes de la
  caída y a que el `Controller` registre `leader_change` después.

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

| Ejecución | `acks` | `ok` | `indeterminate` | `fail` | Indeterminadas en el log | Cambio de líder tras la caída (s) | Sin `ok` (s) | Nueva líder (LEO n1/n2/n3) | Propiedades |
|---|---|---|---|---|---|---|---|---|---|
| all-1 | all | 19 293 | 4 | 0 | 1 | 1,90 | 1,949 | n1 (6881/6880/6880) | todas `verified` |
| all-2 | all | 20 157 | 4 | 0 | 1 | 1,89 | 1,935 | n1 (6946/6945/6945) | todas `verified` |
| all-3 | all | 19 908 | 4 | 0 | 1 | 1,89 | 1,933 | n2 (6915/6915/6915) | todas `verified` |
| all-4 | all | 18 319 | 4 | 0 | 1 | 1,89 | 1,935 | n2 (6282/6282/6282) | todas `verified` |
| all-5 | all | 19 757 | 4 | 0 | 1 | 1,90 | 1,941 | n1 (6870/6869/6869) | todas `verified` |
| leader-1 | leader | 34 124 | 4 | 0 | 1 | 1,93 | 1,973 | n1 (12373/12372/12372) | I-03, I-04 `not_evaluable`; resto `verified` |
| leader-2 | leader | 34 066 | 4 | 0 | 1 | 1,91 | 1,958 | n2 (12023/12023/12023) | I-03, I-04 `not_evaluable`; resto `verified` |
| leader-3 | leader | 34 273 | 4 | 0 | 0 | 1,89 | 1,931 | n2 (12164/12164/12164) | I-03, I-04 `not_evaluable`; resto `verified` |

La líder caída era siempre `n1`. «Indeterminadas en el log» es la diferencia
entre los registros de la lectura final y las escrituras `ok`.

- **Resultado observado:** en cada caída, las 4 escrituras en curso (una por
  escritor) esperan ~1,9 s, hasta que Ray da el proceso por muerto, y el
  gateway responde `REQUEST_TIMED_OUT`. El `Controller` promueve a otra réplica
  a los 1,89–1,93 s y las escrituras vuelven enseguida: 1,93–1,97 s sin
  escrituras `ok`, frente a un máximo de 33 ms sin fallos (EXP-001). Las 4
  indeterminadas caen siempre dentro de ese intervalo (`not_ok_inside_gap` en
  `fault.json`). Cuando la réplica caída tenía un lote más que las demás, el
  `Controller` la vuelve a elegir al reiniciarse, y ese lote, cuya escritura
  quedó indeterminada, pasa a ser visible.
- **Propiedades:** todas `verified` en las 8 ejecuciones, salvo I-03 e I-04,
  `not_evaluable` con `acks=leader`. Con `acks=leader` no se perdió ninguna
  escritura confirmada.
- **Diferencias con la predicción:**
  - Predijimos entre 1 y 4 indeterminadas y «algunas `fail` con `NOT_LEADER`
    mientras se promueve». Hubo siempre exactamente 4 indeterminadas y ninguna
    `fail`: los 4 escritores quedan bloqueados en la líder caída hasta la
    promoción, y cuando vuelven a llamar ya hay líder nueva.
  - La indisponibilidad (1,93–1,97 s) está dentro del rango predicho (1–6 s) y
    es muy estable. No la marca el plazo de 2 s del `Controller`, sino lo que
    tarda Ray en dar el proceso por muerto: el `Controller` promueve en cuanto
    recibe el aviso.
  - No predijimos que la réplica caída fuese a menudo la nueva líder (4 de 8):
    el `Controller` prefiere la de mayor LEO y, si empatan, otra distinta.

## Conclusión e incertidumbre

- **Conclusión:** ante la terminación abrupta del proceso de la líder (F1 /
  F15), el prototipo conserva todas las escrituras confirmadas con `acks=all`,
  en su offset y sin contradicciones, y no confirma ni rechaza en falso las
  escrituras en curso: las da como indeterminadas. El coste es ~1,9 s sin
  escrituras, que dependen de la detección de Ray y no de un mecanismo propio
  (L-06).
- **Límites:** un único host (L-04): un fallo de proceso, no de máquina ni de
  red. Una caída por ejecución, siempre de la líder `n1` y siempre a los 10 s.
  Sin `fsync` por lote: la muerte del proceso no pierde lo escrito en el
  archivo, pero un corte de energía sí podría. No pone a prueba L-01 (épocas):
  tras un `kill -9` no queda ninguna líder antigua viva que siga aceptando
  escrituras; haría falta pausarla o aislarla.
- **Incertidumbre restante:** 8 ejecuciones no excluyen interleavings raros
  (por ejemplo, una caída durante un cambio del ISR). El tiempo de detección
  de Ray puede variar con otra carga o con nodos en máquinas distintas.
- **Acciones:** para el diseño nativo, detectar la caída de la líder con
  latidos y sesiones propios en lugar de depender del runtime (L-06), con un
  plazo explícito y configurable.
