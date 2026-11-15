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
- **Comando de reproducción y limpieza:** se añade con el script del
  experimento.

## Evidencia

Pendiente.

## Resultado

Pendiente.

## Conclusión e incertidumbre

Pendiente.
