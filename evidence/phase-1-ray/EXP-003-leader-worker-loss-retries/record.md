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
- **Comando de reproducción y limpieza:** se añade con el script del
  experimento.

## Evidencia

Pendiente.

## Resultado

Pendiente.

## Conclusión e incertidumbre

Pendiente.
