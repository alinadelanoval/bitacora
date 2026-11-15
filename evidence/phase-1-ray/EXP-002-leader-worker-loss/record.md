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
- **Comando de reproducción y limpieza:** se añade con el script del
  experimento.

## Evidencia

Pendiente.

## Resultado

Pendiente.

## Conclusión e incertidumbre

Pendiente.
