# Scripts de experimentos

Preparan, ejecutan y limpian un escenario: arrancar el clúster, lanzar la
carga, inyectar el fallo (terminación de proceso, pausa, proxy de red), esperar
por condición y ejecutar el Checker. Registran la semilla y la configuración
para reproducir cada ejecución. La salida se guarda en
`evidence/<fase>/EXP-NNN-…/`.

La limpieza se limita a directorios explícitos de datos locales.

## `leader_loss.py` (EXP-001 a EXP-003)

Una ejecución por llamada, con una carpeta de datos nueva y un nodo Ray propio:

```sh
# EXP-001: sin fallo
python scripts/experiments/leader_loss.py --out evidence/local/EXP-001/all-1 --acks all
# EXP-002: SIGKILL al worker de la líder a los 10 s de carga (F1 / F15)
python scripts/experiments/leader_loss.py --out evidence/local/EXP-002/all-1 --acks all --kill-after 10
# EXP-003: igual, con max_task_retries=3 en las réplicas
python scripts/experiments/leader_loss.py --out evidence/local/EXP-003/run-1 --kill-after 10 --task-retries 3
```

La caída se dispara cuando la carga lleva `--kill-after` segundos **y** ya hay
escrituras confirmadas; después se espera a que el `Controller` registre
`leader_change`. Toda espera es por condición, con un plazo de 60 s. La
carpeta de salida recoge `config.yaml`, el historial, el resumen del LoadGen,
el informe del Checker, `fault.json` (PID, líderes, LEO de cada réplica y
tiempo sin escrituras `ok`) y `nodes/` (eventos y metadatos del `Controller`,
log del gateway). El código de salida es el del Checker, o 2 si la ejecución
no se completó.
