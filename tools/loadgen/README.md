# LoadGen

Generador de cargas repetibles para la campaña y la comparación de la Fase 3.
Los mismos workloads se ejecutan contra el prototipo Ray y la versión Go.

- **Definiciones de carga** versionadas en `workloads/` (por ejemplo, escritura
  intensiva con una partición y RF=3, lectura intensiva, clave caliente).
- Cada ejecución registra workload, duración, concurrencia, configuración y
  versión.
- Las mediciones de rendimiento se mantienen separadas de las pruebas de
  corrección.

## Versión mínima (BIT-16)

```sh
go run ./tools/loadgen -endpoint 127.0.0.1:50051 -workload tools/loadgen/workloads/w5.json \
    -history history.ndjson -summary summary.json [-acks leader] [-duration 60] [-final-delay 5s]
go run ./tools/checker -history history.ndjson -json checker.json
```

- **`W5`** ([`workloads/w5.json`](workloads/w5.json)): escritura intensiva, un
  tema de 1 partición con RF=3, un registro por lote y `acks` configurable.
- **Escritores sin reintentos.** Cada escritura lleva un `id` nuevo, que es
  también la clave y el valor del registro, y se envía una sola vez. El
  resultado sigue el contrato: `REQUEST_TIMED_OUT` o un fallo del canal son
  `indeterminate`; cualquier otro `ErrorCode` es `fail`. Tras un error, el
  escritor espera 50 ms y sigue con otro `id`. Así una repetición en el log es
  un efecto del sistema, no del cliente (I-08).
- **Lector `probe`.** Cada `probe_interval_ms` lee lo escrito desde su pasada
  anterior y vuelve a leer los `probe_window` offsets anteriores. Los registros
  cercanos a un fallo se leen antes y después de él (I-01).
- **Lectura final.** Al terminar la carga (y tras `-final-delay`), lee cada
  partición desde `EARLIEST` hasta el HW de `LATEST`. Solo escribe sus
  registros y `final_read_done` si la lectura se completa; ante un error la
  repite entera hasta `-final-timeout`.
- **Resumen.** Escrituras por resultado y por error, throughput de escrituras
  `ok` y latencia p50/p95/p99/máx de las `ok` (percentil por rango más cercano),
  con la carga efectiva, el endpoint y el commit.

Sale con `0` si la lectura final se completa, `1` si no (el Checker no podrá
evaluar I-02, I-03 ni I-08) y `2` si la configuración o el arranque fallan.
