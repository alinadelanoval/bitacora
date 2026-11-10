# Prototipo Ray — Fase 1

Implementación temporal Python/Ray para explorar el problema y fijar el
comportamiento observable. Exponer el mismo contrato público que se usará para
la versión Go; el gateway adapta la API pública a los actores del prototipo.

## Estructura

```text
prototype/
├── errors.py                  Errores del dominio con su ErrorCode del contrato
├── batch.py                   Cabecera del lote: lectura, validación, baseOffset y CRC-32C
├── config.py                  Carpeta de datos y política de fsync
├── actors/
│   ├── partition_replica.py   Réplica de una partición: log, archivo propio y replicación
│   └── controller.py          Réplicas en anillo, papeles, ISR y promoción del líder
├── gateway/                   Servicio gRPC Python
└── tests/                     Pruebas locales del prototipo
```

`errors.py` y `batch.py` no dependen de Ray. Las réplicas y el Controller son
actores `detached` con nombre, en el namespace `bitacora`; Ray los reinicia si
su proceso muere, y cada uno reconstruye su estado desde sus archivos.

## Replicación

Cada partición tiene una réplica líder y las demás son seguidoras. Toda réplica
arranca como seguidora; el `Controller` asigna los papeles al crear el tema y
al promover. Solo la líder atiende `append` y `read`; las seguidoras responden
`NotLeader`.

- `append(lote, acks)`: la líder escribe el lote y lo envía a sus seguidoras.
  Con `acks="all"` responde cuando todas las réplicas del ISR lo tienen; la que
  no responde dentro del plazo sale del ISR, con permiso del `Controller`. Con
  `acks="1"` responde tras escribirlo ella.
- El HW es el menor LEO del ISR; `read` solo devuelve lotes por debajo de él.
- Una seguidora fuera del ISR recibe lo que le falta y vuelve al ISR cuando
  alcanza a la líder. Si tiene lotes que la líder no tiene, los descarta.
- Si la líder no responde, quien la llama avisa al `Controller`
  (`report_leader_unavailable`), que la comprueba y promueve a la réplica viva
  del ISR con mayor LEO.

## Datos

Cada réplica escribe `{tema}-{partición}-{nodo}.log`: los lotes del contrato
uno tras otro, con su `baseOffset` asignado. El Controller escribe
`controller.json` con los temas, el nodo de cada réplica, el líder y el ISR de
cada partición, y añade a `events.ndjson` una línea JSON por cada cambio de ISR
(`isr_change`) y cada promoción (`leader_change`).

| Variable | Efecto | Valor por defecto |
|---|---|---|
| `BITACORA_DATA_DIR` | Carpeta de los archivos | `data/ray/` en la raíz del repositorio |
| `BITACORA_FSYNC` | `1`: `fsync` tras cada lote | Solo `flush`: sobrevive a la caída del proceso, no a la del sistema |
| `BITACORA_REPLICATION_TIMEOUT_MS` | Plazo del líder para cada seguidora y del `Controller` para cada réplica | `2000` |

## Pruebas

`make python-test`. Las pruebas usan un nodo Ray con los recursos
`node:n1..n3`, salvo `test_multinode.py`, que arranca en un proceso aparte un
clúster de tres nodos Ray en la misma máquina (requiere `RAY_AUTH_MODE=token`,
que la prueba fija).

Registrar en la sección «Fase 1: prototipo con Ray» de `docs/architecture.md`
únicamente capacidades de Ray observadas. No usar Ray como dependencia runtime
de la versión nativa.
