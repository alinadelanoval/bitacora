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
│   ├── partition_replica.py   Réplica de una partición: log en memoria y archivo propio
│   └── controller.py          Crea las réplicas en anillo y guarda los metadatos
├── gateway/                   Servicio gRPC Python
└── tests/                     Pruebas locales del prototipo
```

`errors.py` y `batch.py` no dependen de Ray. Las réplicas y el Controller son
actores `detached` con nombre, en el namespace `bitacora`; Ray los reinicia si
su proceso muere, y cada uno reconstruye su estado desde sus archivos.

## Datos

Cada réplica escribe `{tema}-{partición}-{nodo}.log`: los lotes del contrato
uno tras otro, con su `baseOffset` asignado. El Controller escribe
`controller.json` con los temas y el nodo de cada réplica (el líder, primero).

| Variable | Efecto | Valor por defecto |
|---|---|---|
| `BITACORA_DATA_DIR` | Carpeta de los archivos | `data/ray/` en la raíz del repositorio |
| `BITACORA_FSYNC` | `1`: `fsync` tras cada lote | Solo `flush`: sobrevive a la caída del proceso, no a la del sistema |

## Pruebas

`make python-test`. Las pruebas usan un nodo Ray con los recursos
`node:n1..n3`, salvo `test_multinode.py`, que arranca en un proceso aparte un
clúster de tres nodos Ray en la misma máquina (requiere `RAY_AUTH_MODE=token`,
que la prueba fija).

Registrar en la sección «Fase 1: prototipo con Ray» de `docs/architecture.md`
únicamente capacidades de Ray observadas. No usar Ray como dependencia runtime
de la versión nativa.
