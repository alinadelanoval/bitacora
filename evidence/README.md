# Evidencia de experimentos

Cada experimento ocupa **una carpeta autocontenida** con su registro y sus
datos, de modo que pueda reproducirse y revisarse sin buscar piezas en otros
directorios.

```text
evidence/<fase>/EXP-NNN-<nombre>/
├── record.md          registro del experimento (plantilla en docs/experiments/TEMPLATE.md)
├── config.yaml        configuración y semilla
├── history.ndjson     historial de operaciones (confirmaciones y lecturas)
├── checker.json       informe del Checker por propiedad
├── nodes/             logs JSON de los nodos
└── metrics/           instantáneas de métricas, si aplica
```

Fases: `phase-1-ray/`, `phase-2-native/`, `phase-3-comparison/`.

Reglas:

- La **predicción** se escribe y se versiona **antes** de ejecutar; el resultado
  se añade después. Un registro no se reescribe al repetir el experimento: se
  crea otro `EXP-NNN`.
- Cada registro identifica el commit, la configuración, el workload, el
  disparador del fallo, el resultado y la incertidumbre restante.
- Los scripts reutilizables viven en [`scripts/`](../scripts/README.md); aquí se
  conserva lo que produjo cada ejecución.
- Los logs de evidencia se versionan. Los datos locales y los volcados grandes
  van en `evidence/local/` (ignorado por Git) o fuera del repositorio.
- Las propiedades verificadas se citan por su ID (`I-xx`, `AI-xx`) de
  [`docs/requirements.md`](../docs/requirements.md#invariantes-del-problema) y
  [`docs/architecture.md`](../docs/architecture.md#propiedades-de-arquitectura).
