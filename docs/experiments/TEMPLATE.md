# EXP-NNN: título del experimento

Ubicación: `evidence/<fase>/EXP-NNN-<nombre>/record.md`.

- **Fase / implementación:** Ray | Go
- **Issue y requisitos:**
- **Propiedades que pone a prueba:** `I-xx`, `AI-xx`
- **Commit / versión:**
- **Escenario (ID) y workload:** por ejemplo `F15`, `W5`
- **Afirmación bajo prueba:**

## Predicción (escrita antes de ejecutar)

Resultado esperado, con su justificación. Se versiona antes de la ejecución.

- **Fecha y commit de la predicción:**

## Configuración y procedimiento

- **Entorno, versiones y nodos:**
- **Configuración y semilla:** `config.yaml`
- **Workload (operaciones, volumen, concurrencia, duración):**
- **Disparador y momento del fallo:**
- **Comando de reproducción y limpieza:**

## Evidencia

Historial, informe del Checker, logs de nodos y métricas, con su ruta dentro de
la carpeta del experimento.

## Resultado

- **Resultado observado:**
- **Propiedades:** `verified` | `violated` | `not_evaluable`, con la anomalía si aplica
- **Diferencias con la predicción:**

## Conclusión e incertidumbre

- **Conclusión:** qué se demuestra y qué no.
- **Límites** (por ejemplo, un único host, un solo tipo de fallo):
- **Incertidumbre restante:**
- **Acciones:** Issues, ADR o cambios del contrato derivados.
