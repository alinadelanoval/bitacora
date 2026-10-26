# LoadGen

Generador de cargas repetibles para la campaña y la comparación de la Fase 3.
Los mismos workloads se ejecutan contra el prototipo Ray y la versión Go.

- **Definiciones de carga** versionadas en `workloads/` (por ejemplo, escritura
  intensiva con una partición y RF=3, lectura intensiva, clave caliente).
- Cada ejecución registra workload, duración, concurrencia, configuración y
  versión.
- Las mediciones de rendimiento se mantienen separadas de las pruebas de
  corrección.
