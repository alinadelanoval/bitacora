# Requisitos de Bitácora

## Propósito

Bitácora es un sistema distribuido de registro y entrega de eventos. Los
productores escriben registros en temas particionados y los consumidores los
leen de forma independiente. El sistema persiste y replica los registros,
mantiene el progreso de los grupos de consumidores y permite verificar su
comportamiento ante fallos.

Este documento fija el alcance y los resultados que debe ofrecer el sistema.
El comportamiento visible de las operaciones se concreta en
[`contract.md`](contract.md); la organización de los componentes y sus
mecanismos internos se describe en [`architecture.md`](architecture.md).

## Alcance

### Incluido

- Crear y consultar temas, particiones y sus metadatos.
- Publicar y leer registros con orden definido por partición.
- Persistir los registros y recuperar el log después de reiniciar un nodo.
- Replicar particiones y gestionar la pérdida o recuperación de nodos.
- Permitir que productores y consumidores operen de forma independiente.
- Coordinar grupos de consumidores y asignarles particiones.
- Confirmar el progreso de lectura y reanudar desde una posición.
- Definir el resultado de reintentos de escritura y permitir el reprocesamiento.
- Autenticar clientes mediante tokens asociados a principales y roles.
- Probar el comportamiento funcional, distribuido y de rendimiento, y conservar
  resultados reproducibles.

### Fuera de alcance

- Compatibilidad con el protocolo binario de Apache Kafka.
- Transacciones atómicas entre particiones y garantía exactly-once de extremo
  a extremo.
- Compactación, almacenamiento por niveles, georreplicación, cuotas y
  retención automática.
- Reequilibrio cooperativo, membresía estática y colas de mensajes fallidos.
- TLS mutuo, ACL por tema y gestión avanzada de identidades.

## Requisitos funcionales

Los criterios de aceptación describen resultados observables. Las reglas
relativas a offsets, confirmaciones, errores y límites de replicación se
detallan en [`contract.md`](contract.md).

| ID | Requisito | Criterio de aceptación | Historias relacionadas |
|---|---|---|---|
| RF-01 | Gestionar temas y particiones. | Un cliente autorizado puede crear un tema con el número de particiones solicitado y consultar sus metadatos. Una petición inválida o un nombre ya existente obtiene un error definido. | BIT-10, BIT-41 |
| RF-02 | Publicar registros ordenados por partición. | Cada registro aceptado recibe un offset único, contiguo y creciente en su partición. Las lecturas respetan el orden de esos offsets. | BIT-11, BIT-20–23, BIT-50 |
| RF-03 | Desacoplar productores y consumidores. | Se pueden publicar registros sin consumidores conectados. Un consumidor que se conecte después puede leer los registros retenidos desde la posición solicitada. | BIT-30, BIT-31, BIT-50–51 |
| RF-04 | Persistir y recuperar el log. | Tras reiniciar un nodo, se recuperan los registros completos y las posiciones confirmadas. En 100 pruebas de terminación abrupta durante una escritura, no se pierde ningún registro confirmado ni se entrega un registro parcial o corrupto. | BIT-20–23 |
| RF-05 | Replicar particiones y recuperarse de fallos. | Con factor de replicación 3 y un mínimo de 2 réplicas sincronizadas, no se pierden escrituras confirmadas con `acks=all` al caer un nodo. El failover es inferior a 10 s y un líder obsoleto no confirma operaciones. | BIT-12, BIT-16, BIT-60–64 |
| RF-06 | Mantener el progreso de lectura. | Un consumidor puede confirmar la siguiente posición que desea leer y recuperar ese progreso al reiniciarse. | BIT-72 |
| RF-07 | Coordinar grupos de consumidores. | Cada partición se asigna como máximo a un miembro activo de un grupo y generación. Los cambios de membresía producen una nueva asignación en menos de 15 s; operaciones de generaciones anteriores se rechazan. | BIT-13, BIT-70–71 |
| RF-08 | Reanudar y reprocesar registros. | Un consumidor puede iniciar la lectura desde el principio, desde el final, desde un offset explícito o desde una fecha. La CLI permite solicitar el reprocesamiento desde una posición declarada sin alterar el log. | BIT-73, BIT-81 |
| RF-09 | Hacer seguros los reintentos de escritura. | La repetición de una petición identificada como ya aceptada no añade una segunda copia del registro. Una respuesta perdida no se interpreta por sí sola como rechazo de la escritura. | BIT-16, BIT-30, BIT-80, BIT-91 |
| RF-10 | Autenticar y autorizar clientes. | Las operaciones requieren un token válido asociado a un principal y a un rol (`producer`, `consumer`, `admin` o `node`). Las credenciales inválidas o un rol insuficiente se rechazan sin ejecutar la operación solicitada. | BIT-86 |
| RF-11 | Exponer errores accionables. | Los errores distinguen, como mínimo, entradas inválidas, recursos inexistentes, falta de autorización, conflictos de generación, indisponibilidad y timeouts. | BIT-10, BIT-16, BIT-70 |

## Invariantes

Estas propiedades deben conservarse durante las operaciones normales, la
recuperación y los escenarios de fallo definidos para el proyecto.

| ID | Invariante | Verificación |
|---|---|---|
| I-01 | Los offsets de una partición son únicos, contiguos, crecientes y no se reutilizan para registros aceptados. | Publicación concurrente, reinicio y cambio de líder. |
| I-02 | Los registros aceptados son inmutables y se entregan en el orden de su partición. | Comparación del log antes y después de lecturas y fallos. |
| I-03 | Los consumidores solo leen registros hasta el límite de visibilidad publicado por la réplica líder. | Lecturas con réplicas retrasadas y durante cambios de líder. |
| I-04 | Una partición no se asigna a dos miembros activos del mismo grupo y generación. | Rebalanceo, salida de miembros y operaciones con generaciones antiguas. |
| I-05 | Un nodo que haya perdido el liderazgo no puede confirmar escrituras ni avanzar el límite de visibilidad. | Aislamiento del líder anterior y recuperación del clúster. |
| I-06 | Un registro incompleto o corrupto no se expone como válido. | Interrupción de escritura, truncamiento y recuperación del almacenamiento. |
| I-07 | La deduplicación de un productor se conserva durante el periodo de validez de su identidad y secuencia. | Reintento con respuesta perdida y recuperación del estado del productor. |

## Requisitos de calidad

Las mediciones deben registrar la configuración, el hardware, el número de
nodos, el tamaño de los mensajes y la carga utilizada. Un objetivo no se
considera resultado hasta que se acompaña de evidencia.

| ID | Atributo | Objetivo y criterio de verificación |
|---|---|---|
| RNF-01 | Durabilidad | Una escritura confirmada se conserva ante los fallos tolerados por la política de replicación configurada. Verificar mediante fallos controlados y lectura posterior. |
| RNF-02 | Disponibilidad | Con factor de replicación 3 y un mínimo de 2 réplicas sincronizadas, tolerar la caída de cualquier nodo sin perder escrituras confirmadas con `acks=all`; medir un failover inferior a 10 s en la configuración de referencia. |
| RNF-03 | Rendimiento | Objetivo orientativo: al menos 20 000 mensajes/s de 1 KiB con lotes y `acks=1`; p99 de escritura inferior a 50 ms con `acks=all`. Ejecutar varias repeticiones con carga y configuración documentadas; este objetivo no sustituye los criterios de corrección. |
| RNF-04 | Consistencia | Un registro confirmado y visible conforme a la política de confirmación no desaparece tras el failover. Verificar con historiales de operaciones y lecturas. |
| RNF-05 | Observabilidad | Los logs permiten correlacionar nodo, solicitud, tema, partición y época o generación. Las métricas permiten analizar disponibilidad, replicación, latencia y throughput. |
| RNF-06 | Mantenibilidad | La lógica se organiza en componentes con responsabilidades identificables; las pruebas Go pueden ejecutarse localmente y en CI. |
| RNF-07 | Portabilidad | La implementación nativa se ejecuta sin Ray como dependencia de ejecución. |
| RNF-08 | Seguridad | Los tokens se asocian a principales y roles; la autorización se aplica antes de ejecutar la operación. Los secretos no aparecen en logs y las acciones administrativas quedan auditables. |
| RNF-09 | Reproducibilidad | Cada experimento conserva versión del código, configuración, carga, procedimiento, resultado y límites observados. |

## Verificación y entregas

| Hito | Resultado esperado |
|---|---|
| R0 — Prototipo Ray | Prototipo funcional; contrato y arquitectura iniciales; mapa de capacidades de Ray observado en el código; pruebas iniciales y un experimento reproducible. |
| R1 — Implementación nativa | Servidor Go con almacenamiento, recuperación y replicación implementados conforme al alcance acordado. |
| R2 — Conformidad | Suite común ejecutada contra Ray y Go. Las diferencias de comportamiento quedan corregidas o justificadas explícitamente. |
| R3 — Evaluación y defensa | Campaña reproducible de fallos y carga, comparación Ray/Go, análisis de garantías y límites, informe y evidencia; ambas integrantes pueden defender individualmente las decisiones del sistema. |

## Trazabilidad

Cada requisito debe enlazarse con la regla de contrato o decisión que lo
concreta, la historia `BIT-*`, su prueba y, cuando corresponda, el experimento
que aporta evidencia.

| Requisito | Contrato / decisión | Historia / Issue | Prueba | Experimento / evidencia |
|---|---|---|---|---|
| RF-xx / RNF-xx / I-xx | Sección o ADR | BIT-xx / #N | Caso de prueba | EXP-xx / `evidence/...` |

```text
Requisito → contrato o decisión → Issue → prueba → PR revisado → evidencia
```

Actualizar los requisitos cuando cambie el alcance o una prueba contradiga una
expectativa. Distinguir siempre entre lo especificado, lo implementado y lo
demostrado.
