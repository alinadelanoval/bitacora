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

## Invariantes del problema

Propiedades de *safety* que el sistema debe conservar durante la operación
normal, la recuperación y los escenarios de fallo. Se formulan sin referirse a
un mecanismo concreto: valen para el prototipo Ray y para la versión Go, y se
comprueban con las mismas herramientas (`tools/checker/`). Las propiedades que
dependen de una decisión de diseño se recogen en
[`architecture.md`](architecture.md#propiedades-de-arquitectura) y se exigen
cuando el ADR correspondiente se acepta.

Los tiempos de conmutación, reequilibrio y los objetivos de rendimiento no son
invariantes: se tratan como objetivos de calidad (RNF) y se miden.

| ID | Nombre | Propiedad | Condiciones y fuera de alcance | Requisitos | Escenarios |
|---|---|---|---|---|---|
| I-01 | Append-only | Un registro aceptado no se modifica ni se elimina. Confirmar o mover una posición de lectura no altera el log. | La retención automática está fuera de alcance. | RF-02, RF-08 | F0, F2, F15 |
| I-02 | Orden por partición | Cada partición tiene un único orden total: offsets únicos y crecientes; todos los consumidores observan la misma secuencia; el offset devuelto por `Produce` es aquel en el que se lee ese registro. No se promete orden entre particiones. | La contigüidad de offsets es regla del [contrato](contract.md). | RF-02, RF-03 | F0, F2, F15 |
| I-03 | Durabilidad declarada | Toda operación que el sistema reconoce como durable sobrevive a los fallos del modelo declarado: aparece en la lectura final con su offset y valor. | Se declara por nivel de confirmación (`acks`). Modelo: caída de proceso y partición de red. Fuera de alcance: pérdida simultánea de la caché de varios nodos y fallos bizantinos. | RF-04, RF-05, RNF-01 | F1, F2, F5, F7, F15 |
| I-04 | Historia única bajo fallo y partición | No existen dos historias reconocidas distintas para una partición: ninguna escritura reconocida es contradicha por otra en el mismo offset, ni durante un cambio de líder ni durante una partición. El lado que no puede garantizarlo deja de confirmar. | Compromiso declarado: ante una partición se rechaza antes que confirmar. | RF-05, RNF-04 | F5, F7, F15 |
| I-05 | Integridad | Un registro incompleto o corrupto no se entrega como válido; la recuperación no pierde registros reconocidos ni inventa registros. | Fuera de alcance: corrupción del medio no detectable. | RF-04 | F2 |
| I-06 | Posición de consumo declarada y observable | La posición confirmada de un grupo es consultable y, tras reiniciar un consumidor, la lectura continúa desde ella. Una posición inválida se comunica; no se sustituye en silencio por otra. | Reiniciar posiciones es una operación explícita (`ResetGroupOffsets`). | RF-06, RF-08 | P1, P3, P4 |
| I-07 | Propiedad de consumo | En cada instante, cada partición de un grupo tiene como máximo un propietario de consumo; un miembro que ya no lo es no puede avanzar el progreso del grupo. | Siempre. | RF-07 | P2, F18 |
| I-08 | Reintento seguro | Reintentar con la misma identidad una operación declarada reintentable no produce un segundo efecto. Un timeout no se interpreta como rechazo: el resultado es indeterminado. | Operaciones reintentables y ventana de validez de la identidad, según el contrato. | RF-09, RF-01 | F8, F9, F21 |
| I-09 | Autorización sin efecto | Una operación sin permiso se rechaza sin modificar el estado protegido y deja un evento de auditoría sin secretos; repetir una mutación autenticada no duplica su efecto. | Alcance según el modelo de confianza. | RF-10, RNF-08 | F20, F21 |

Los escenarios `F*` son los del catálogo común de fallos y cargas. Los `P*` son
escenarios específicos del tema: `P1` reinicio de un consumidor, `P2` consumidor
pausado, `P3` reinicio completo del clúster, `P4` reprocesamiento explícito.

Estado de cada invariante respecto a su verificación:

| Propiedad | Evaluable con el historial del cliente | Fase 1 (Ray) | Fase 2 (Go) |
|---|---|---|---|
| I-01, I-02, I-03, I-08 | Sí | Se mide en `EXP-001…003` | Pendiente |
| I-04, I-07, I-09 | Parcial (requiere logs de nodos) | Se mide en `EXP-002` | Pendiente |
| I-06 | Parcial (consulta de posiciones tras reiniciar) | Pendiente | Pendiente |
| I-05 | No (prueba del almacenamiento) | No aplica a la Fase 1 | Pendiente |

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
