# Arquitectura de Bitácora

## Principios de diseño

Bitácora separa el comportamiento que observa el cliente de los mecanismos
internos que lo proporcionan. El contrato de
[`contract.md`](contract.md) se mantiene entre el prototipo Ray y la versión
nativa Go para las operaciones incluidas en ambas fases.

La arquitectura se desarrolla de forma incremental: la primera fase permite
observar el comportamiento real del middleware y la segunda implementa en Go
los mecanismos que el sistema necesita sin depender de Ray en ejecución. Las
decisiones que cambien garantías, interfaces o límites de componentes se
registran en ADR y se verifican mediante pruebas.

## Fase 1: prototipo con Ray

El prototipo valida el flujo funcional y permite observar qué capacidades de
Ray intervienen en el sistema. Su arquitectura documentada debe corresponder
al código que se ejecute, no a un diseño supuesto.

Los artefactos de esta fase viven en [`phase-1-ray/`](phase-1-ray/):

- [`dependencies.md`](phase-1-ray/dependencies.md): diagrama de arquitectura
  (componentes de Bitácora y de Ray) y mapa de dependencias del middleware.
- [`exit-gate.md`](phase-1-ray/exit-gate.md): puerta de salida y congelación
  del contrato.

Las observaciones del prototipo deben distinguirse de las garantías del
contrato: una limitación de Ray o del prototipo no redefine por sí misma el
comportamiento objetivo. Este documento describe a partir de aquí la
arquitectura nativa; el material de la Fase 1 queda congelado al cerrar la
fase.

La fase termina con el prototipo funcional, el contrato inicial, un diagrama,
el mapa de dependencias observado, pruebas iniciales, al menos un experimento
reproducible y la puerta de salida respondida.

## Fase 2: implementación nativa en Go

La versión nativa organiza el sistema en dos planos coordinados:

- **Plano de control:** administra la membresía del clúster, el estado de los
  temas y particiones, el liderazgo y la coordinación de grupos.
- **Plano de datos:** recibe, almacena, replica y entrega registros de cada
  partición.

El plano de control utiliza la biblioteca `hashicorp/raft` para mantener el
estado de coordinación. La replicación de los datos de las particiones es
independiente de ese consenso: cada partición tiene un líder y réplicas
seguidoras que solicitan los registros pendientes. El mecanismo de consenso no
sustituye la replicación del log de datos.

### Componentes

| Componente | Responsabilidad |
|---|---|
| API y servidor | Validar solicitudes, autenticar clientes mediante interceptores y exponer las operaciones del contrato. |
| Metadatos y clúster | Mantener la configuración de temas, particiones, nodos y liderazgo mediante el plano de control. |
| Log y almacenamiento | Añadir registros, asignar offsets, persistir segmentos y recuperar el estado después de un reinicio. |
| Replicación | Transferir registros del líder a las réplicas, mantener el conjunto de réplicas sincronizadas y gestionar épocas de liderazgo. |
| Productor | Asignar identidad y secuencias a las escrituras, aplicar la política de confirmación y permitir reintentos sin duplicar registros aceptados. |
| Coordinador de grupos | Gestionar membresía, generaciones, asignaciones y offsets confirmados. |
| Cliente | Ofrecer una interfaz para publicar, leer y operar con grupos conforme al contrato. |

La estructura de paquetes mantiene estas responsabilidades y evita interfaces
innecesarias. No se crea un paquete separado por cada concepto si no existe
una frontera de estado o una historia que lo justifique. La organización actual
es:

```text
cmd/bitacorad/         Ejecutable del servidor
cmd/bitacora/          Interfaz de línea de comandos
proto/bitacora/v1/     Definiciones versionadas de la API
pkg/client/            Cliente público
internal/record/       Modelo de registro
internal/storage/      Log de partición y almacenamiento
internal/replica/      Replicación y liderazgo de particiones
internal/metadata/     Metadatos y coordinación del clúster
internal/producer/      Identidad, secuencias y confirmación de escrituras
internal/group/        Membresía, asignación y offsets
internal/auth/         Autenticación de clientes
internal/server/       Servicios y transporte
tests/                 Pruebas funcionales, distribuidas, de fallos y carga
tools/                 Verificación de invariantes y generación de carga
```

Las carpetas representan las fronteras funcionales del proyecto; los README
de cada componente delimitan su responsabilidad hasta que comience su
implementación.

### Despliegue de referencia

La configuración distribuida de referencia utiliza tres procesos
`bitacorad`. Cada proceso participa en el grupo Raft de metadatos y puede
alojar líderes o réplicas de particiones. El factor de replicación 3 y el
mínimo de 2 réplicas sincronizadas permiten evaluar la pérdida de un nodo sin
perder escrituras confirmadas con `acks=all`. El clúster se ejecuta localmente
de forma reproducible; la CLI opera como proceso independiente.

### Flujo de escritura

1. El servidor valida la solicitud, las credenciales y la identidad/secuencia
   del productor.
2. El líder vigente asigna el siguiente offset de la partición y añade el
   registro al log local.
3. Las réplicas seguidoras solicitan y almacenan los registros pendientes.
4. El líder determina qué registros están replicados y actualiza el límite de
   visibilidad de acuerdo con la configuración del clúster.
5. La respuesta se envía según el nivel de confirmación solicitado.

Las solicitudes con la misma identidad y secuencia aceptada se reconocen como
reintentos y no añaden otro registro. La información necesaria para conservar
esta propiedad debe recuperarse tras reiniciar el proceso.

### Flujo de lectura

1. El consumidor solicita una partición desde un offset y especifica los
   límites de lectura.
2. El servidor consulta el líder vigente y obtiene registros disponibles hasta
   el límite de visibilidad.
3. El cliente recibe los registros en orden de offset.
4. Si pertenece a un grupo, el consumidor confirma la siguiente posición que
   leerá. El coordinador valida que la generación siga vigente.

### Replicación, liderazgo y recuperación

Cada partición tiene un líder y un conjunto de réplicas seguidoras. El líder
coordina las escrituras y mantiene el límite hasta el que los registros pueden
ser visibles. Los productores y consumidores no deben aceptar operaciones de
un líder que haya perdido su autoridad; las épocas permiten rechazar mensajes
obsoletos.

La configuración fija el factor de replicación y el número mínimo de réplicas
sincronizadas necesario para aceptar escrituras con `acks=all`. Si no se alcanza
ese mínimo, el servidor rechaza la escritura. La elección de líder respeta el
conjunto de réplicas sincronizadas de modo que no se elija una réplica que
carezca de registros confirmados conforme a esa política.

Al iniciar, cada nodo recupera sus segmentos y reconstruye los offsets y el
estado necesario del log. Los registros parciales o inválidos se detectan y
no se publican como registros completos. Una réplica atrasada solicita los
datos que le faltan antes de volver a considerarse sincronizada.

## Protocolo interno

El protocolo entre nodos es independiente de la API de clientes. Para cada
mensaje interno, documentar su emisor y receptor, el cambio de estado que
produce, el orden requerido, el timeout, la política de reintento y el
comportamiento ante una respuesta perdida.

| Flujo | Emisor → receptor | Efecto | Fallo y recuperación |
|---|---|---|---|
| Replicación de datos | Seguidor → líder | Solicita registros posteriores a la última posición local | Reintentar desde la siguiente posición faltante; el líder valida época y posición. |
| Estado de réplica | Líder → plano de control | Informa progreso y elegibilidad de la réplica | Una réplica que deja de cumplir los límites de sincronización sale del conjunto activo. |
| Coordinación | Nodo → grupo Raft | Propone un cambio de metadatos o estado de grupo | El cambio se considera efectivo al confirmarse por consenso. |

Completar y ajustar esta tabla a los mensajes realmente implementados. Las
decisiones que afecten al comportamiento externo deben mantenerse también en
[`contract.md`](contract.md), sin duplicar allí los detalles internos.

## Propiedades de arquitectura

Propiedades que dependen de un mecanismo concreto. Cada una **sostiene** uno o
más invariantes del problema ([`requirements.md`](requirements.md#invariantes-del-problema))
y se vuelve exigible cuando se acepta el ADR que la activa; hasta entonces es
una propuesta de diseño, no una garantía.

| ID | Propiedad | Mecanismo previsto | Sostiene | Decisión que la activa | Estado | Verificación |
|---|---|---|---|---|---|---|
| AI-01 | Una época antigua no modifica el estado del líder vigente. | Validación de época en operaciones de datos y de control | I-04 | ADR: elección de líder y cercado | Propuesta | Aislar y reincorporar al líder anterior |
| AI-02 | Como máximo un nodo acepta escrituras por (partición, época). | Época replicada por consenso | I-04 | ADR: elección de líder y cercado | Propuesta | Logs de cambios de rol: nunca dos líderes con la misma época |
| AI-03 | Una réplica solo cuenta para la confirmación si posee el prefijo requerido del log. | Conjunto de réplicas sincronizadas | I-03, I-04 | ADR: confirmación y réplicas sincronizadas | Propuesta | Réplica retrasada y reingreso al conjunto |
| AI-04 | Un consumidor solo lee hasta el límite de visibilidad: nunca lee un registro que luego desaparece. | Límite de visibilidad (high watermark) | I-03, I-02 | ADR: visibilidad de lecturas | Propuesta | Lecturas durante cambios de líder frente a la lectura final |
| AI-05 | Un seguidor elimina los registros no confirmados del líder anterior antes de replicar. | Truncamiento por época | I-03, I-04 | ADR: reconciliación tras cambio de líder | Propuesta | Cadena de dos caídas seguidas |
| AI-06 | El estado de metadatos confirmado no diverge entre nodos. Se heredan *Election Safety* y *State-Machine Safety* de la librería de consenso; no se reverifican. | Replicación mediante Raft | I-07 | ADR: consenso para metadatos | Propuesta | Caídas y recuperación de miembros del clúster |
| AI-07 | Un miembro con generación obsoleta no puede confirmar offsets. | Generación de grupo | I-07 | ADR: coordinación de grupos | Propuesta | Consumidor pausado más que la sesión |
| AI-08 | Los offsets confirmados de un grupo no retroceden, salvo reinicio explícito con el grupo vacío. | Validación en el coordinador | I-06 | ADR: offsets de grupo | Propuesta | Secuencia de commits por grupo y partición |
| AI-09 | Para cada clave, el orden de lectura coincide con el orden de confirmación. | Partición por clave | I-02 | ADR: partición por clave | Propuesta | Comparación por clave, un productor por clave |
| AI-10 | La recuperación local no expone registros parciales como completos. | Formato verificable de segmentos | I-05 | ADR: formato de segmentos | Propuesta | Interrupciones y corrupción controlada |
| AI-11 | El estado de deduplicación se reconstruye tras una recuperación o un cambio de líder. | Estado por identidad de productor, replicado con los lotes | I-08 | ADR: idempotencia de productor | Propuesta | Reintento tras `kill -9` y tras conmutación |

Los límites de fallo, el mecanismo implementado y el resultado de la prueba
deben acompañar cada propiedad cuando su ADR se acepte. Las propiedades
observables por el cliente se definen en el contrato y en los invariantes del
problema.

## Fase 3: verificación y evolución

Las pruebas comunes de conformidad se ejecutan contra Ray y Go. Las pruebas de
integración validan interacciones entre componentes; las pruebas distribuidas
verifican coordinación y replicación; los experimentos de fallos y carga
caracterizan los límites del sistema.

Cada experimento conserva la configuración, la versión del código, el
procedimiento, los resultados y las incertidumbres. Si un resultado contradice
una expectativa, se actualizan el contrato, la arquitectura o la decisión
correspondiente antes de presentar la propiedad como garantizada.
