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

Para cada comportamiento relevante, registrar:

| Requisito | Componente de Bitácora | Capacidad de Ray observada | Evidencia | Implicación para Go |
|---|---|---|---|---|
| RF-* | Actor, tarea u operación | Capacidad realmente utilizada | Prueba o experimento | Estado y responsabilidad que debe asumir la versión nativa |

La vista debe identificar quién posee el estado, qué operaciones lo modifican,
cómo se comunican los componentes y qué sucede al reiniciar o perder un actor.
Las observaciones del prototipo deben distinguirse de las garantías del
contrato: una limitación de Ray o del prototipo no redefine por sí misma el
comportamiento objetivo.

La fase termina con el prototipo funcional, el contrato inicial, un diagrama,
el mapa de capacidades observado, pruebas iniciales y al menos un experimento
reproducible.

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

## Invariantes internas

| ID | Propiedad | Mecanismo | Verificación |
|---|---|---|---|
| AI-01 | Una época antigua no modifica el estado del líder vigente. | Validación de época en operaciones del plano de datos y de control. | Aislamiento y recuperación del líder anterior. |
| AI-02 | Una réplica solo se considera sincronizada si posee el prefijo requerido del log. | Comparación de posiciones y actualización del conjunto de réplicas sincronizadas. | Réplica retrasada y reingreso al conjunto. |
| AI-03 | El estado confirmado del plano de control no diverge entre nodos participantes. | Replicación mediante Raft. | Caídas y recuperación de miembros del clúster. |
| AI-04 | La recuperación local no expone registros parciales como completos. | Formato verificable de segmentos y recuperación del log. | Interrupciones y corrupción controlada. |

Los límites de fallos, el mecanismo implementado y el resultado de la prueba
deben acompañar cada invariante. Las propiedades observables para el cliente se
definen únicamente en el contrato.

## Fase 3: verificación y evolución

Las pruebas comunes de conformidad se ejecutan contra Ray y Go. Las pruebas de
integración validan interacciones entre componentes; las pruebas distribuidas
verifican coordinación y replicación; los experimentos de fallos y carga
caracterizan los límites del sistema.

Cada experimento conserva la configuración, la versión del código, el
procedimiento, los resultados y las incertidumbres. Si un resultado contradice
una expectativa, se actualizan el contrato, la arquitectura o la decisión
correspondiente antes de presentar la propiedad como garantizada.
