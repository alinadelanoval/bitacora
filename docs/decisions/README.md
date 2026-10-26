# Decisiones de arquitectura

Este directorio recoge las decisiones que afectan a garantías, interfaces o
límites de componentes. Tiene dos vistas complementarias:

1. **Registro de decisiones** (esta página): una tabla compacta que muestra qué
   proporcionaba el runtime del prototipo, qué alternativas existen en la
   versión nativa, qué se elige y por qué.
2. **ADR** (`ADR-NNN-*.md`): el razonamiento detallado de cada decisión
   importante, con alternativas, consecuencias y verificación.

No se crea un ADR para cada detalle de implementación. Una decisión aceptada
no se edita: se sustituye por un ADR nuevo que la enlace.

## Estados

| Estado | Significado |
|---|---|
| Por redactar | La decisión está identificada pero no se ha estudiado |
| Propuesta | Hay un ADR con alternativas; no está aceptada por las dos integrantes |
| Aceptada | Aceptada con alternativas comparadas |
| Respaldada | Hay un experimento que apoya los supuestos |
| Sustituida | Reemplazada por otro ADR |

## Registro de decisiones Ray → nativo

La columna *Fase 1 (Ray)* se completa con lo observado en
[`phase-1-ray/dependencies.md`](../phase-1-ray/dependencies.md). La columna
*Propiedades* indica qué invariantes sostiene la decisión
([`requirements.md`](../requirements.md#invariantes-del-problema),
[`architecture.md`](../architecture.md#propiedades-de-arquitectura)).

| Decisión | Fase 1 (Ray) | Alternativas nativas | Diseño elegido | Razón y compromiso | Propiedades | ADR | Estado |
|---|---|---|---|---|---|---|---|
| Comunicación entre procesos | Llamadas a actores | gRPC · protocolo binario propio · HTTP/JSON | Pendiente | Pendiente | — | — | Por redactar |
| Descubrimiento y arranque | Actores con nombre | Semillas estáticas · registro · gossip | Pendiente | Pendiente | — | — | Por redactar |
| Membresía y detección de fallos | Runtime de Ray | Latidos y sesiones · lease · gossip | Pendiente | Pendiente | AI-01 | — | Por redactar |
| Propiedad del estado de una partición | Actor por réplica | Líder por partición · primario/copia | Pendiente | Pendiente | I-04 | — | Por redactar |
| Confirmación y durabilidad | Espera a réplicas vivas | Réplicas sincronizadas · quórum · confirmación local | Pendiente | Pendiente | I-03, AI-03 | — | Por redactar |
| Replicación de datos | Envío del líder (*push*) | *Pull* de seguidores · quórum | Pendiente | Pendiente | I-03, AI-03 | — | Por redactar |
| Visibilidad de lecturas | Lectura hasta el límite del líder | Límite de visibilidad · lectura de cualquier réplica | Pendiente | Pendiente | I-03, AI-04 | — | Por redactar |
| Elección de líder y cercado | Promoción por el `Controller` | Elección entre sincronizadas con época · sin cercado | Pendiente | Pendiente | I-04, AI-01, AI-02 | — | Por redactar |
| Reconciliación tras cambio de líder | Ninguna | Truncamiento por posición · por época | Pendiente | Pendiente | I-03, I-04, AI-05 | — | Por redactar |
| Consenso para metadatos | Actor `Controller` único | Biblioteca de Raft · servicio externo · Raft propio | Pendiente | Pendiente | AI-06 | — | Por redactar |
| Reintentos y duplicados | Reintentos del runtime | Idempotencia de productor · identidad de petición · sin reintento | Pendiente | Pendiente | I-08, AI-11 | — | Por redactar |
| Coordinación de grupos y offsets | Actor `GroupCoordinator` | Coordinador en el líder de consenso · por hash · en el cliente | Pendiente | Pendiente | I-06, I-07, AI-07, AI-08 | — | Por redactar |
| Partición por clave | No aplica | Hash de clave · explícita · sin clave | Pendiente | Pendiente | AI-09 | — | Por redactar |
| Formato de almacenamiento y recuperación | Archivo por réplica | Segmentos con integridad e índices · registro por archivo | Pendiente | Pendiente | I-05, AI-10 | — | Por redactar |
| Control de carga | Cola del actor | Colas acotadas y plazos · rechazo · créditos | Pendiente | Pendiente | — | — | Por redactar |
| Observabilidad | Dashboard de Ray | Logs estructurados con identificadores y métricas | Pendiente | Pendiente | — | — | Por redactar |
| Seguridad | Ninguna | Tokens por rol · TLS mutuo · sin autenticación | Pendiente | Pendiente | I-09 | — | Por redactar |

Los mecanismos que no se implementen se registran con una omisión justificada
en la misma tabla.

## Plantilla de ADR

Ver [`ADR-000-template.md`](ADR-000-template.md). Cada ADR cubre una decisión,
enumera alternativas serias con su razón de descarte, indica los cambios de
contexto que obligarían a revisarla y enlaza los experimentos que respaldan sus
supuestos.
