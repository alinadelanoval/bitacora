# Bitácora

Bitácora es el proyecto P13 de Sistemas Distribuidos: un log de eventos
particionado y replicado con grupos de consumidores. El comportamiento externo
se prototipa primero con Ray y después se implementa de forma nativa en Go.
La fase final verifica las garantías mediante fallos, cargas y evidencia
reproducible.

Este repositorio organiza el trabajo de las tres fases académicas. No implica
compatibilidad con el protocolo de Apache Kafka. Las garantías concretas se
consideran acordadas solo cuando están documentadas en el contrato y cubiertas
por pruebas.

## Fases y entregas

| Fase | Peso | Resultado esperado | Hito del plan |
|---|---:|---|---|
| 1. Prototipo con Ray | 15 % | Prototipo funcional, contrato, arquitectura, mapa real de dependencias, pruebas de conformidad y experimento inicial | R0 |
| 2. Rediseño nativo en Go | 50 % | Mismo contrato observable sin Ray como dependencia de ejecución; decisiones justificadas y pruebas | R1 / R2 |
| 3. Verificación y defensa | 35 % | Campaña reproducible, comparación Ray/Go, análisis de garantías e informe/defensa | R3 |

Las fechas y los criterios oficiales se mantienen en la planificación del
equipo y deben contrastarse con el calendario oficial del curso.

## Estructura

```text
proto/bitacora/v1/         Contrato de red versionado
prototype/                 Prototipo Python/Ray de Fase 1
cmd/bitacorad/              Servidor nativo Go
cmd/bitacora/               CLI del proyecto
internal/{record,storage,
  replica,producer,metadata,
  group,auth,server}/        Componentes internos Go
pkg/client/                 Cliente Go
contracttest/               Contrato común contra Ray y Go
tests/{functional,
  integration,distributed,
  faults,workload}/          Pruebas del sistema
tools/{checker,loadgen}/      Herramientas de verificación/carga
scripts/                    Automatización local
docs/                       Requisitos, contrato, arquitectura y proceso
experiments/                Fichas y ejecuciones por fase
evidence/                   Resultados seleccionados y reproducibles
```

Los README dentro de directorios de implementación son marcadores de alcance,
no código funcional. Crear módulos y paquetes de Go/Python cuando comiencen las
historias correspondientes y sus interfaces estén acordadas.

## Documentos clave

- [Requisitos y trazabilidad](docs/requirements.md)
- [Contrato observable](docs/contract.md)
- [Arquitectura, middleware y protocolo interno](docs/architecture.md)
- [Plantilla de experimento](docs/experiments/TEMPLATE.md)
- [Decisiones de arquitectura (ADR)](docs/decisions/README.md)
- [Experimentos](experiments/README.md)
- [Evidencia](evidence/README.md)
- [Uso de IA](AI_USE.yaml)

## Ciclo de trabajo

```text
Requisito → regla o decisión → Issue → prueba → PR revisado → evidencia
```

No es necesario cerrar todo el diseño ni crear el backlog completo antes de
programar. Antes de empezar cada Issue, identificad sus requisitos, criterios
de aceptación, prueba prevista y decisiones o dependencias que la bloquean.
Refinad el trabajo con lo aprendido en cada fase; mantened los IDs `BIT-*`
existentes en lugar de crear un segundo backlog.

Cada PR debe enlazar el Issue y los requisitos relacionados, mostrar las
pruebas ejecutadas y recibir revisión cruzada. Actualizad contrato o ADR si
cambia una regla acordada. Si el cambio investiga un fallo o una carga, guardad
una ficha en `experiments/` y evidencia reproducible en `evidence/`.

## Scaffold del repositorio

Las carpetas de implementación delimitan las responsabilidades previstas; sus
README describen el alcance de cada componente hasta que se implemente. Los
detalles de API y comportamiento deben mantenerse alineados entre requisitos,
contrato, pruebas y código.

El módulo Go utiliza actualmente `example.com/bitacora`; debe reemplazarse por
la ruta del repositorio antes de publicar o importar el módulo desde otros
proyectos.
