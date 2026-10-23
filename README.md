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

## Entorno local

El entorno de referencia es Linux, con Go 1.26.x para las fases nativas y
Python 3.11 o superior para el prototipo Ray. La CI fija Go 1.26.10 y Python
3.11; utilizad esas mismas versiones para reproducir sus resultados. También
se necesitan GNU Make y Buf 1.75.0 para los comandos del proyecto.

Desde la raíz del repositorio, instalad las dependencias Python fijadas:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --require-hashes -r requirements.lock
```

Los comandos comunes están en el `Makefile`:

```sh
make verify      # compilación, vet, pruebas Go con race y pytest
make proto       # validar y generar los stubs desde proto/bitacora/v1
make cluster     # arrancar tres procesos locales de prueba
make stop        # detener solo los procesos iniciados por make cluster
```

Instalad Buf 1.75.0 con `make buf-install` y añadid `$(go env GOPATH)/bin` al
`PATH` antes de ejecutar `make proto`.

El comando de clúster inicia tres procesos locales de prueba. Estos todavía no
exponen la API ni implementan el servicio Bitácora. El arranque de esos procesos
y la generación de código requieren Linux; el código Go también puede
compilarse en Windows y macOS.

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

Los directorios de implementación incluyen README que describen la
responsabilidad de cada componente. El código se añadirá conforme avancen las
historias y queden definidas sus interfaces.

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

Los directorios internos separan las responsabilidades de almacenamiento,
replicación, metadatos, grupos, autenticación y servidor. Sus README describen
la función prevista de cada componente; su implementación avanzará junto con
las historias correspondientes. La API y su comportamiento deben mantenerse
alineados con los requisitos, el contrato y las pruebas.
