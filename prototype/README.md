# Prototipo Ray — Fase 1

Implementación temporal Python/Ray para explorar el problema y fijar el
comportamiento observable. Exponer el mismo contrato público que se usará para
la versión Go; el gateway adapta la API pública a los actores del prototipo.

## Estructura prevista

```text
prototype/
├── actors/       Controller, PartitionReplica y GroupCoordinator
├── gateway/      Servicio gRPC Python
└── tests/        Pruebas locales del prototipo
```

Completar la estructura y el entorno Python en BIT-02/BIT-11–14. Registrar en
la sección «Fase 1: prototipo con Ray» de `docs/architecture.md` únicamente
capacidades de Ray observadas. No usar Ray como dependencia runtime de la
versión nativa.
