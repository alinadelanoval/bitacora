# API v1

`bitacora.proto` será la fuente versionada de la API pública. Acordar sus
servicios (`Log`, `Groups`, `Admin`), operaciones, mensajes y códigos de error
en BIT-10; generar stubs Python/Go desde esta fuente. La semántica observable se
documenta en `docs/contract.md`. El protocolo de replicación interno no forma
parte de esta API.

No cerrar el schema a partir de supuestos: offsets, confirmaciones, reintentos
y resultados inciertos deben acordarse primero con las dos integrantes.
