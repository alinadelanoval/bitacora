# Scripts locales

Automatización pequeña y documentada: arrancar el prototipo o el clúster,
ejecutar pruebas y reproducir experimentos.

- [`dev/`](dev/README.md): arranque y parada del clúster local.
- [`test/`](test/README.md): atajos para ejecutar pruebas.
- [`experiments/`](experiments/README.md): preparación, ejecución y limpieza de escenarios.

Los scripts limitan cualquier limpieza a rutas explícitas bajo `data/` o
`evidence/local/`. Los resultados de cada ejecución se guardan en
`evidence/<fase>/EXP-NNN-…/`.
