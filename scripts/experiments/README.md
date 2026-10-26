# Scripts de experimentos

Preparan, ejecutan y limpian un escenario: arrancar el clúster, lanzar la
carga, inyectar el fallo (terminación de proceso, pausa, proxy de red), esperar
por condición y ejecutar el Checker. Registran la semilla y la configuración
para reproducir cada ejecución. La salida se guarda en
`evidence/<fase>/EXP-NNN-…/`.

La limpieza se limita a directorios explícitos de datos locales.
