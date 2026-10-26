# Pruebas del sistema

Las pruebas unitarias de Go viven junto al código (`*_test.go`) y las del
prototipo en `prototype/tests/`. Este directorio contiene solo pruebas que
levantan procesos reales:

- [`integration/`](integration/README.md): interacción de componentes y procesos
  en un despliegue local reproducible, sin inyección de fallos.
- [`faults/`](faults/README.md): escenarios de fallo automatizados con las
  aserciones del Checker.

Fuera de este directorio:

- Las pruebas de conformidad del contrato, comunes a Ray y Go, están en
  [`contracttest/`](../contracttest/README.md).
- Las definiciones de carga están en [`tools/loadgen/`](../tools/loadgen/README.md).
- Los registros y resultados de experimentos están en [`evidence/`](../evidence/README.md).

Se crean subdirectorios nuevos solo cuando aparece la primera prueba que los
necesita.
