# Pruebas de conformidad del contrato

Suite única que comprueba el contrato observable
([`docs/contract.md`](../docs/contract.md)) contra un endpoint gRPC. Se ejecuta
sin cambiar sus expectativas contra el gateway Ray y contra el servidor Go.

- El endpoint se configura con `BITACORA_ENDPOINTS` o el mecanismo acordado.
- Cada caso corresponde a una regla aceptada del contrato; no se convierten
  preguntas abiertas en aserciones hasta decidir su semántica.
- Una diferencia entre implementaciones se corrige o se registra con su alcance
  y justificación. No se relaja una prueba para ocultarla.
- Los casos de operaciones de usuario sobre un endpoint viven aquí, no en
  `tests/`.
