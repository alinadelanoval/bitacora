# Pruebas de fallos

Escenarios de fallo controlado automatizados (`F1`, `F2`, `F5`, `F7`, `F8`,
`F15`…). Cada caso:

- identifica el escenario, la propiedad que pone a prueba (`I-xx`, `AI-xx`) y
  el experimento que lo registra en `evidence/`;
- usa el Checker para decidir el resultado, no solo la ausencia de error;
- espera por condición, nunca con pausas fijas.
