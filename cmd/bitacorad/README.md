# Servidor nativo

Punto de entrada del proceso Go para las fases nativas. Mantener aquí el
arranque/configuración y delegar la lógica en paquetes internos acordados.

El programa actual solo permite probar el ciclo local de procesos requerido
por BIT-01. Arrancado con `--dev-placeholder`, espera una señal de parada y no
abre puertos ni implementa la API. Sustituir este modo al iniciar la
implementación nativa.
