# ADR-003: reglas para los nombres de tema y los identificadores de grupo

- **Estado:** propuesta
- **Fecha:** 2026-11-11
- **Responsables:** equipo de Bitácora
- **Requisitos / historias relacionados:** RF-01, RF-07, RNF-08

## Contexto

Los nombres de tema y los identificadores de grupo los elige el cliente. Las
implementaciones pueden usarlos para nombrar archivos o directorios: una réplica
guarda su log en un archivo cuyo nombre incluye el del tema, y el coordinador de
grupos guarda cada grupo en un archivo cuyo nombre incluye su identificador. Si
un nombre puede contener `/` y `..`, el cliente puede hacer que el servidor
escriba fuera de su directorio de datos.

## Decisión

Los nombres de tema y los identificadores de grupo solo pueden contener letras
ASCII, dígitos, `.`, `_` y `-`, y no pueden ser `.` ni `..`. Un nombre de tema
tiene como máximo 249 caracteres; un identificador de grupo, como máximo 64. Un
nombre que no cumpla la regla se rechaza con `ERROR_CODE_INVALID_REQUEST`.

La regla se expresa como una lista de caracteres permitidos, no como una lista
de secuencias prohibidas: sin separador de rutas no hay forma de cambiar de
directorio.

## Alternativas consideradas

- **No restringir los nombres y guardarlos como datos.** Los archivos se
  nombran con un número derivado del nombre y el nombre se guarda dentro. Admite
  cualquier nombre, pero los archivos dejan de ser identificables a simple
  vista.
- **Registrar los grupos en un log común.** El identificador es un campo de
  cada registro y nunca forma parte de una ruta. Exige construir y compactar un
  log adicional.

## Consecuencias y límites

- Los nombres de archivo y directorio son legibles y seguros.
- Algunos nombres que antes eran válidos (con espacios, barras o caracteres no
  ASCII) dejan de serlo.
- El límite de 249 caracteres deja sitio para el sufijo de la partición dentro
  del límite habitual de 255 bytes por nombre de archivo.

## Verificación

Pruebas de `CreateTopic` y de `Heartbeat` con nombres vacíos, `.`, `..`,
nombres con `/` y nombres que superan la longitud máxima.

## Trazabilidad

[`contract.md`](../contract.md#nombres); Issue #5.
