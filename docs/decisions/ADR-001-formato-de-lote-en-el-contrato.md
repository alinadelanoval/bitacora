# ADR-001: formato de lote binario para los registros

- **Estado:** propuesta
- **Fecha:** 2026-11-07
- **Responsables:** equipo de Bitácora
- **Requisitos / historias relacionados:** RF-02, RF-04, RF-11; I-05

## Contexto

El contrato debe fijar cómo se representan los registros en `Produce` y `Fetch`
antes de congelarse, porque cambiar esa representación después obliga a
modificar todos los clientes.

Intervienen cuatro fuerzas que tiran en direcciones distintas. La legibilidad
del contrato favorece representar cada registro como un mensaje Protobuf. La
integridad exige detectar datos dañados entre el productor y el almacenamiento.
El servidor tiene que validar y almacenar cada escritura con un coste
predecible. Y el formato tiene que admitir más adelante identidad de productor
y época de líder sin romper a los clientes existentes.

## Decisión

Los registros se transportan y se almacenan como lotes binarios: una cabecera
fija con un CRC-32C, seguida de los registros. La especificación del formato
está en [`contract.md`](../contract.md#formato-del-lote). La identidad del
productor forma parte de la cabecera; el valor `-1` indica que no la hay. Los
números de campo que dejan de usarse en el `.proto` quedan reservados.

El productor calcula el CRC y el servidor lo comprueba antes de escribir, de
modo que se detecta cualquier daño en el tramo de red o en el disco. El
servidor valida la cabecera, asigna los offsets y almacena los bytes recibidos
sin decodificar los registros, y la lectura devuelve esos mismos bytes: un solo
formato evita la conversión entre representaciones. La cabecera incluye desde
el principio los campos de identidad de productor y de época de líder, con su
valor nulo, para que activarlos no cambie el formato congelado.

## Alternativas consideradas

- **Un mensaje Protobuf por registro.** Ofrece un contrato más legible y no
  requiere codificador binario en los clientes. Se descarta porque obliga al
  servidor a reempaquetar cada escritura, deja sin verificar la integridad en
  la red y separa el formato de la red del de almacenamiento.
- **Mensajes por registro y, más adelante, un campo adicional con el lote.**
  Añadir el campo no rompe la compatibilidad, pero mantener dos
  representaciones equivalentes duplica la validación y las pruebas. Se
  descarta.

## Consecuencias y límites

- El formato forma parte del contrato. Un cambio requiere un nuevo ADR y un
  nuevo valor de `magic`.
- Los clientes necesitan un codificador de lotes. La biblioteca estándar de
  Python no ofrece CRC-32C, y `zlib.crc32` implementa otro polinomio
  (CRC-32 IEEE), así que hace falta una implementación propia verificada con un
  valor de referencia.
- `Fetch` devuelve lotes completos; el cliente descarta los registros
  anteriores al offset pedido.
- Las herramientas genéricas, como `grpcurl`, muestran el lote como bytes. La
  inspección requiere una herramienta propia.
- La cabecera ocupa 61 bytes por lote, independientemente del número de
  registros.
- La decisión debe revisarse si se adoptan compresión, transacciones u otro
  formato de registro.

## Verificación

- Un vector de prueba, un lote con sus bytes exactos, se produce y se lee igual
  en las implementaciones Python y Go.
- Las pruebas de conformidad comprueban que un lote con CRC incorrecto se
  rechaza con `ERROR_CODE_CORRUPT_MESSAGE`, que uno con campos de cabecera
  inválidos se rechaza con `ERROR_CODE_INVALID_REQUEST` y que en ningún caso se
  escribe nada.
- Reescribir `baseOffset` y `partitionLeaderEpoch` no invalida el CRC.

## Trazabilidad

[`contract.md`](../contract.md#formato-del-lote) ·
`proto/bitacora/v1/bitacora.proto` · [`README.md`](README.md), fila «Formato de
almacenamiento y recuperación».
