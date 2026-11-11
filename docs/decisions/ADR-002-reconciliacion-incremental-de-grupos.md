# ADR-002: reconciliación incremental de las asignaciones de grupo

- **Estado:** propuesta
- **Fecha:** 2026-11-11
- **Responsables:** equipo de Bitácora
- **Requisitos / historias relacionados:** RF-06, RF-07; I-06, I-07

## Contexto

Al cambiar la membresía de un grupo, alguna partición pasa de un miembro a
otro. Los dos miembros son procesos distintos que solo conocen el cambio al
comunicarse con el coordinador, de modo que el traspaso no puede ser
instantáneo. El contrato debe fijar qué ocurre durante ese intervalo, porque es
comportamiento observable: si dos miembros pueden leer la misma partición, si
el grupo entero se detiene y qué mensajes intercambian cliente y coordinador.

## Decisión

Las asignaciones convergen de forma incremental mediante los latidos:

- Cada cambio de membresía incrementa la época del grupo y produce un reparto
  objetivo con la estrategia `range`.
- En cada latido, el miembro envía todas las particiones que tiene. El
  coordinador le devuelve las que puede usar y las pendientes.
- Una partición solo se entrega a un miembro cuando ningún otro la tiene en su
  asignación actual.
- La época de cada miembro avanza a la del grupo cuando ha dejado las
  particiones que ya no le corresponden. Los latidos y los commits se validan
  con la época del miembro; un commit solo se acepta para particiones que el
  miembro tiene, incluidas las que está dejando.
- Entrar, latir y salir usan la misma operación, `Heartbeat`, distinguidas por
  la época del miembro (`0`, positiva y `-1`). Se retiran `JoinGroup` y
  `LeaveGroup`.
- El servidor fija el intervalo de latido, el plazo de sesión y el plazo para
  dejar particiones. Un miembro que no deja una partición a tiempo sale del
  grupo.

## Alternativas consideradas

- **Respuesta inmediata.** El coordinador entrega el reparto nuevo en cuanto
  cambia la membresía y los demás miembros lo conocen en su siguiente latido.
  Es la más sencilla, pero durante ese intervalo dos miembros leen la misma
  partición, y lo que el miembro anterior procesó sin confirmar se procesa dos
  veces.
- **Barrera.** Todos los miembros dejan todas sus particiones y esperan a que
  los demás vuelvan a unirse. Evita los lectores duplicados, pero detiene todo
  el grupo en cada cambio, incluidas las particiones que no cambian de miembro,
  y un miembro lento retrasa a todos.
- **Mantener `JoinGroup` y `LeaveGroup` ampliando sus mensajes.** Cambia menos
  el contrato, pero la asignación se entregaría por dos operaciones distintas.

## Consecuencias y límites

- Nunca hay dos miembros con la misma partición en su asignación actual, y
  solo se detienen las particiones que cambian de miembro.
- Si el miembro anterior confirma su progreso antes de dejar la partición, el
  nuevo continúa exactamente donde lo dejó.
- El coordinador mantiene estado por miembro y por partición, y debe persistir
  la asignación actual de cada miembro para conservar esa propiedad tras un
  reinicio.
- Los clientes implementan la reconciliación: comparar lo que tienen con lo que
  reciben, confirmar y dejar las particiones retiradas.
- `ERROR_CODE_REBALANCE_IN_PROGRESS` queda reservado y sin uso.
- No evita el procesamiento repetido de lo leído y no confirmado cuando un
  miembro falla: la entrega sigue siendo al menos una vez.
- La versión nativa implementa la misma reconciliación, con el estado del
  coordinador replicado.

## Verificación

- Invariante comprobado tras cada operación en las pruebas del coordinador:
  ninguna partición en la asignación actual de dos miembros.
- Pruebas de entrada, salida, expiración de sesión, plazo de reequilibrio,
  época obsoleta, commit de una partición que se está dejando y reinicio del
  coordinador.
- Casos de `range` compartidos entre implementaciones en
  `internal/group/testdata/range_cases.json`.

## Trazabilidad

[`contract.md`](../contract.md#grupos-de-consumidores);
`proto/bitacora/v1/bitacora.proto` (servicio `Groups`); Issue #5.
