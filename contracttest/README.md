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

## Ejecución

```sh
make contract                                        # contra el gateway del prototipo Ray
BITACORA_ENDPOINTS=127.0.0.1:9091 BITACORA_PROFILE=basic go test -count=1 ./contracttest/
```

`BITACORA_PROFILE` declara el perfil de la implementación: `basic` (por
defecto) o `complete`. Sin `BITACORA_ENDPOINTS` las pruebas se saltan, de modo
que `make verify` no necesita un servidor. Cada prueba crea temas y grupos con
nombres aleatorios: la suite puede repetirse contra el mismo endpoint.

## Casos

Numeración de la tabla «Conformidad» de [`docs/contract.md`](../docs/contract.md).

| # | Prueba | Perfil |
|---|---|---|
| 1 | `TestProduceFetch_SeveralBatches_ContiguousOffsetsInOrder` | Básico |
| 2 | `TestFetch_ExplicitOffsetEndAndOutOfRange_FollowRangeRules`, `TestFetch_AtEndWhileProducing_ReturnsNewRecords` | Básico |
| 3 | `TestListOffsets_EarliestLatestAndTimestamp_ReturnDeclaredPositions`, `TestResetGroupOffsets_OnlyWhenEmpty_MovesPositionBack` | Básico |
| 4 | `TestGroup_RestartedMember_ResumesFromCommittedOffset` | Básico |
| 5 | `TestGroup_MembersJoin_ExclusiveAssignmentAndFencedCommits`, `TestHeartbeat_UnknownOrStaleMembers_AreRejectedAndLeaveIsIdempotent` | Básico |
| 6 | `TestProduce_CorruptOrIncompleteRequests_RejectedWithoutWriting` | Básico |
| — | `TestCreateTopic_RetryAndConflict_FollowRequestID`, `TestGetMetadata_CreatedTopic_ReportsLeaderReplicasAndISR`, `TestProduce_AcksNone_ReturnsMinusOne` | Básico |
| — | `TestInitProducerID_BasicProfile_IsUnimplemented` | Básico (en el completo, pendiente) |
| 7–10 | Pendientes: llegan con la versión nativa (BIT-61, BIT-63, BIT-80, BIT-86) | Completo |

La comprobación 3 incluye el reprocesamiento desde la CLI; hasta que exista la
CLI (BIT-81), se comprueba con `ResetGroupOffsets`, la operación que usará.
