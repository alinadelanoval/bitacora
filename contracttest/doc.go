// Package contracttest contiene las pruebas públicas de conformidad del
// contrato bitacora.v1 (docs/contract.md, «Conformidad»).
//
// Se ejecutan contra un endpoint gRPC, sin saber qué implementación hay
// detrás:
//
//	BITACORA_ENDPOINTS=127.0.0.1:9091 BITACORA_PROFILE=basic go test ./contracttest/
//
// Sin BITACORA_ENDPOINTS las pruebas se saltan. BITACORA_PROFILE declara el
// perfil que cumple la implementación («basic» por defecto o «complete»); las
// comprobaciones del perfil completo se informan como «no aplica» contra una
// implementación básica, nunca como superadas.
//
// `make contract` arranca el gateway del prototipo Ray y ejecuta la suite
// contra él.
package contracttest
