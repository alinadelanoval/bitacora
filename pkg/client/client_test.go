package client

import (
	"errors"
	"fmt"
	"testing"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
)

func TestCode_ContractError_ReturnsItsCode(t *testing.T) {
	err := fmt.Errorf("envuelto: %w", check("Produce", pb.ErrorCode_ERROR_CODE_NOT_LEADER))
	if got := Code(err); got != pb.ErrorCode_ERROR_CODE_NOT_LEADER {
		t.Fatalf("Code = %v", got)
	}
}

func TestCode_NilOrChannelError_ReturnsNone(t *testing.T) {
	for _, err := range []error{nil, errors.New("conexión rechazada")} {
		if got := Code(err); got != pb.ErrorCode_ERROR_CODE_NONE {
			t.Fatalf("Code(%v) = %v", err, got)
		}
	}
}

func TestCheck_None_ReturnsNil(t *testing.T) {
	if err := check("Fetch", pb.ErrorCode_ERROR_CODE_NONE); err != nil {
		t.Fatalf("check(NONE) = %v", err)
	}
}

func TestDial_InvalidTarget_DoesNotConnectUntilUsed(t *testing.T) {
	// grpc.NewClient no abre la conexión al crear el cliente: Dial no falla
	// aunque no haya servidor; el error llega en la primera llamada.
	c, err := Dial("127.0.0.1:1")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
