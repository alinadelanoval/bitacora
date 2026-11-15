package contracttest

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
	"github.com/alinadelanoval/bitacora/pkg/client"
)

// CreateTopic: el mismo request_id es un reintento; otro request_id choca con el tema existente.
func TestCreateTopic_RetryAndConflict_FollowRequestID(t *testing.T) {
	e := connect(t)
	topic, req := name("ct"), name("req")
	ctx, cancel := context.WithTimeout(context.Background(), createTimeout)
	defer cancel()

	first, err := e.c.CreateTopic(ctx, req, topic, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.GetPartitions()) != 3 {
		t.Fatalf("metadatos con %d particiones", len(first.GetPartitions()))
	}
	if _, err := e.c.CreateTopic(ctx, req, topic, 3, 3); err != nil {
		t.Fatalf("reintento con el mismo request_id: %v", err)
	}
	if _, err := e.c.CreateTopic(ctx, name("req"), topic, 3, 3); client.Code(err) !=
		pb.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS {
		t.Fatalf("otra petición con el mismo nombre: %v", err)
	}
	cases := []struct {
		name           string
		topic          string
		partitions, rf int32
		want           pb.ErrorCode
	}{
		{"factor de replicación mayor que los nodos", name("ct"), 1, 99, pb.ErrorCode_ERROR_CODE_INVALID_REPLICATION_FACTOR},
		{"sin particiones", name("ct"), 0, 1, errInvalidRequest},
		{"nombre con «/»", "a/../b", 1, 1, errInvalidRequest},
		{"nombre «..»", "..", 1, 1, errInvalidRequest},
	}
	for _, c := range cases {
		if _, err := e.c.CreateTopic(ctx, name("req"), c.topic, c.partitions, c.rf); client.Code(err) != c.want {
			t.Errorf("%s: %v; se esperaba %v", c.name, err, c.want)
		}
	}
}

// GetMetadata: líder, réplicas e ISR por partición; un tema desconocido es un error.
func TestGetMetadata_CreatedTopic_ReportsLeaderReplicasAndISR(t *testing.T) {
	e := connect(t)
	topic := e.topic(2, 3)
	resp, err := e.c.Admin.GetMetadata(e.ctx(), &pb.GetMetadataRequest{Topics: []string{topic}})
	if err != nil || resp.GetError() != errNone || len(resp.GetTopics()) != 1 {
		t.Fatalf("GetMetadata: %v %v", err, resp)
	}
	for _, p := range resp.GetTopics()[0].GetPartitions() {
		if len(p.GetReplicaIds()) != 3 || p.GetLeaderId() == "" {
			t.Errorf("partición %d: %+v", p.GetPartition(), p)
		}
		inISR := false
		for _, id := range p.GetIsrIds() {
			inISR = inISR || id == p.GetLeaderId()
		}
		if !inISR {
			t.Errorf("partición %d: el líder %s no está en el ISR %v", p.GetPartition(), p.GetLeaderId(), p.GetIsrIds())
		}
	}
	missing, err := e.c.Admin.GetMetadata(e.ctx(), &pb.GetMetadataRequest{Topics: []string{name("nadie")}})
	if err != nil || missing.GetError() != pb.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatalf("tema desconocido: %v %v", err, missing.GetError())
	}
}

// InitProducerId: una implementación básica responde UNIMPLEMENTED (docs/contract.md, «Perfiles»).
func TestInitProducerID_BasicProfile_IsUnimplemented(t *testing.T) {
	e := connect(t)
	if e.profile == profileComplete {
		t.Skip("pendiente: la comprobación del perfil completo llega con la identidad de productor (BIT-80)")
	}
	_, err := e.c.Admin.InitProducerId(e.ctx(), &pb.InitProducerIdRequest{RequestId: name("req")})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("estado gRPC %v; se esperaba UNIMPLEMENTED", status.Code(err))
	}
}
