package contracttest

import (
	"fmt"
	"testing"
	"time"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
	"github.com/alinadelanoval/bitacora/internal/record"
	"github.com/alinadelanoval/bitacora/pkg/client"
)

const (
	errNone           = pb.ErrorCode_ERROR_CODE_NONE
	errInvalidRequest = pb.ErrorCode_ERROR_CODE_INVALID_REQUEST
)

// Comprobación 1 (básico): escritura y lectura ordenada dentro de una partición.
func TestProduceFetch_SeveralBatches_ContiguousOffsetsInOrder(t *testing.T) {
	e := connect(t)
	topic := e.topic(1, 3)
	var want []string
	for b := range 4 {
		values := []string{fmt.Sprintf("%d-a", b), fmt.Sprintf("%d-b", b), fmt.Sprintf("%d-c", b)}
		if base := e.produce(topic, 0, values...); base != int64(len(want)) {
			t.Fatalf("lote %d: base_offset %d, se esperaba %d", b, base, len(want))
		}
		want = append(want, values...)
	}
	got := e.readAll(topic, 0, 0)
	if len(got) != len(want) {
		t.Fatalf("leídos %d registros, se esperaban %d", len(got), len(want))
	}
	for i, r := range got {
		if r.Offset != int64(i) || string(r.Value) != want[i] {
			t.Fatalf("posición %d: offset %d valor %q; se esperaba %d %q", i, r.Offset, r.Value, i, want[i])
		}
	}
}

// Comprobación 2 (básico): lectura desde un offset explícito, en el LEO y fuera de rango.
func TestFetch_ExplicitOffsetEndAndOutOfRange_FollowRangeRules(t *testing.T) {
	e := connect(t)
	topic := e.topic(1, 3)
	e.produce(topic, 0, "a", "b", "c")
	e.produce(topic, 0, "d", "e")

	// Desde la mitad de un lote: el cliente descarta los registros anteriores.
	got := e.readAll(topic, 0, 1)
	if len(got) != 4 || got[0].Offset != 1 || string(got[0].Value) != "b" {
		t.Fatalf("lectura desde 1: %+v", got)
	}
	// En el LEO: respuesta vacía, sin error, tras esperar como mucho max_wait_ms.
	start := time.Now()
	recs, hw, err := e.c.Fetch(e.ctx(), topic, 0, 5, 1<<20, 200)
	if err != nil || len(recs) != 0 || hw != 5 {
		t.Fatalf("lectura en el LEO: %d registros, hw %d, err %v", len(recs), hw, err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("la espera en el LEO duró %v con max_wait_ms = 200", time.Since(start))
	}
	// Más allá del LEO: OFFSET_OUT_OF_RANGE, sin sustituir la posición.
	_, _, err = e.c.Fetch(e.ctx(), topic, 0, 6, 1<<20, 0)
	if code := client.Code(err); code != pb.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_RANGE {
		t.Fatalf("lectura en 6 con LEO 5: %v", err)
	}
}

// Comprobación 2 (básico): la espera termina en cuanto llegan registros visibles.
func TestFetch_AtEndWhileProducing_ReturnsNewRecords(t *testing.T) {
	e := connect(t)
	topic := e.topic(1, 3)
	done := make(chan []record.Record, 1)
	go func() {
		recs, _, _ := e.c.Fetch(e.ctx(), topic, 0, 0, 1<<20, 10_000)
		done <- recs
	}()
	time.Sleep(300 * time.Millisecond)
	e.produce(topic, 0, "llega")
	select {
	case recs := <-done:
		if len(recs) != 1 || string(recs[0].Value) != "llega" {
			t.Fatalf("la espera devolvió %+v", recs)
		}
	case <-time.After(9 * time.Second):
		t.Fatal("la espera no terminó al llegar el registro")
	}
}

// Comprobación 3 (básico): búsqueda por posición y por fecha. El reprocesamiento
// desde la CLI se comprueba con ResetGroupOffsets en groups_test.go.
func TestListOffsets_EarliestLatestAndTimestamp_ReturnDeclaredPositions(t *testing.T) {
	e := connect(t)
	topic := e.topic(1, 3)
	recs := []record.Record{{TimestampMs: 1000, Value: []byte("a")}, {TimestampMs: 3000, Value: []byte("b")},
		{TimestampMs: 2000, Value: []byte("c")}}
	if _, err := e.c.Produce(e.ctx(), topic, 0, pb.Acks_ACKS_ALL, 10_000, recs); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		position pb.OffsetPosition
		ts       int64
		want     int64
	}{
		{pb.OffsetPosition_OFFSET_POSITION_EARLIEST, 0, 0},
		{pb.OffsetPosition_OFFSET_POSITION_LATEST, 0, 3},
		{pb.OffsetPosition_OFFSET_POSITION_TIMESTAMP, 0, 0},
		{pb.OffsetPosition_OFFSET_POSITION_TIMESTAMP, 1500, 1}, // el primero con marca ≥ 1500, no el de marca más cercana
		{pb.OffsetPosition_OFFSET_POSITION_TIMESTAMP, 2500, 1},
		{pb.OffsetPosition_OFFSET_POSITION_TIMESTAMP, 3001, -1},
	}
	for _, c := range cases {
		got, err := e.c.ListOffsets(e.ctx(), topic, 0, c.position, c.ts)
		if err != nil || got != c.want {
			t.Errorf("%v ts=%d: offset %d err %v; se esperaba %d", c.position, c.ts, got, err, c.want)
		}
	}
	_, err := e.c.ListOffsets(e.ctx(), topic, 0, pb.OffsetPosition_OFFSET_POSITION_UNSPECIFIED, 0)
	if client.Code(err) != errInvalidRequest {
		t.Errorf("posición sin especificar: %v", err)
	}
}

// Comprobación 6 (básico): rechazo de lotes dañados y de campos obligatorios ausentes, sin escribir nada.
func TestProduce_CorruptOrIncompleteRequests_RejectedWithoutWriting(t *testing.T) {
	e := connect(t)
	topic := e.topic(1, 3)
	good, _ := record.Encode([]record.Record{{TimestampMs: 1, Value: []byte("x")}}, record.NoProducer)
	damaged := append([]byte(nil), good...)
	damaged[len(damaged)-1] ^= 0xFF
	partial, _ := record.Encode([]record.Record{{TimestampMs: 1, Value: []byte("x")}},
		record.Producer{ID: 7, Epoch: -1, BaseSequence: -1})
	zero := int32(0)
	unknown := int32(99)

	cases := []struct {
		name string
		req  *pb.ProduceRequest
		want pb.ErrorCode
	}{
		{"CRC incorrecto", &pb.ProduceRequest{Topic: topic, Partition: &zero, Acks: pb.Acks_ACKS_ALL,
			TimeoutMs: 5000, RecordBatch: damaged}, pb.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE},
		{"lote cortado", &pb.ProduceRequest{Topic: topic, Partition: &zero, Acks: pb.Acks_ACKS_ALL,
			TimeoutMs: 5000, RecordBatch: good[:len(good)-1]}, pb.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE},
		{"identidad de productor parcial", &pb.ProduceRequest{Topic: topic, Partition: &zero,
			Acks: pb.Acks_ACKS_ALL, TimeoutMs: 5000, RecordBatch: partial}, errInvalidRequest},
		{"sin partición", &pb.ProduceRequest{Topic: topic, Acks: pb.Acks_ACKS_ALL, TimeoutMs: 5000,
			RecordBatch: good}, errInvalidRequest},
		{"acks sin especificar", &pb.ProduceRequest{Topic: topic, Partition: &zero, TimeoutMs: 5000,
			RecordBatch: good}, errInvalidRequest},
		{"timeout_ms = 0", &pb.ProduceRequest{Topic: topic, Partition: &zero, Acks: pb.Acks_ACKS_ALL,
			RecordBatch: good}, errInvalidRequest},
		{"partición inexistente", &pb.ProduceRequest{Topic: topic, Partition: &unknown, Acks: pb.Acks_ACKS_ALL,
			TimeoutMs: 5000, RecordBatch: good}, pb.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION},
		{"tema inexistente", &pb.ProduceRequest{Topic: name("nadie"), Partition: &zero, Acks: pb.Acks_ACKS_ALL,
			TimeoutMs: 5000, RecordBatch: good}, pb.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION},
	}
	for _, c := range cases {
		resp, err := e.c.Log.Produce(e.ctx(), c.req)
		if err != nil || resp.GetError() != c.want {
			t.Errorf("%s: error %v (canal: %v); se esperaba %v", c.name, resp.GetError(), err, c.want)
		}
	}
	if got := e.readAll(topic, 0, 0); len(got) != 0 {
		t.Fatalf("se escribieron %d registros rechazados", len(got))
	}
	_, _, err := e.c.Fetch(e.ctx(), topic, 0, 0, 0, 0)
	if client.Code(err) != errInvalidRequest {
		t.Errorf("Fetch con max_bytes = 0: %v", err)
	}
}

// ACKS_NONE responde sin offset: el productor no sabe si el lote se guardó.
func TestProduce_AcksNone_ReturnsMinusOne(t *testing.T) {
	e := connect(t)
	topic := e.topic(1, 3)
	base, err := e.c.Produce(e.ctx(), topic, 0, pb.Acks_ACKS_NONE, 5000,
		[]record.Record{{TimestampMs: 1, Value: []byte("x")}})
	if err != nil || base != -1 {
		t.Fatalf("base_offset %d err %v; se esperaba -1", base, err)
	}
}
