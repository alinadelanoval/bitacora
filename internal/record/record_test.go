package record

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

// vector es un caso de testdata/batch_vectors.json, el mismo archivo que usa
// prototype/tests/test_batch_vectors.py.
type vector struct {
	Name     string `json:"name"`
	Producer *struct {
		ID           int64 `json:"id"`
		Epoch        int16 `json:"epoch"`
		BaseSequence int32 `json:"base_sequence"`
	} `json:"producer"`
	Records []struct {
		Key         *string `json:"key"`
		Value       *string `json:"value"`
		TimestampMs int64   `json:"timestamp_ms"`
	} `json:"records"`
	Batch string `json:"batch"`
}

func loadVectors(t *testing.T) []vector {
	t.Helper()
	raw, err := os.ReadFile("testdata/batch_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vs []vector
	if err := json.Unmarshal(raw, &vs); err != nil {
		t.Fatal(err)
	}
	return vs
}

func optional(s *string) []byte {
	if s == nil {
		return nil
	}
	return []byte(*s)
}

func (v vector) input() ([]Record, Producer) {
	recs := make([]Record, len(v.Records))
	for i, r := range v.Records {
		recs[i] = Record{TimestampMs: r.TimestampMs, Key: optional(r.Key), Value: optional(r.Value)}
	}
	p := NoProducer
	if v.Producer != nil {
		p = Producer{ID: v.Producer.ID, Epoch: v.Producer.Epoch, BaseSequence: v.Producer.BaseSequence}
	}
	return recs, p
}

func TestEncode_SharedVectors_MatchBytes(t *testing.T) {
	for _, v := range loadVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			recs, p := v.input()
			got, err := Encode(recs, p)
			if err != nil {
				t.Fatal(err)
			}
			if want := v.Batch; hex.EncodeToString(got) != want {
				t.Fatalf("lote distinto\n got %x\nwant %s", got, want)
			}
		})
	}
}

func TestDecode_SharedVectors_ReturnRecordsWithOffsets(t *testing.T) {
	for _, v := range loadVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			data, _ := hex.DecodeString(v.Batch)
			copy(data, []byte{0, 0, 0, 0, 0, 0, 0, 100}) // baseOffset = 100, fuera del CRC
			h, got, err := Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			want, p := v.input()
			if h.Producer != p || int(h.RecordCount) != len(want) {
				t.Fatalf("cabecera %+v", h)
			}
			for i := range want {
				want[i].Offset = 100 + int64(i)
				if got[i].Offset != want[i].Offset || got[i].TimestampMs != want[i].TimestampMs ||
					!bytes.Equal(got[i].Key, want[i].Key) || (got[i].Key == nil) != (want[i].Key == nil) ||
					!bytes.Equal(got[i].Value, want[i].Value) || (got[i].Value == nil) != (want[i].Value == nil) {
					t.Fatalf("registro %d: got %+v want %+v", i, got[i], want[i])
				}
			}
		})
	}
}

func TestDecode_DamagedByte_ReturnsErrCorrupt(t *testing.T) {
	data, _ := Encode([]Record{{TimestampMs: 1, Value: []byte("hola")}}, NoProducer)
	data[len(data)-2] ^= 0xFF
	if _, _, err := Decode(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestDecode_WrongLengthOrMagic_ReturnsErrCorrupt(t *testing.T) {
	data, _ := Encode([]Record{{TimestampMs: 1, Value: []byte("hola")}}, NoProducer)
	if _, _, err := Decode(data[:len(data)-1]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("lote cortado: err = %v", err)
	}
	bad := bytes.Clone(data)
	bad[16] = 1
	if _, _, err := Decode(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("magic 1: err = %v", err)
	}
}

func TestEncode_NoRecords_ReturnsError(t *testing.T) {
	if _, err := Encode(nil, NoProducer); err == nil {
		t.Fatal("se esperaba un error")
	}
}

func TestSplit_ConcatenatedBatches_ReturnsEach(t *testing.T) {
	a, _ := Encode([]Record{{TimestampMs: 1, Value: []byte("a")}}, NoProducer)
	b, _ := Encode([]Record{{TimestampMs: 2, Value: []byte("b")}, {TimestampMs: 3, Value: []byte("c")}}, NoProducer)
	got, err := Split(append(bytes.Clone(a), b...))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !bytes.Equal(got[0], a) || !bytes.Equal(got[1], b) {
		t.Fatalf("Split devolvió %d lotes", len(got))
	}
	if _, err := Split(append(bytes.Clone(a), b[:20]...)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("lote cortado: err = %v", err)
	}
}
