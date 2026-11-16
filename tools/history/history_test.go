package history

import (
	"bytes"
	"strings"
	"testing"
)

func TestWriteRead_RoundTrip_KeepsFieldsAndLines(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	in := []Entry{
		{Op: OpProduce, TS: "t0", Topic: "w5", ID: "w-1", Key: "k", Acks: AcksAll, Result: ResultOK, Offset: Int64(0), LatencyUs: 900},
		{Op: OpProduce, TS: "t1", Topic: "w5", ID: "w-2", Acks: AcksAll, Result: ResultIndeterminate, Offset: Int64(-1), Error: "REQUEST_TIMED_OUT"},
		{Op: OpRead, TS: "t2", Topic: "w5", Reader: FinalReader, Offset: Int64(0), Value: "w-1"},
		{Op: OpFinalReadDone, TS: "t3", Topic: "w5", StartOffset: Int64(0), HighWatermark: Int64(1)},
	}
	for _, e := range in {
		if err := w.Write(e); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	out, err := Read(&buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("leídas %d entradas, esperadas %d", len(out), len(in))
	}
	for i := range in {
		if out[i].Line != i+1 {
			t.Errorf("entrada %d: Line = %d", i, out[i].Line)
		}
	}
	if *out[0].Offset != 0 || out[0].LatencyUs != 900 || *out[3].HighWatermark != 1 || out[1].Error != "REQUEST_TIMED_OUT" {
		t.Fatalf("campos perdidos: %+v", out)
	}
}

func TestRead_InvalidLines_ReportsLineNumber(t *testing.T) {
	cases := map[string]string{
		"json mal formado":  `{"op":"produce"`,
		"campo desconocido": `{"op":"read","ts":"","topic":"t","partition":0,"reader":"final","offset":0,"value":"a","extra":1}`,
		"op desconocida":    `{"op":"delete","ts":"","topic":"t","partition":0}`,
		"ok sin offset":     `{"op":"produce","ts":"","topic":"t","partition":0,"id":"a","acks":"all","result":"ok"}`,
		"acks desconocido":  `{"op":"produce","ts":"","topic":"t","partition":0,"id":"a","acks":"quorum","result":"fail"}`,
		"read sin offset":   `{"op":"read","ts":"","topic":"t","partition":0,"reader":"final","value":"a"}`,
		"done invertido":    `{"op":"final_read_done","ts":"","topic":"t","partition":0,"start_offset":5,"high_watermark":2}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			ok := `{"op":"final_read_done","ts":"","topic":"t","partition":0,"start_offset":0,"high_watermark":0}`
			_, err := Read(strings.NewReader(ok + "\n" + line + "\n"))
			if err == nil || !strings.Contains(err.Error(), "línea 2") {
				t.Fatalf("err = %v, se esperaba un error en la línea 2", err)
			}
		})
	}
}

func TestRead_AcksNoneOkWithoutOffset_Accepted(t *testing.T) {
	_, err := Read(strings.NewReader(`{"op":"produce","ts":"","topic":"t","partition":0,"id":"a","acks":"none","result":"ok","offset":-1}` + "\n"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
}

func TestWrite_InvalidEntry_Rejected(t *testing.T) {
	if err := NewWriter(&bytes.Buffer{}).Write(Entry{Op: OpRead, Topic: "t"}); err == nil {
		t.Fatal("Write aceptó un read sin reader")
	}
}
