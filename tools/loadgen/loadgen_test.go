package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
	"github.com/alinadelanoval/bitacora/pkg/client"
	"github.com/alinadelanoval/bitacora/tools/history"
)

// Acciones del servidor falso para la escritura n (desde 1).
const (
	normal    = iota
	notLeader // rechaza sin escribir
	timedOut  // escribe y responde REQUEST_TIMED_OUT
	duplicate // escribe el lote dos veces (como un reintento interno)
)

// fakeLog es un log en memoria de una partición que habla el contrato.
type fakeLog struct {
	pb.UnimplementedLogServer
	pb.UnimplementedAdminServer
	mu          sync.Mutex
	batches     [][]byte // un registro por lote: el índice es el offset
	produced    int
	action      func(n int) int
	fetchErrors int // las primeras fetchErrors lecturas responden NOT_LEADER
}

func (f *fakeLog) CreateTopic(_ context.Context, r *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	return &pb.CreateTopicResponse{Metadata: &pb.TopicMetadata{Topic: r.Topic}}, nil
}

func (f *fakeLog) appendLocked(batch []byte) int64 {
	b := append([]byte(nil), batch...)
	off := int64(len(f.batches))
	binary.BigEndian.PutUint64(b[0:], uint64(off)) // el CRC no cubre baseOffset
	f.batches = append(f.batches, b)
	return off
}

func (f *fakeLog) Produce(_ context.Context, r *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.produced++
	act := normal
	if f.action != nil {
		act = f.action(f.produced)
	}
	switch act {
	case notLeader:
		return &pb.ProduceResponse{Error: pb.ErrorCode_ERROR_CODE_NOT_LEADER}, nil
	case timedOut:
		f.appendLocked(r.RecordBatch)
		return &pb.ProduceResponse{Error: pb.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT}, nil
	case duplicate:
		off := f.appendLocked(r.RecordBatch)
		f.appendLocked(r.RecordBatch)
		return &pb.ProduceResponse{BaseOffset: off}, nil
	}
	return &pb.ProduceResponse{BaseOffset: f.appendLocked(r.RecordBatch)}, nil
}

func (f *fakeLog) Fetch(_ context.Context, r *pb.FetchRequest) (*pb.FetchResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetchErrors > 0 {
		f.fetchErrors--
		return &pb.FetchResponse{Error: pb.ErrorCode_ERROR_CODE_NOT_LEADER}, nil
	}
	hw := int64(len(f.batches))
	var data []byte
	for off := r.Offset; off < hw && int32(len(data)) < r.MaxBytes; off++ {
		data = append(data, f.batches[off]...)
	}
	return &pb.FetchResponse{HighWatermark: hw, RecordBatches: data}, nil
}

func (f *fakeLog) ListOffsets(_ context.Context, r *pb.ListOffsetsRequest) (*pb.ListOffsetsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Position == pb.OffsetPosition_OFFSET_POSITION_EARLIEST {
		return &pb.ListOffsetsResponse{Offset: 0}, nil
	}
	return &pb.ListOffsetsResponse{Offset: int64(len(f.batches))}, nil
}

func serve(t *testing.T, f *fakeLog) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterLogServer(s, f)
	pb.RegisterAdminServer(s, f)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func testWorkload(maxWrites int) Workload {
	return Workload{Name: "test", Topic: "w5", Partitions: 1, ReplicationFactor: 3, Acks: history.AcksAll,
		Concurrency: 4, MaxWrites: maxWrites, TimeoutMs: 1000, ProbeIntervalMs: 5, ProbeWindow: 20}
}

// runLoad ejecuta la carga contra f y devuelve el resumen y el historial.
func runLoad(t *testing.T, f *fakeLog, w Workload) (*Summary, string, []history.Entry) {
	t.Helper()
	addr := serve(t, f)
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	path := filepath.Join(t.TempDir(), "history.ndjson")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	sum, err := Run(context.Background(), c, addr, w, Options{CreateTopic: true, FinalTimeout: 5 * time.Second},
		history.NewWriter(out))
	out.Close()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	es, err := history.ReadFile(path)
	if err != nil {
		t.Fatalf("historial ilegible: %v", err)
	}
	return sum, path, es
}

// checker ejecuta tools/checker sobre el historial y devuelve código y salida.
func checker(t *testing.T, path string) (int, string) {
	t.Helper()
	cmd := exec.Command("go", "run", "../checker", "-history", path)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, out.String()
	case errors.As(err, &exit):
		return exit.ExitCode(), out.String()
	}
	t.Fatalf("checker: %v", err)
	return 0, ""
}

func count(es []history.Entry, op, result string) int {
	n := 0
	for _, e := range es {
		if e.Op == op && (result == "" || e.Result == result) {
			n++
		}
	}
	return n
}

func TestRun_HealthyServer_CheckerVerifiesAll(t *testing.T) {
	sum, path, es := runLoad(t, &fakeLog{}, testWorkload(300))
	if sum.Writes[history.ResultOK] != 300 || count(es, history.OpProduce, history.ResultOK) != 300 {
		t.Fatalf("escrituras ok: resumen %v, historial %d", sum.Writes, count(es, history.OpProduce, history.ResultOK))
	}
	if !sum.Complete() || sum.FinalRead[0].Records != 300 || sum.FinalRead[0].HighWatermark != 300 {
		t.Fatalf("lectura final: %+v", sum.FinalRead)
	}
	if sum.LatencyOK.Count != 300 || sum.LatencyOK.P50 <= 0 || sum.LatencyOK.P99 < sum.LatencyOK.P50 {
		t.Fatalf("latencia: %+v", sum.LatencyOK)
	}
	code, out := checker(t, path)
	if code != 0 {
		t.Fatalf("checker salió con %d:\n%s", code, out)
	}
	for _, id := range []string{"I-02", "I-03", "I-04", "I-08"} {
		if !strings.Contains(out, id) || strings.Contains(out, "violated") {
			t.Fatalf("informe inesperado:\n%s", out)
		}
	}
}

func TestRun_EachIdSentOnce_NoClientRetries(t *testing.T) {
	f := &fakeLog{action: func(n int) int {
		if n%5 == 0 {
			return notLeader
		}
		return normal
	}}
	sum, _, es := runLoad(t, f, testWorkload(100))
	seen := map[string]bool{}
	for _, e := range es {
		if e.Op != history.OpProduce {
			continue
		}
		if seen[e.ID] {
			t.Fatalf("id %s enviado dos veces", e.ID)
		}
		seen[e.ID] = true
	}
	if len(seen) != 100 || f.produced != 100 {
		t.Fatalf("ids %d, peticiones al servidor %d; esperados 100", len(seen), f.produced)
	}
	if sum.Writes[history.ResultFail] != 20 || sum.Errors["ERROR_CODE_NOT_LEADER"] != 20 {
		t.Fatalf("resumen: %v %v", sum.Writes, sum.Errors)
	}
}

func TestRun_RequestTimedOut_IndeterminateAndNotObliged(t *testing.T) {
	f := &fakeLog{action: func(n int) int {
		if n%4 == 0 {
			return timedOut
		}
		return normal
	}}
	_, path, es := runLoad(t, f, testWorkload(80))
	if n := count(es, history.OpProduce, history.ResultIndeterminate); n != 20 {
		t.Fatalf("indeterminadas = %d, esperadas 20", n)
	}
	for _, e := range es {
		if e.Result == history.ResultIndeterminate && (e.Offset == nil || *e.Offset != -1 || e.Error != "ERROR_CODE_REQUEST_TIMED_OUT") {
			t.Fatalf("entrada indeterminada mal registrada: %+v", e)
		}
	}
	// Escritas aunque indeterminadas, aparecen una vez: nada que objetar.
	if code, out := checker(t, path); code != 0 {
		t.Fatalf("checker salió con %d:\n%s", code, out)
	}
}

func TestRun_ServerDuplicatesBatch_CheckerReportsDuplicate(t *testing.T) {
	f := &fakeLog{action: func(n int) int {
		if n == 10 {
			return duplicate
		}
		return normal
	}}
	_, path, _ := runLoad(t, f, testWorkload(30))
	code, out := checker(t, path)
	if code != 1 || !strings.Contains(out, "duplicate") {
		t.Fatalf("checker salió con %d, se esperaba duplicate:\n%s", code, out)
	}
}

func TestRun_FetchFailsAtFirst_FinalReadRetriesAndCompletes(t *testing.T) {
	w := testWorkload(20)
	w.ProbeIntervalMs = 0
	f := &fakeLog{fetchErrors: 3}
	sum, path, es := runLoad(t, f, w)
	if !sum.Complete() || count(es, history.OpFinalReadDone, "") != 1 || count(es, history.OpRead, "") != 20 {
		t.Fatalf("lectura final: %+v; lecturas en el historial %d", sum.FinalRead, count(es, history.OpRead, ""))
	}
	if code, out := checker(t, path); code != 0 {
		t.Fatalf("checker salió con %d:\n%s", code, out)
	}
}

func TestRun_Probe_RereadsOffsets(t *testing.T) {
	w := testWorkload(0)
	w.DurationS, w.Concurrency = 1, 1
	sum, path, es := runLoad(t, &fakeLog{}, w)
	if sum.ProbePasses < 2 {
		t.Fatalf("probe: %d pasadas", sum.ProbePasses)
	}
	reads := map[int64]int{}
	for _, e := range es {
		if e.Op == history.OpRead && e.Reader == probeReader {
			reads[*e.Offset]++
		}
	}
	reread := 0
	for _, n := range reads {
		if n > 1 {
			reread++
		}
	}
	if reread == 0 {
		t.Fatal("el probe no relee ningún offset: I-01 quedaría sin evaluar")
	}
	if code, out := checker(t, path); code != 0 || !strings.Contains(out, "I-01") || strings.Contains(out, "not_evaluable") {
		t.Fatalf("checker %d:\n%s", code, out)
	}
}

func TestClassify_ContractAndChannelErrors(t *testing.T) {
	cases := []struct {
		err    error
		result string
	}{
		{nil, history.ResultOK},
		{&client.Error{Op: "Produce", Code: pb.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT}, history.ResultIndeterminate},
		{&client.Error{Op: "Produce", Code: pb.ErrorCode_ERROR_CODE_NOT_LEADER}, history.ResultFail},
		{context.DeadlineExceeded, history.ResultIndeterminate},
		{errors.New("conexión interrumpida"), history.ResultIndeterminate},
	}
	for _, c := range cases {
		if got, _ := classify(c.err); got != c.result {
			t.Errorf("classify(%v) = %s, esperado %s", c.err, got, c.result)
		}
	}
}

func TestSummarize_NearestRank(t *testing.T) {
	var ds []time.Duration
	for i := 100; i >= 1; i-- {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	l := summarize(ds)
	if l.Count != 100 || l.P50 != 50 || l.P95 != 95 || l.P99 != 99 || l.Max != 100 {
		t.Fatalf("summarize = %+v", l)
	}
	if (summarize(nil) != Latency{}) {
		t.Fatal("summarize(nil) no es cero")
	}
}

func TestLoadWorkload_W5_Valid(t *testing.T) {
	w, err := LoadWorkload("workloads/w5.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Validate(); err != nil {
		t.Fatal(err)
	}
	if w.Partitions != 1 || w.ReplicationFactor != 3 || w.Acks != history.AcksAll {
		t.Fatalf("W5 = %+v", w)
	}
}

func TestValidate_InvalidWorkloads_Rejected(t *testing.T) {
	base := testWorkload(10)
	for name, mod := range map[string]func(*Workload){
		"sin tema":            func(w *Workload) { w.Topic = "" },
		"acks desconocido":    func(w *Workload) { w.Acks = "quorum" },
		"sin fin":             func(w *Workload) { w.MaxWrites, w.DurationS = 0, 0 },
		"sin escritores":      func(w *Workload) { w.Concurrency = 0 },
		"probe sin ventana":   func(w *Workload) { w.ProbeWindow = 0 },
		"timeout no positivo": func(w *Workload) { w.TimeoutMs = 0 },
	} {
		w := base
		mod(&w)
		if w.Validate() == nil {
			t.Errorf("%s: Validate aceptó %+v", name, w)
		}
	}
}

func TestMain_MissingFlags_ExitsTwo(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"-endpoint", "127.0.0.1:1"}, &stdout, &stderr); code != 2 {
		t.Fatalf("código %d, esperado 2", code)
	}
}

func TestMain_EndToEnd_WritesHistoryAndSummary(t *testing.T) {
	addr := serve(t, &fakeLog{})
	dir := t.TempDir()
	hist, summary := filepath.Join(dir, "h.ndjson"), filepath.Join(dir, "s.json")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-endpoint", addr, "-workload", "workloads/w5.json", "-history", hist,
		"-summary", summary, "-max-writes", "50", "-acks", "leader"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("código %d\n%s\n%s", code, &stdout, &stderr)
	}
	for _, want := range []string{"acks=leader", "ok=50", "p99=", "lectura final p0: 50 registros"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("falta %q en:\n%s", want, &stdout)
		}
	}
	if _, err := os.Stat(summary); err != nil {
		t.Fatal(err)
	}
}

func TestRun_ElapsedIsMonotonicAndCoversLatency(t *testing.T) {
	_, _, es := runLoad(t, &fakeLog{}, testWorkload(100))
	n := 0
	for _, e := range es {
		if e.Op != history.OpProduce {
			continue
		}
		n++
		// La respuesta llega después de enviar: elapsed >= latencia > 0.
		if e.ElapsedUs <= 0 || e.ElapsedUs < e.LatencyUs {
			t.Fatalf("%s: elapsed_us=%d latency_us=%d", e.ID, e.ElapsedUs, e.LatencyUs)
		}
	}
	if n != 100 {
		t.Fatalf("%d escrituras", n)
	}
}
