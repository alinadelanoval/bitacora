package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
	"github.com/alinadelanoval/bitacora/internal/record"
	"github.com/alinadelanoval/bitacora/pkg/client"
	"github.com/alinadelanoval/bitacora/tools/history"
)

const (
	probeReader = "probe"
	fetchBytes  = 1 << 20
	// backoff tras un error, para no repetir en bucle contra una líder caída.
	backoff = 50 * time.Millisecond
)

// Options son los parámetros de una ejecución que no forman parte de la carga.
type Options struct {
	CreateTopic  bool          // crear el tema antes de escribir
	FinalDelay   time.Duration // espera entre el fin de la carga y la lectura final
	FinalTimeout time.Duration // plazo para completar la lectura final
}

// Summary es el resumen de una ejecución: configuración, resultados y cifras.
type Summary struct {
	Workload    Workload       `json:"workload"`
	Endpoint    string         `json:"endpoint"`
	Commit      string         `json:"commit,omitempty"`
	Started     string         `json:"started"`
	DurationS   float64        `json:"duration_s"`
	Writes      map[string]int `json:"writes"` // por resultado: ok, fail, indeterminate
	Errors      map[string]int `json:"errors"` // por código o tipo de error
	ThroughputS float64        `json:"throughput_ok_per_s"`
	LatencyOK   Latency        `json:"latency_ok"`
	ProbePasses int            `json:"probe_passes"`
	ProbeErrors int            `json:"probe_errors"`
	FinalRead   []FinalRead    `json:"final_read"`
}

// FinalRead es el resultado de la lectura final de una partición.
type FinalRead struct {
	Partition     int32  `json:"partition"`
	Complete      bool   `json:"complete"`
	StartOffset   int64  `json:"start_offset"`
	HighWatermark int64  `json:"high_watermark"`
	Records       int    `json:"records"`
	Error         string `json:"error,omitempty"`
}

// Complete indica si todas las particiones tienen lectura final completa.
func (s *Summary) Complete() bool {
	for _, f := range s.FinalRead {
		if !f.Complete {
			return false
		}
	}
	return len(s.FinalRead) > 0
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// classify decide el resultado de una escritura según el contrato
// («Resultado indeterminado»): REQUEST_TIMED_OUT o un fallo del canal no dicen
// si el lote se escribió; otro ErrorCode sí dice que no tuvo efecto.
func classify(err error) (result, reason string) {
	if err == nil {
		return history.ResultOK, ""
	}
	code := client.Code(err)
	switch {
	case code == pb.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT:
		return history.ResultIndeterminate, code.String()
	case code != pb.ErrorCode_ERROR_CODE_NONE:
		return history.ResultFail, code.String()
	case errors.Is(err, context.DeadlineExceeded):
		return history.ResultIndeterminate, "DEADLINE_EXCEEDED"
	default:
		return history.ResultIndeterminate, "CHANNEL: " + err.Error()
	}
}

// errorKey agrupa los errores para el resumen sin el texto variable del canal.
func errorKey(reason string) string {
	if len(reason) > 7 && reason[:7] == "CHANNEL" {
		return "CHANNEL"
	}
	return reason
}

// Run ejecuta la carga: escritores concurrentes que nunca reintentan un id,
// un lector probe que relee la cola del log y, al terminar, la lectura final.
func Run(ctx context.Context, c *client.Client, endpoint string, w Workload, opt Options, out *history.Writer,
) (*Summary, error) {
	acks, err := acksProto(w.Acks)
	if err != nil {
		return nil, err
	}
	if opt.CreateTopic {
		_, err := c.CreateTopic(ctx, "loadgen-"+w.Topic, w.Topic, w.Partitions, w.ReplicationFactor)
		if err != nil {
			return nil, fmt.Errorf("CreateTopic %s: %w", w.Topic, err)
		}
	}

	sum := &Summary{Workload: w, Endpoint: endpoint, Started: now(), Writes: map[string]int{}, Errors: map[string]int{}}
	var (
		mu       sync.Mutex
		okLat    []time.Duration
		next     atomic.Int64
		writeErr atomic.Pointer[error]
	)
	loadCtx := ctx
	if w.DurationS > 0 {
		var cancel context.CancelFunc
		loadCtx, cancel = context.WithTimeout(ctx, time.Duration(w.DurationS)*time.Second)
		defer cancel()
	}
	start := time.Now()

	writer := func() {
		for loadCtx.Err() == nil {
			n := next.Add(1)
			if w.MaxWrites > 0 && n > int64(w.MaxWrites) {
				return
			}
			id := fmt.Sprintf("w-%07d", n)
			part := int32((n - 1) % int64(w.Partitions))
			// El plazo del canal deja margen sobre timeout_ms: así el
			// servidor responde REQUEST_TIMED_OUT antes de que el cliente corte.
			callCtx, cancel := context.WithTimeout(ctx, time.Duration(w.TimeoutMs)*time.Millisecond+2*time.Second)
			t0 := time.Now()
			off, err := c.Produce(callCtx, w.Topic, part, acks, w.TimeoutMs,
				[]record.Record{{TimestampMs: t0.UnixMilli(), Key: []byte(id), Value: []byte(id)}})
			lat := time.Since(t0)
			cancel()
			result, reason := classify(err)
			if result != history.ResultOK || acks == pb.Acks_ACKS_NONE {
				off = -1
			}
			e := history.Entry{Op: history.OpProduce, TS: now(), Topic: w.Topic, Partition: part, ID: id, Key: id,
				Acks: w.Acks, Result: result, Error: reason, Offset: history.Int64(off), LatencyUs: lat.Microseconds(),
				ElapsedUs: time.Since(start).Microseconds()}
			if err := out.Write(e); err != nil {
				writeErr.CompareAndSwap(nil, &err)
				return
			}
			mu.Lock()
			sum.Writes[result]++
			if result == history.ResultOK {
				okLat = append(okLat, lat)
			} else {
				sum.Errors[errorKey(reason)]++
			}
			mu.Unlock()
			if result != history.ResultOK {
				select {
				case <-loadCtx.Done():
				case <-time.After(backoff):
				}
			}
		}
	}

	var wg sync.WaitGroup
	for range w.Concurrency {
		wg.Add(1)
		go func() { defer wg.Done(); writer() }()
	}
	// El probe termina con los escritores, también cuando la carga acaba por
	// max_writes antes del plazo.
	probeCtx, stopProbe := context.WithCancel(loadCtx)
	defer stopProbe()
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		if w.ProbeIntervalMs > 0 {
			sum.ProbePasses, sum.ProbeErrors = probe(probeCtx, c, w, out)
		}
	}()
	wg.Wait()
	elapsed := time.Since(start)
	stopProbe()
	<-probeDone
	if p := writeErr.Load(); p != nil {
		return nil, fmt.Errorf("historial: %w", *p)
	}

	sum.DurationS = elapsed.Seconds()
	sum.LatencyOK = summarize(okLat)
	if elapsed > 0 {
		sum.ThroughputS = float64(sum.Writes[history.ResultOK]) / elapsed.Seconds()
	}

	select {
	case <-ctx.Done():
		return sum, ctx.Err()
	case <-time.After(opt.FinalDelay):
	}
	for p := range w.Partitions {
		fr := finalRead(ctx, c, w.Topic, p, opt.FinalTimeout, out)
		sum.FinalRead = append(sum.FinalRead, fr)
	}
	return sum, nil
}
