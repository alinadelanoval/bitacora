// Package history define el historial NDJSON que escribe el generador de carga
// y lee el verificador (tools/checker/README.md): una línea por escritura, una
// por registro leído y una por lectura final completada.
package history

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// Tipos de línea (campo op).
const (
	OpProduce       = "produce"         // una escritura y su resultado
	OpRead          = "read"            // un registro leído por un lector
	OpFinalReadDone = "final_read_done" // la lectura final de una partición llegó a su HW
)

// Resultados de una escritura (docs/contract.md, «Resultado indeterminado»).
const (
	ResultOK            = "ok"            // offset conocido
	ResultFail          = "fail"          // se sabe que no tuvo efecto
	ResultIndeterminate = "indeterminate" // timeout, conexión interrumpida o REQUEST_TIMED_OUT
)

// Niveles de confirmación.
const (
	AcksNone   = "none"
	AcksLeader = "leader"
	AcksAll    = "all"
)

// FinalReader es el lector de la lectura final: un grupo nuevo que lee desde
// el inicio hasta el HW después de sanar el fallo.
const FinalReader = "final"

// Entry es una línea del historial. Los campos usados dependen de Op.
type Entry struct {
	Op        string `json:"op"`
	TS        string `json:"ts"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`

	// produce
	ID        string `json:"id,omitempty"` // identificador lógico; es también el valor del registro
	Key       string `json:"key,omitempty"`
	Acks      string `json:"acks,omitempty"`
	Result    string `json:"result,omitempty"`
	Error     string `json:"error,omitempty"`
	LatencyUs int64  `json:"latency_us,omitempty"`
	// ElapsedUs: microsegundos desde el inicio de la carga hasta la respuesta,
	// con reloj monotónico. ts es la hora de pared, que puede saltar (por
	// ejemplo, cuando WSL2 corrige su reloj): los intervalos se calculan con
	// ElapsedUs.
	ElapsedUs int64 `json:"elapsed_us,omitempty"`

	// produce (offset devuelto, -1 si no es ok) y read (offset leído)
	Offset *int64 `json:"offset,omitempty"`

	// read
	Reader string `json:"reader,omitempty"`
	Value  string `json:"value,omitempty"`

	// final_read_done
	StartOffset   *int64 `json:"start_offset,omitempty"`
	HighWatermark *int64 `json:"high_watermark,omitempty"`

	// Line es la línea del historial (desde 1); no se serializa.
	Line int `json:"-"`
}

// Int64 devuelve un puntero a v, para los campos opcionales.
func Int64(v int64) *int64 { return &v }

// Validate comprueba que la entrada tiene los campos de su tipo.
func (e *Entry) Validate() error {
	if e.Topic == "" {
		return fmt.Errorf("falta topic")
	}
	switch e.Op {
	case OpProduce:
		if e.ID == "" {
			return fmt.Errorf("produce sin id")
		}
		switch e.Acks {
		case AcksNone, AcksLeader, AcksAll:
		default:
			return fmt.Errorf("acks desconocido %q", e.Acks)
		}
		switch e.Result {
		case ResultOK:
			if e.Acks != AcksNone && (e.Offset == nil || *e.Offset < 0) {
				return fmt.Errorf("produce ok sin offset")
			}
		case ResultFail, ResultIndeterminate:
		default:
			return fmt.Errorf("result desconocido %q", e.Result)
		}
	case OpRead:
		if e.Reader == "" {
			return fmt.Errorf("read sin reader")
		}
		if e.Offset == nil || *e.Offset < 0 {
			return fmt.Errorf("read sin offset")
		}
	case OpFinalReadDone:
		if e.StartOffset == nil || e.HighWatermark == nil {
			return fmt.Errorf("final_read_done sin start_offset o high_watermark")
		}
		if *e.StartOffset > *e.HighWatermark {
			return fmt.Errorf("start_offset %d mayor que high_watermark %d", *e.StartOffset, *e.HighWatermark)
		}
	default:
		return fmt.Errorf("op desconocida %q", e.Op)
	}
	return nil
}

// Read lee un historial completo. Rechaza líneas mal formadas o con campos
// desconocidos: un historial que no se entiende no se verifica.
func Read(r io.Reader) ([]Entry, error) {
	var out []Entry
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		var e Entry
		if err := dec.Decode(&e); err != nil {
			return nil, fmt.Errorf("línea %d: %w", n, err)
		}
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("línea %d: %w", n, err)
		}
		e.Line = n
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ReadFile lee el historial de path.
func ReadFile(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Read(f)
}

// Writer escribe entradas como NDJSON; es seguro desde varias gorrutinas.
type Writer struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewWriter escribe en w.
func NewWriter(w io.Writer) *Writer { return &Writer{enc: json.NewEncoder(w)} }

// Write valida y escribe una entrada.
func (w *Writer) Write(e Entry) error {
	if err := e.Validate(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(&e)
}
