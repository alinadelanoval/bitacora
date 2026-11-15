package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
	"github.com/alinadelanoval/bitacora/tools/history"
)

// Workload es una definición de carga versionada en workloads/.
type Workload struct {
	Name              string `json:"name"`
	Description       string `json:"description"`
	Topic             string `json:"topic"`
	Partitions        int32  `json:"partitions"`
	ReplicationFactor int32  `json:"replication_factor"`
	Acks              string `json:"acks"`
	Concurrency       int    `json:"concurrency"`
	DurationS         int    `json:"duration_s"`
	MaxWrites         int    `json:"max_writes"` // 0: sin límite, solo duración
	TimeoutMs         int32  `json:"timeout_ms"`
	ProbeIntervalMs   int    `json:"probe_interval_ms"` // 0: sin lector probe
	ProbeWindow       int64  `json:"probe_window"`      // offsets ya leídos que cada pasada vuelve a leer
}

// LoadWorkload lee una definición y rechaza campos desconocidos.
func LoadWorkload(path string) (Workload, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Workload{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var w Workload
	if err := dec.Decode(&w); err != nil {
		return Workload{}, fmt.Errorf("%s: %w", path, err)
	}
	return w, nil
}

// Validate comprueba que la carga se puede ejecutar.
func (w Workload) Validate() error {
	switch {
	case w.Topic == "":
		return fmt.Errorf("falta topic")
	case w.Partitions < 1 || w.ReplicationFactor < 1:
		return fmt.Errorf("partitions y replication_factor deben ser positivos")
	case w.Concurrency < 1:
		return fmt.Errorf("concurrency debe ser positiva")
	case w.DurationS <= 0 && w.MaxWrites <= 0:
		return fmt.Errorf("hace falta duration_s o max_writes")
	case w.TimeoutMs <= 0:
		return fmt.Errorf("timeout_ms debe ser positivo")
	case w.ProbeIntervalMs < 0 || (w.ProbeIntervalMs > 0 && w.ProbeWindow < 1):
		return fmt.Errorf("probe_interval_ms necesita probe_window positivo")
	}
	if _, err := acksProto(w.Acks); err != nil {
		return err
	}
	return nil
}

func acksProto(acks string) (pb.Acks, error) {
	switch acks {
	case history.AcksNone:
		return pb.Acks_ACKS_NONE, nil
	case history.AcksLeader:
		return pb.Acks_ACKS_LEADER, nil
	case history.AcksAll:
		return pb.Acks_ACKS_ALL, nil
	}
	return 0, fmt.Errorf("acks desconocido %q (none, leader, all)", acks)
}
