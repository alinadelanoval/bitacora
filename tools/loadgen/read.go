package main

import (
	"context"
	"fmt"
	"time"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
	"github.com/alinadelanoval/bitacora/pkg/client"
	"github.com/alinadelanoval/bitacora/tools/history"
)

// readRange lee [from, to) de una partición y devuelve sus registros como
// entradas read del lector dado, sin escribirlas todavía.
func readRange(ctx context.Context, c *client.Client, topic string, part int32, reader string, from, to int64,
) ([]history.Entry, error) {
	var out []history.Entry
	for off := from; off < to; {
		recs, _, err := c.Fetch(ctx, topic, part, off, fetchBytes, 0)
		if err != nil {
			return nil, err
		}
		if len(recs) == 0 {
			return nil, fmt.Errorf("Fetch en %d sin registros por debajo del HW %d", off, to)
		}
		for _, r := range recs {
			if r.Offset >= to {
				break
			}
			if r.Offset != off {
				// Un hueco o un desorden se registra tal cual: lo juzga el Checker.
				off = r.Offset
			}
			out = append(out, history.Entry{Op: history.OpRead, TS: now(), Topic: topic, Partition: part,
				Reader: reader, Offset: history.Int64(r.Offset), Value: string(r.Value)})
			off++
		}
	}
	return out, nil
}

// probe lee cada probe_interval_ms lo escrito desde su pasada anterior y
// vuelve a leer los últimos probe_window offsets que ya había leído. El
// solape no depende del ritmo de escritura: los registros cercanos a un fallo
// se leen antes y después de él, que es lo que permite evaluar I-01. Los
// errores (por ejemplo NOT_LEADER durante un cambio de líder) se cuentan y la
// pasada se repite en el siguiente intervalo desde el mismo punto.
func probe(ctx context.Context, c *client.Client, w Workload, out *history.Writer) (passes, errs int) {
	tick := time.NewTicker(time.Duration(w.ProbeIntervalMs) * time.Millisecond)
	defer tick.Stop()
	prev := make([]int64, w.Partitions) // HW leído en la última pasada correcta
	for {
		select {
		case <-ctx.Done():
			return passes, errs
		case <-tick.C:
		}
		for p := range w.Partitions {
			hw, err := c.ListOffsets(ctx, w.Topic, p, pb.OffsetPosition_OFFSET_POSITION_LATEST, 0)
			if err == nil {
				var es []history.Entry
				if es, err = readRange(ctx, c, w.Topic, p, probeReader, max(0, prev[p]-w.ProbeWindow), hw); err == nil {
					for _, e := range es {
						if err = out.Write(e); err != nil {
							break
						}
					}
				}
			}
			if err != nil {
				if ctx.Err() != nil {
					return passes, errs
				}
				errs++
				continue
			}
			prev[p] = hw
			passes++
		}
	}
}

// finalRead lee la partición desde el inicio hasta el HW y, solo si la lectura
// se completa, escribe sus registros y final_read_done. Ante un error vuelve
// a empezar: una lectura a medias repetida duplicaría offsets en el historial.
func finalRead(ctx context.Context, c *client.Client, topic string, part int32, timeout time.Duration,
	out *history.Writer) FinalRead {
	fr := FinalRead{Partition: part}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		start, hw, es, err := readAll(ctx, c, topic, part)
		if err == nil {
			for _, e := range es {
				if err = out.Write(e); err != nil {
					break
				}
			}
			if err == nil {
				err = out.Write(history.Entry{Op: history.OpFinalReadDone, TS: now(), Topic: topic, Partition: part,
					StartOffset: history.Int64(start), HighWatermark: history.Int64(hw)})
			}
			if err != nil {
				fr.Error = "historial: " + err.Error()
				return fr
			}
			fr.Complete, fr.StartOffset, fr.HighWatermark, fr.Records = true, start, hw, len(es)
			return fr
		}
		fr.Error = err.Error()
		select {
		case <-ctx.Done():
			return fr
		case <-time.After(backoff):
		}
	}
}

func readAll(ctx context.Context, c *client.Client, topic string, part int32) (int64, int64, []history.Entry, error) {
	start, err := c.ListOffsets(ctx, topic, part, pb.OffsetPosition_OFFSET_POSITION_EARLIEST, 0)
	if err != nil {
		return 0, 0, nil, err
	}
	hw, err := c.ListOffsets(ctx, topic, part, pb.OffsetPosition_OFFSET_POSITION_LATEST, 0)
	if err != nil {
		return 0, 0, nil, err
	}
	if start < 0 {
		start = hw // partición vacía
	}
	es, err := readRange(ctx, c, topic, part, history.FinalReader, start, hw)
	return start, hw, es, err
}
