package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/alinadelanoval/bitacora/tools/history"
)

// Estados de una propiedad.
const (
	Verified     = "verified"
	Violated     = "violated"
	NotEvaluable = "not_evaluable"
)

// Anomaly es una violación concreta con los registros implicados y las líneas
// del historial donde se observa.
type Anomaly struct {
	Kind      string   `json:"kind"`
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Offset    *int64   `json:"offset,omitempty"`
	IDs       []string `json:"ids,omitempty"`
	Lines     []int    `json:"lines"`
	Detail    string   `json:"detail"`
}

// Property es el estado de una invariante con su justificación.
type Property struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	Detail    string    `json:"detail"`
	Anomalies []Anomaly `json:"anomalies"`
}

// Report es el informe completo; no hay resultado global sin el desglose.
type Report struct {
	History    string     `json:"history"`
	Entries    int        `json:"entries"`
	Properties []Property `json:"properties"`
}

// Violated indica si alguna propiedad evaluada está violada.
func (r *Report) Violated() bool {
	for _, p := range r.Properties {
		if p.Status == Violated {
			return true
		}
	}
	return false
}

type tp struct {
	topic     string
	partition int32
}

type obs struct {
	reader string
	value  string
	line   int
}

// state agrupa el historial por partición.
type state struct {
	produces []history.Entry
	reads    map[tp]map[int64][]obs // todas las lecturas, por offset
	final    map[tp]map[int64][]obs // solo la lectura final
	done     map[tp]history.Entry   // final_read_done
}

func index(entries []history.Entry) *state {
	s := &state{reads: map[tp]map[int64][]obs{}, final: map[tp]map[int64][]obs{}, done: map[tp]history.Entry{}}
	add := func(m map[tp]map[int64][]obs, k tp, off int64, o obs) {
		if m[k] == nil {
			m[k] = map[int64][]obs{}
		}
		m[k][off] = append(m[k][off], o)
	}
	for _, e := range entries {
		k := tp{e.Topic, e.Partition}
		switch e.Op {
		case history.OpProduce:
			s.produces = append(s.produces, e)
		case history.OpRead:
			o := obs{reader: e.Reader, value: e.Value, line: e.Line}
			add(s.reads, k, *e.Offset, o)
			if e.Reader == history.FinalReader {
				add(s.final, k, *e.Offset, o)
			}
		case history.OpFinalReadDone:
			s.done[k] = e
		}
	}
	return s
}

// loc es un offset donde la lectura final encontró un id.
type loc struct {
	off  int64
	line int
}

// locations indexa la lectura final de una partición por id.
func (s *state) locations(k tp) map[string][]loc {
	at := map[string][]loc{}
	for _, off := range sortedOffsets(s.final[k]) {
		for _, o := range s.final[k][off] {
			at[o.value] = append(at[o.value], loc{off, o.line})
		}
	}
	return at
}

// finalComplete indica si la partición tiene una lectura final terminada.
func (s *state) finalComplete(k tp) bool { _, ok := s.done[k]; return ok }

// durable indica si la escritura genera la obligación de I-03: confirmada con
// ACKS_ALL. ACKS_LEADER puede perderse si el líder cae antes de replicar, y
// una escritura indeterminada no obliga (docs/contract.md).
func durable(e history.Entry) bool { return e.Result == history.ResultOK && e.Acks == history.AcksAll }

// Check evalúa I-01, I-02, I-03, I-04 e I-08 sobre el historial.
func Check(path string, entries []history.Entry) *Report {
	s := index(entries)
	return &Report{History: path, Entries: len(entries), Properties: []Property{
		checkI01(s), checkI02(s), checkI03(s), checkI04(s), checkI08(s),
	}}
}

func status(anomalies []Anomaly) string {
	if len(anomalies) > 0 {
		return Violated
	}
	return Verified
}

func sortAnomalies(a []Anomaly) {
	sort.SliceStable(a, func(i, j int) bool {
		if a[i].Topic != a[j].Topic {
			return a[i].Topic < a[j].Topic
		}
		if a[i].Partition != a[j].Partition {
			return a[i].Partition < a[j].Partition
		}
		oi, oj := int64(-1), int64(-1)
		if a[i].Offset != nil {
			oi = *a[i].Offset
		}
		if a[j].Offset != nil {
			oj = *a[j].Offset
		}
		if oi != oj {
			return oi < oj
		}
		return a[i].Kind < a[j].Kind
	})
}

func sortedKeys[V any](m map[tp]V) []tp {
	keys := make([]tp, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].topic != keys[j].topic {
			return keys[i].topic < keys[j].topic
		}
		return keys[i].partition < keys[j].partition
	})
	return keys
}

func sortedOffsets(m map[int64][]obs) []int64 {
	offs := make([]int64, 0, len(m))
	for o := range m {
		offs = append(offs, o)
	}
	sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
	return offs
}

func distinctValues(os []obs) []string {
	seen := map[string]bool{}
	var vals []string
	for _, o := range os {
		if !seen[o.value] {
			seen[o.value] = true
			vals = append(vals, o.value)
		}
	}
	return vals
}

func lines(os []obs) []int {
	ls := make([]int, len(os))
	for i, o := range os {
		ls[i] = o.line
	}
	return ls
}

// checkI01: un mismo lector que relee un offset ve siempre el mismo valor.
func checkI01(s *state) Property {
	p := Property{ID: "I-01", Name: "Append-only", Anomalies: []Anomaly{}}
	reread := 0
	for _, k := range sortedKeys(s.reads) {
		for _, off := range sortedOffsets(s.reads[k]) {
			byReader := map[string][]obs{}
			for _, o := range s.reads[k][off] {
				byReader[o.reader] = append(byReader[o.reader], o)
			}
			readers := make([]string, 0, len(byReader))
			for r := range byReader {
				readers = append(readers, r)
			}
			sort.Strings(readers)
			for _, r := range readers {
				os := byReader[r]
				if len(os) < 2 {
					continue
				}
				reread++
				if vals := distinctValues(os); len(vals) > 1 {
					p.Anomalies = append(p.Anomalies, Anomaly{Kind: "log_mutation", Topic: k.topic,
						Partition: k.partition, Offset: history.Int64(off), IDs: vals, Lines: lines(os),
						Detail: fmt.Sprintf("el lector %s leyó valores distintos en el mismo offset", r)})
				}
			}
		}
	}
	if reread == 0 {
		p.Status, p.Detail = NotEvaluable, "ningún lector relee un offset; lo cubren las pruebas del almacenamiento y de la réplica"
		return p
	}
	sortAnomalies(p.Anomalies)
	p.Status = status(p.Anomalies)
	p.Detail = fmt.Sprintf("%d offsets releídos por el mismo lector", reread)
	return p
}

// checkI02: offsets únicos y contiguos en la lectura final, el mismo valor
// para todos los lectores y el offset de Produce igual al offset leído.
func checkI02(s *state) Property {
	p := Property{ID: "I-02", Name: "Orden por partición", Anomalies: []Anomaly{}}
	if len(s.done) == 0 {
		p.Status, p.Detail = NotEvaluable, "no hay lectura final completa"
		return p
	}
	for _, k := range sortedKeys(s.done) {
		d := s.done[k]
		fin := s.final[k]
		for off := *d.StartOffset; off < *d.HighWatermark; off++ {
			switch os := fin[off]; {
			case len(os) == 0:
				p.Anomalies = append(p.Anomalies, Anomaly{Kind: "offset_gap", Topic: k.topic, Partition: k.partition,
					Offset: history.Int64(off), Lines: []int{d.Line},
					Detail: "offset ausente en la lectura final entre el inicio y el HW"})
			case len(os) > 1:
				p.Anomalies = append(p.Anomalies, Anomaly{Kind: "offset_duplicate", Topic: k.topic,
					Partition: k.partition, Offset: history.Int64(off), IDs: distinctValues(os), Lines: lines(os),
					Detail: "offset repetido en la lectura final"})
			}
		}
	}
	for _, k := range sortedKeys(s.reads) {
		for _, off := range sortedOffsets(s.reads[k]) {
			os := s.reads[k][off]
			readers := map[string]bool{}
			for _, o := range os {
				readers[o.reader] = true
			}
			if len(readers) > 1 && len(distinctValues(os)) > 1 {
				p.Anomalies = append(p.Anomalies, Anomaly{Kind: "value_divergence", Topic: k.topic,
					Partition: k.partition, Offset: history.Int64(off), IDs: distinctValues(os), Lines: lines(os),
					Detail: "lectores distintos ven valores distintos en el mismo offset"})
			}
		}
	}
	where := map[tp]map[string][]loc{}
	for k := range s.done {
		where[k] = s.locations(k)
	}
	for _, e := range s.produces {
		k := tp{e.Topic, e.Partition}
		if e.Result != history.ResultOK || e.Offset == nil || *e.Offset < 0 || !s.finalComplete(k) {
			continue
		}
		for _, at := range where[k][e.ID] {
			if at.off != *e.Offset {
				p.Anomalies = append(p.Anomalies, Anomaly{Kind: "offset_mismatch", Topic: k.topic,
					Partition: k.partition, Offset: history.Int64(*e.Offset), IDs: []string{e.ID},
					Lines:  []int{e.Line, at.line},
					Detail: fmt.Sprintf("Produce devolvió el offset %d y el registro se lee en el %d", *e.Offset, at.off)})
			}
		}
	}
	sortAnomalies(p.Anomalies)
	p.Status = status(p.Anomalies)
	p.Detail = fmt.Sprintf("particiones con lectura final completa: %d", len(s.done))
	return p
}

// checkI03: cada escritura ok con ACKS_ALL aparece en la lectura final con su
// offset y su valor.
func checkI03(s *state) Property {
	p := Property{ID: "I-03", Name: "Durabilidad declarada", Anomalies: []Anomaly{}}
	obliged, unchecked := 0, 0
	for _, e := range s.produces {
		if !durable(e) {
			continue
		}
		k := tp{e.Topic, e.Partition}
		if !s.finalComplete(k) {
			unchecked++
			continue
		}
		obliged++
		os := s.final[k][*e.Offset]
		found := false
		for _, o := range os {
			if o.value == e.ID {
				found = true
			}
		}
		if !found {
			detail := "ausente en la lectura final"
			if len(os) > 0 {
				detail = fmt.Sprintf("su offset contiene otro valor (%s)", strings.Join(distinctValues(os), ", "))
			}
			p.Anomalies = append(p.Anomalies, Anomaly{Kind: "lost_write", Topic: k.topic, Partition: k.partition,
				Offset: history.Int64(*e.Offset), IDs: []string{e.ID}, Lines: append([]int{e.Line}, lines(os)...),
				Detail: "escritura confirmada con acks=all " + detail})
		}
	}
	switch {
	case obliged == 0 && unchecked == 0:
		p.Status, p.Detail = NotEvaluable, "ninguna escritura confirmada con acks=all; acks=leader puede perderse y una escritura indeterminada no obliga"
		return p
	case obliged == 0:
		p.Status, p.Detail = NotEvaluable, fmt.Sprintf("%d escrituras con acks=all sin lectura final completa de su partición", unchecked)
		return p
	}
	sortAnomalies(p.Anomalies)
	p.Status = status(p.Anomalies)
	p.Detail = fmt.Sprintf("%d escrituras confirmadas con acks=all comprobadas", obliged)
	if unchecked > 0 {
		p.Detail += fmt.Sprintf("; %d sin lectura final de su partición", unchecked)
	}
	return p
}

// checkI04: dos escrituras reconocidas con ACKS_ALL no comparten offset.
func checkI04(s *state) Property {
	p := Property{ID: "I-04", Name: "Historia única bajo fallo y partición", Anomalies: []Anomaly{}}
	type at struct {
		k   tp
		off int64
	}
	acked := map[at][]history.Entry{}
	n := 0
	for _, e := range s.produces {
		if durable(e) {
			n++
			a := at{tp{e.Topic, e.Partition}, *e.Offset}
			acked[a] = append(acked[a], e)
		}
	}
	if n == 0 {
		p.Status, p.Detail = NotEvaluable, "ninguna escritura confirmada con acks=all"
		return p
	}
	for a, es := range acked {
		ids, ls, seen := []string{}, []int{}, map[string]bool{}
		for _, e := range es {
			ls = append(ls, e.Line)
			if !seen[e.ID] {
				seen[e.ID] = true
				ids = append(ids, e.ID)
			}
		}
		if len(ids) > 1 {
			p.Anomalies = append(p.Anomalies, Anomaly{Kind: "divergent_ack", Topic: a.k.topic, Partition: a.k.partition,
				Offset: history.Int64(a.off), IDs: ids, Lines: ls,
				Detail: "dos escrituras distintas confirmadas con acks=all en el mismo offset"})
		}
	}
	sortAnomalies(p.Anomalies)
	p.Status = status(p.Anomalies)
	p.Detail = fmt.Sprintf("%d escrituras confirmadas con acks=all", n)
	return p
}

// checkI08: ningún id aparece más de una vez en la lectura final, y ninguna
// escritura rechazada (fail) aparece en ella. El generador mínimo envía cada
// id una sola vez y no reintenta, así que una repetición es un segundo efecto
// producido por el sistema. Una escritura rechazada que sí tuvo efecto engaña
// al cliente: si la reintenta, la duplica (docs/contract.md, «Resultado
// indeterminado»).
func checkI08(s *state) Property {
	p := Property{ID: "I-08", Name: "Reintento seguro", Anomalies: []Anomaly{}}
	if len(s.done) == 0 {
		p.Status, p.Detail = NotEvaluable, "no hay lectura final completa"
		return p
	}
	sent := map[string]bool{}
	for _, e := range s.produces {
		sent[e.ID] = true
	}
	for _, e := range s.produces {
		k := tp{e.Topic, e.Partition}
		if e.Result != history.ResultFail || !s.finalComplete(k) {
			continue
		}
		if at := s.locations(k)[e.ID]; len(at) > 0 {
			ls := []int{e.Line}
			for _, l := range at {
				ls = append(ls, l.line)
			}
			p.Anomalies = append(p.Anomalies, Anomaly{Kind: "rejected_write_present", Topic: k.topic,
				Partition: k.partition, Offset: history.Int64(at[0].off), IDs: []string{e.ID}, Lines: ls,
				Detail: fmt.Sprintf("Produce respondió %s y el registro está en el log", e.Error)})
		}
	}
	for _, k := range sortedKeys(s.done) {
		at := s.locations(k)
		ids := make([]string, 0, len(at))
		for id := range at {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			offs := map[int64]bool{}
			var where []string
			var ls []int
			for _, l := range at[id] {
				if !offs[l.off] {
					offs[l.off] = true
					where = append(where, fmt.Sprint(l.off))
				}
				ls = append(ls, l.line)
			}
			if sent[id] && len(offs) > 1 {
				p.Anomalies = append(p.Anomalies, Anomaly{Kind: "duplicate", Topic: k.topic, Partition: k.partition,
					IDs: []string{id}, Lines: ls,
					Detail: "el mismo id aparece en los offsets " + strings.Join(where, ", ")})
			}
		}
	}
	sortAnomalies(p.Anomalies)
	p.Status = status(p.Anomalies)
	p.Detail = fmt.Sprintf("particiones con lectura final completa: %d; cada id se envía una sola vez", len(s.done))
	return p
}
