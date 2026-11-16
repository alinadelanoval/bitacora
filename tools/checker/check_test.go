package main

import (
	"testing"

	"github.com/alinadelanoval/bitacora/tools/history"
)

// hist construye historiales de una partición con números de línea.
type hist struct{ es []history.Entry }

func (h *hist) add(e history.Entry) *hist {
	e.Topic, e.Line = "w5", len(h.es)+1
	h.es = append(h.es, e)
	return h
}

func (h *hist) produce(id string, off int64, acks, result string) *hist {
	return h.add(history.Entry{Op: history.OpProduce, ID: id, Acks: acks, Result: result, Offset: history.Int64(off)})
}

func (h *hist) read(reader string, off int64, value string) *hist {
	return h.add(history.Entry{Op: history.OpRead, Reader: reader, Offset: history.Int64(off), Value: value})
}

func (h *hist) done(start, hw int64) *hist {
	return h.add(history.Entry{Op: history.OpFinalReadDone, StartOffset: history.Int64(start), HighWatermark: history.Int64(hw)})
}

// final añade la lectura final completa de los valores dados desde el offset 0.
func (h *hist) final(values ...string) *hist {
	for i, v := range values {
		h.read(history.FinalReader, int64(i), v)
	}
	return h.done(0, int64(len(values)))
}

func prop(t *testing.T, h *hist, id string) Property {
	t.Helper()
	for _, p := range Check("mem", h.es).Properties {
		if p.ID == id {
			return p
		}
	}
	t.Fatalf("falta %s en el informe", id)
	return Property{}
}

func expect(t *testing.T, p Property, status string, kinds ...string) {
	t.Helper()
	if p.Status != status {
		t.Fatalf("%s: estado %s, esperado %s (%s; %+v)", p.ID, p.Status, status, p.Detail, p.Anomalies)
	}
	if len(p.Anomalies) != len(kinds) {
		t.Fatalf("%s: %d anomalías, esperadas %v: %+v", p.ID, len(p.Anomalies), kinds, p.Anomalies)
	}
	for i, k := range kinds {
		if p.Anomalies[i].Kind != k {
			t.Fatalf("%s: anomalía %d = %s, esperada %s", p.ID, i, p.Anomalies[i].Kind, k)
		}
	}
}

const (
	all, leader, none = history.AcksAll, history.AcksLeader, history.AcksNone
	ok, fail, indet   = history.ResultOK, history.ResultFail, history.ResultIndeterminate
)

func clean() *hist {
	h := &hist{}
	h.produce("a", 0, all, ok).produce("b", 1, all, ok).produce("c", -1, all, indet).produce("d", -1, all, fail)
	h.read("probe", 0, "a").read("probe", 1, "b").read("probe", 0, "a").read("probe", 1, "b")
	return h.final("a", "b", "c")
}

func TestCheck_CleanHistory_AllVerified(t *testing.T) {
	rep := Check("mem", clean().es)
	for _, p := range rep.Properties {
		expect(t, p, Verified)
	}
	if rep.Violated() {
		t.Fatal("Violated() con todas verificadas")
	}
}

func TestCheck_NoFinalRead_FinalReadPropertiesNotEvaluable(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok)
	for _, id := range []string{"I-01", "I-02", "I-03", "I-08"} {
		expect(t, prop(t, h, id), NotEvaluable)
	}
	// I-04 no necesita la lectura final: compara las confirmaciones.
	expect(t, prop(t, h, "I-04"), Verified)
}

func TestCheckI01_RereadChangesValue_LogMutation(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok).read("probe", 0, "a").read("probe", 0, "x").final("a")
	p := prop(t, h, "I-01")
	expect(t, p, Violated, "log_mutation")
	if l := p.Anomalies[0].Lines; len(l) != 2 || l[0] != 2 || l[1] != 3 {
		t.Fatalf("líneas = %v, esperadas [2 3]", l)
	}
}

func TestCheckI01_OnlyOneReadPerReader_NotEvaluable(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok).read("probe", 0, "a").final("a")
	expect(t, prop(t, h, "I-01"), NotEvaluable)
}

func TestCheckI02_MissingOffset_OffsetGap(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok).read(history.FinalReader, 0, "a").done(0, 2)
	expect(t, prop(t, h, "I-02"), Violated, "offset_gap")
}

func TestCheckI02_RepeatedOffset_OffsetDuplicate(t *testing.T) {
	h := (&hist{}).read(history.FinalReader, 0, "a").read(history.FinalReader, 0, "a").done(0, 1)
	expect(t, prop(t, h, "I-02"), Violated, "offset_duplicate")
}

func TestCheckI02_ReadersDisagree_ValueDivergence(t *testing.T) {
	h := (&hist{}).read("probe", 0, "x").final("a")
	expect(t, prop(t, h, "I-02"), Violated, "value_divergence")
}

func TestCheckI02_RecordAtOtherOffset_OffsetMismatch(t *testing.T) {
	h := (&hist{}).produce("a", 1, leader, ok).final("a", "b")
	expect(t, prop(t, h, "I-02"), Violated, "offset_mismatch")
}

func TestCheckI03_AcksAllMissing_LostWrite(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok).produce("b", 1, all, ok).final("a")
	p := prop(t, h, "I-03")
	expect(t, p, Violated, "lost_write")
	if p.Anomalies[0].IDs[0] != "b" {
		t.Fatalf("id = %v", p.Anomalies[0].IDs)
	}
}

func TestCheckI03_AcksAllOverwritten_LostWrite(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok).final("z")
	expect(t, prop(t, h, "I-03"), Violated, "lost_write")
}

func TestCheckI03_OnlyAcksLeaderLost_NotEvaluable(t *testing.T) {
	// ACKS_LEADER puede perderse si el líder cae antes de replicar: no obliga.
	h := (&hist{}).produce("a", 0, leader, ok).final()
	expect(t, prop(t, h, "I-03"), NotEvaluable)
}

func TestCheckI03_IndeterminateAbsent_Verified(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok).produce("b", -1, all, indet).final("a")
	expect(t, prop(t, h, "I-03"), Verified)
}

func TestCheckI04_TwoAcksAllSameOffset_DivergentAck(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok).produce("b", 0, all, ok)
	p := prop(t, h, "I-04")
	expect(t, p, Violated, "divergent_ack")
	if len(p.Anomalies[0].IDs) != 2 {
		t.Fatalf("ids = %v", p.Anomalies[0].IDs)
	}
}

func TestCheckI04_OnlyAcksNone_NotEvaluable(t *testing.T) {
	h := (&hist{}).produce("a", -1, none, ok).final("a")
	expect(t, prop(t, h, "I-04"), NotEvaluable)
}

func TestCheckI08_IdTwice_Duplicate(t *testing.T) {
	h := (&hist{}).produce("a", -1, all, indet).final("a", "a")
	p := prop(t, h, "I-08")
	expect(t, p, Violated, "duplicate")
	if l := p.Anomalies[0].Lines; len(l) != 2 {
		t.Fatalf("líneas = %v", l)
	}
}

func TestCheckI08_IndeterminateOnce_Verified(t *testing.T) {
	h := (&hist{}).produce("a", -1, all, indet).final("a")
	expect(t, prop(t, h, "I-08"), Verified)
}

func TestCheck_MultiplePartitions_EvaluatedSeparately(t *testing.T) {
	h := &hist{}
	h.produce("a", 0, all, ok).final("a")
	// Mismo offset en otra partición: no es divergent_ack ni offset_duplicate.
	h.es = append(h.es,
		history.Entry{Op: history.OpProduce, Topic: "w5", Partition: 1, ID: "b", Acks: all, Result: ok, Offset: history.Int64(0), Line: 10},
		history.Entry{Op: history.OpRead, Topic: "w5", Partition: 1, Reader: history.FinalReader, Offset: history.Int64(0), Value: "b", Line: 11},
		history.Entry{Op: history.OpFinalReadDone, Topic: "w5", Partition: 1, StartOffset: history.Int64(0), HighWatermark: history.Int64(1), Line: 12})
	for _, id := range []string{"I-02", "I-03", "I-04", "I-08"} {
		expect(t, prop(t, h, id), Verified)
	}
}

func TestCheckI08_FailedWriteInLog_RejectedWritePresent(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok).produce("b", -1, all, fail).final("a", "b")
	p := prop(t, h, "I-08")
	expect(t, p, Violated, "rejected_write_present")
	if a := p.Anomalies[0]; a.IDs[0] != "b" || *a.Offset != 1 || len(a.Lines) != 2 || a.Lines[0] != 2 {
		t.Fatalf("anomalía = %+v", a)
	}
}

func TestCheckI08_FailedWriteAbsent_Verified(t *testing.T) {
	h := (&hist{}).produce("a", 0, all, ok).produce("b", -1, all, fail).final("a")
	expect(t, prop(t, h, "I-08"), Verified)
}
