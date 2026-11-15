package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRun_CleanHistory_ExitsZeroAndWritesJSON(t *testing.T) {
	out := filepath.Join(t.TempDir(), "checker.json")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-history", "testdata/clean.ndjson", "-json", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("código %d; stdout:\n%s\nstderr:\n%s", code, &stdout, &stderr)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var rep Report
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Properties) != 5 || rep.Entries != 6 {
		t.Fatalf("informe: %+v", rep)
	}
	for _, p := range rep.Properties {
		if p.Status != Verified {
			t.Errorf("%s = %s (%s)", p.ID, p.Status, p.Detail)
		}
	}
}

func TestRun_LostWrite_ExitsOneAndNamesAnomaly(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-history", "testdata/lost_write.ndjson"}, &stdout, &stderr); code != 1 {
		t.Fatalf("código %d, esperado 1; stderr: %s", code, &stderr)
	}
	for _, want := range []string{"I-03", "violated", "lost_write", "w-000002", "líneas 2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("falta %q en:\n%s", want, &stdout)
		}
	}
}

func TestRun_BadInput_ExitsTwo(t *testing.T) {
	for _, args := range [][]string{{}, {"-history", "testdata/malformed.ndjson"}, {"-history", "testdata/no-existe.ndjson"}} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Errorf("args %v: código %d, esperado 2", args, code)
		}
	}
}
