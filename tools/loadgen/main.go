// Command loadgen ejecuta una carga de workloads/ contra un endpoint del
// contrato y escribe el historial que verifica tools/checker.
//
//	loadgen -endpoint 127.0.0.1:50051 -workload tools/loadgen/workloads/w5.json \
//	        -history history.ndjson [-summary summary.json] [-acks leader] [-duration 60]
//
// Sale con 0 si la lectura final se completa, con 1 si no (el historial queda
// sin final_read_done y el Checker no puede evaluar I-02, I-03 ni I-08) y con
// 2 si la configuración o el arranque fallan.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alinadelanoval/bitacora/pkg/client"
	"github.com/alinadelanoval/bitacora/tools/history"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	endpoint := fs.String("endpoint", "", "host:puerto del endpoint")
	wlPath := fs.String("workload", "", "definición de carga (JSON)")
	histPath := fs.String("history", "", "historial NDJSON de salida")
	sumPath := fs.String("summary", "", "resumen JSON de salida (opcional)")
	acks := fs.String("acks", "", "sustituye acks de la carga: none, leader o all")
	duration := fs.Int("duration", 0, "sustituye duration_s de la carga")
	maxWrites := fs.Int("max-writes", -1, "sustituye max_writes de la carga")
	concurrency := fs.Int("concurrency", 0, "sustituye concurrency de la carga")
	topic := fs.String("topic", "", "sustituye el tema de la carga")
	noCreate := fs.Bool("no-create", false, "no crear el tema (ya existe)")
	finalDelay := fs.Duration("final-delay", 0, "espera antes de la lectura final")
	finalTimeout := fs.Duration("final-timeout", time.Minute, "plazo de la lectura final")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	fail := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "loadgen: "+format+"\n", a...)
		return 2
	}
	if *endpoint == "" || *wlPath == "" || *histPath == "" {
		return fail("hacen falta -endpoint, -workload y -history")
	}
	w, err := LoadWorkload(*wlPath)
	if err != nil {
		return fail("%v", err)
	}
	if *acks != "" {
		w.Acks = *acks
	}
	if *duration > 0 {
		w.DurationS = *duration
	}
	if *maxWrites >= 0 {
		w.MaxWrites = *maxWrites
	}
	if *concurrency > 0 {
		w.Concurrency = *concurrency
	}
	if *topic != "" {
		w.Topic = *topic
	}
	if err := w.Validate(); err != nil {
		return fail("carga %s: %v", *wlPath, err)
	}

	f, err := os.Create(*histPath)
	if err != nil {
		return fail("%v", err)
	}
	defer f.Close()
	c, err := client.Dial(*endpoint)
	if err != nil {
		return fail("%v", err)
	}
	defer c.Close()

	sum, err := Run(ctx, c, *endpoint, w, Options{CreateTopic: !*noCreate, FinalDelay: *finalDelay,
		FinalTimeout: *finalTimeout}, history.NewWriter(f))
	if err != nil && sum == nil {
		return fail("%v", err)
	}
	sum.Commit = commit()
	if err := f.Sync(); err != nil {
		return fail("historial: %v", err)
	}
	if *sumPath != "" {
		b, _ := json.MarshalIndent(sum, "", "  ")
		if err := os.WriteFile(*sumPath, append(b, '\n'), 0o644); err != nil {
			return fail("%v", err)
		}
	}
	writeText(stdout, sum)
	if err != nil {
		fmt.Fprintf(stderr, "loadgen: %v\n", err)
	}
	if !sum.Complete() {
		return 1
	}
	return 0
}

// commit identifica la versión del repositorio, con «-dirty» si hay cambios
// sin confirmar; vacío fuera de un repositorio.
func commit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	c := strings.TrimSpace(string(out))
	if st, err := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output(); err == nil && len(st) > 0 {
		c += "-dirty"
	}
	return c
}

func writeText(w io.Writer, s *Summary) {
	fmt.Fprintf(w, "carga %s contra %s (acks=%s, %d escritores, commit %s)\n", s.Workload.Name, s.Endpoint,
		s.Workload.Acks, s.Workload.Concurrency, s.Commit)
	fmt.Fprintf(w, "escrituras: ok=%d fail=%d indeterminate=%d en %.1f s; %.1f ok/s\n",
		s.Writes[history.ResultOK], s.Writes[history.ResultFail], s.Writes[history.ResultIndeterminate],
		s.DurationS, s.ThroughputS)
	l := s.LatencyOK
	fmt.Fprintf(w, "latencia ok (ms): p50=%.2f p95=%.2f p99=%.2f max=%.2f\n", l.P50, l.P95, l.P99, l.Max)
	if len(s.Errors) > 0 {
		b, _ := json.Marshal(s.Errors)
		fmt.Fprintf(w, "errores: %s\n", b)
	}
	fmt.Fprintf(w, "probe: %d pasadas, %d errores\n", s.ProbePasses, s.ProbeErrors)
	for _, f := range s.FinalRead {
		if f.Complete {
			fmt.Fprintf(w, "lectura final p%d: %d registros en [%d, %d)\n", f.Partition, f.Records, f.StartOffset, f.HighWatermark)
		} else {
			fmt.Fprintf(w, "lectura final p%d: INCOMPLETA (%s)\n", f.Partition, f.Error)
		}
	}
}
