// Command checker verifica las invariantes I-01, I-02, I-03, I-04 e I-08 sobre
// el historial de una ejecución (tools/checker/README.md).
//
//	checker -history history.ndjson [-json checker.json]
//
// Imprime el informe en texto. Sale con 1 si alguna propiedad está violada y
// con 2 si el historial no se puede leer.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/alinadelanoval/bitacora/tools/history"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("checker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("history", "", "historial NDJSON")
	jsonOut := fs.String("json", "", "ruta del informe JSON (opcional)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *path == "" {
		fmt.Fprintln(stderr, "checker: falta -history")
		return 2
	}
	entries, err := history.ReadFile(*path)
	if err != nil {
		fmt.Fprintf(stderr, "checker: %s: %v\n", *path, err)
		return 2
	}
	rep := Check(*path, entries)
	if *jsonOut != "" {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err == nil {
			err = os.WriteFile(*jsonOut, append(b, '\n'), 0o644)
		}
		if err != nil {
			fmt.Fprintf(stderr, "checker: %v\n", err)
			return 2
		}
	}
	writeText(stdout, rep)
	if rep.Violated() {
		return 1
	}
	return 0
}

// writeText escribe una línea por propiedad y, debajo, sus anomalías.
func writeText(w io.Writer, rep *Report) {
	fmt.Fprintf(w, "historial: %s (%d entradas)\n", rep.History, rep.Entries)
	for _, p := range rep.Properties {
		fmt.Fprintf(w, "%-5s %-38s %-13s %s\n", p.ID, p.Name, p.Status, p.Detail)
		for _, a := range p.Anomalies {
			where := fmt.Sprintf("%s/%d", a.Topic, a.Partition)
			if a.Offset != nil {
				where += fmt.Sprintf("@%d", *a.Offset)
			}
			ls := make([]string, len(a.Lines))
			for i, l := range a.Lines {
				ls[i] = fmt.Sprint(l)
			}
			fmt.Fprintf(w, "      - %s %s [%s]: %s (líneas %s)\n", a.Kind, where, strings.Join(a.IDs, ", "),
				a.Detail, strings.Join(ls, ", "))
		}
	}
}
