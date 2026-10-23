package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	nodeID := flag.String("node-id", "", "local scaffold process identifier")
	runID := flag.String("run-id", "", "unique identifier for the local scaffold run")
	placeholder := flag.Bool("dev-placeholder", false, "run the BIT-01 process placeholder")
	flag.Parse()

	if !*placeholder || *nodeID == "" || *runID == "" {
		slog.Error("this scaffold executable only supports --dev-placeholder with --node-id and --run-id")
		os.Exit(2)
	}

	slog.Warn("BIT-01 placeholder process started; no Bitacora API is available", "node_id", *nodeID, "run_id", *runID)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
	signal.Stop(signals)
	slog.Info("BIT-01 placeholder process stopped", "node_id", *nodeID, "run_id", *runID)
}
