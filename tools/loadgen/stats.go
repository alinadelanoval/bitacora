package main

import (
	"math"
	"sort"
	"time"
)

// Latency resume una distribución de latencias en milisegundos.
type Latency struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

// summarize calcula los percentiles por rango más cercano: el valor en la
// posición ceil(p·n) de la muestra ordenada, sin interpolar.
func summarize(ds []time.Duration) Latency {
	if len(ds) == 0 {
		return Latency{}
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	rank := func(p float64) time.Duration {
		i := int(math.Ceil(p*float64(len(s)))) - 1
		return s[max(0, min(i, len(s)-1))]
	}
	return Latency{Count: len(s), P50: ms(rank(0.50)), P95: ms(rank(0.95)), P99: ms(rank(0.99)), Max: ms(s[len(s)-1])}
}
