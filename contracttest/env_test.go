package contracttest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
	"github.com/alinadelanoval/bitacora/internal/record"
	"github.com/alinadelanoval/bitacora/pkg/client"
)

const (
	profileBasic    = "basic"
	profileComplete = "complete"
	// createTimeout: crear un tema puede arrancar procesos (una réplica por copia en el prototipo).
	createTimeout = 60 * time.Second
	callTimeout   = 15 * time.Second
)

// env es la conexión de una prueba al endpoint bajo prueba.
type env struct {
	t       *testing.T
	c       *client.Client
	profile string
}

// connect conecta con el primer endpoint de BITACORA_ENDPOINTS o salta la prueba.
func connect(t *testing.T) *env {
	t.Helper()
	endpoints := os.Getenv("BITACORA_ENDPOINTS")
	if endpoints == "" {
		t.Skip("BITACORA_ENDPOINTS sin definir: no hay implementación contra la que probar")
	}
	profile := os.Getenv("BITACORA_PROFILE")
	if profile == "" {
		profile = profileBasic
	}
	if profile != profileBasic && profile != profileComplete {
		t.Fatalf("BITACORA_PROFILE = %q; se esperaba %q o %q", profile, profileBasic, profileComplete)
	}
	c, err := client.Dial(strings.Split(endpoints, ",")[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &env{t: t, c: c, profile: profile}
}

// requireComplete marca la prueba como «no aplica» contra una implementación básica.
func (e *env) requireComplete() {
	e.t.Helper()
	if e.profile != profileComplete {
		e.t.Skip("no aplica: regla del perfil completo y la implementación declara el básico")
	}
}

func (e *env) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	e.t.Cleanup(cancel)
	return ctx
}

// name devuelve un nombre de tema o de grupo que ninguna otra ejecución usa.
func name(prefix string) string {
	b := make([]byte, 6)
	rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}

// topic crea un tema nuevo y devuelve su nombre.
func (e *env) topic(partitions, replicationFactor int32) string {
	e.t.Helper()
	topic := name("ct")
	ctx, cancel := context.WithTimeout(context.Background(), createTimeout)
	defer cancel()
	if _, err := e.c.CreateTopic(ctx, name("req"), topic, partitions, replicationFactor); err != nil {
		e.t.Fatalf("CreateTopic: %v", err)
	}
	return topic
}

// produce escribe registros con ACKS_ALL y devuelve el offset del primero.
func (e *env) produce(topic string, partition int32, values ...string) int64 {
	e.t.Helper()
	recs := make([]record.Record, len(values))
	now := time.Now().UnixMilli()
	for i, v := range values {
		recs[i] = record.Record{TimestampMs: now, Value: []byte(v)}
	}
	base, err := e.c.Produce(e.ctx(), topic, partition, pb.Acks_ACKS_ALL, 10_000, recs)
	if err != nil {
		e.t.Fatalf("Produce: %v", err)
	}
	return base
}

// readAll lee la partición desde offset hasta el high watermark.
func (e *env) readAll(topic string, partition int32, offset int64) []record.Record {
	e.t.Helper()
	var out []record.Record
	for {
		recs, hw, err := e.c.Fetch(e.ctx(), topic, partition, offset, 1<<20, 0)
		if err != nil {
			e.t.Fatalf("Fetch(%d): %v", offset, err)
		}
		out = append(out, recs...)
		if len(recs) == 0 || recs[len(recs)-1].Offset+1 >= hw {
			return out
		}
		offset = recs[len(recs)-1].Offset + 1
	}
}

// eventually repite cond hasta que se cumple o vence el plazo.
func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("la condición no se cumplió en %v", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func tp(topic string, partition int32) *pb.TopicPartition {
	return &pb.TopicPartition{Topic: topic, Partition: partition}
}
