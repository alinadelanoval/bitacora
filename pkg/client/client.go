// Package client es la librería cliente de Bitácora: habla el contrato
// bitacora.v1 con cualquier implementación (el gateway del prototipo Ray o
// bitacorad) sin saber cuál hay detrás.
//
// Esta versión es mínima: una conexión, una operación por método y los errores
// del contrato como valores de Go. El productor con descubrimiento de líderes,
// particionado, lotes y reintentos llega con BIT-50, y el consumidor con
// grupos, con BIT-51 y BIT-70.
package client

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/alinadelanoval/bitacora/gen/go/bitacora/v1"
	"github.com/alinadelanoval/bitacora/internal/record"
)

// Error es un ErrorCode distinto de NONE devuelto por el servidor.
type Error struct {
	Op   string
	Code pb.ErrorCode
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Op, e.Code) }

// Code devuelve el ErrorCode de err, ERROR_CODE_NONE si err es nil y
// ERROR_CODE_NONE también si err no es un error del contrato (fallo de canal).
func Code(err error) pb.ErrorCode {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return pb.ErrorCode_ERROR_CODE_NONE
}

func check(op string, code pb.ErrorCode) error {
	if code == pb.ErrorCode_ERROR_CODE_NONE {
		return nil
	}
	return &Error{Op: op, Code: code}
}

// Client es una conexión a un endpoint de Bitácora. Los stubs quedan
// accesibles para quien necesite la petición o la respuesta completas.
type Client struct {
	conn   *grpc.ClientConn
	Log    pb.LogClient
	Groups pb.GroupsClient
	Admin  pb.AdminClient
}

// Dial abre una conexión sin TLS (el contrato deja TLS fuera de alcance).
func Dial(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, Log: pb.NewLogClient(conn), Groups: pb.NewGroupsClient(conn),
		Admin: pb.NewAdminClient(conn)}, nil
}

// Close cierra la conexión.
func (c *Client) Close() error { return c.conn.Close() }

// CreateTopic crea un tema; repetirlo con el mismo requestID es un reintento.
func (c *Client) CreateTopic(ctx context.Context, requestID, topic string, partitions, replicationFactor int32,
) (*pb.TopicMetadata, error) {
	resp, err := c.Admin.CreateTopic(ctx, &pb.CreateTopicRequest{RequestId: requestID, Topic: topic,
		Partitions: partitions, ReplicationFactor: replicationFactor})
	if err != nil {
		return nil, err
	}
	return resp.GetMetadata(), check("CreateTopic", resp.GetError())
}

// Produce codifica los registros en un lote sin identidad de productor, lo
// envía y devuelve el offset del primero. Un REQUEST_TIMED_OUT o un fallo del
// canal dejan el resultado indeterminado: el lote pudo escribirse.
func (c *Client) Produce(ctx context.Context, topic string, partition int32, acks pb.Acks, timeoutMs int32,
	records []record.Record) (int64, error) {
	batch, err := record.Encode(records, record.NoProducer)
	if err != nil {
		return 0, err
	}
	resp, err := c.Log.Produce(ctx, &pb.ProduceRequest{Topic: topic, Partition: &partition, Acks: acks,
		TimeoutMs: timeoutMs, RecordBatch: batch})
	if err != nil {
		return 0, err
	}
	return resp.GetBaseOffset(), check("Produce", resp.GetError())
}

// Fetch lee desde offset y devuelve los registros desde ese offset (descarta
// los anteriores del primer lote) y el high watermark de la respuesta.
func (c *Client) Fetch(ctx context.Context, topic string, partition int32, offset int64, maxBytes,
	maxWaitMs int32) ([]record.Record, int64, error) {
	resp, err := c.Log.Fetch(ctx, &pb.FetchRequest{Topic: topic, Partition: &partition, Offset: offset,
		MaxBytes: maxBytes, MaxWaitMs: maxWaitMs})
	if err != nil {
		return nil, 0, err
	}
	if err := check("Fetch", resp.GetError()); err != nil {
		return nil, 0, err
	}
	batches, err := record.Split(resp.GetRecordBatches())
	if err != nil {
		return nil, 0, err
	}
	var out []record.Record
	for _, b := range batches {
		_, recs, err := record.Decode(b)
		if err != nil {
			return nil, 0, err
		}
		for _, r := range recs {
			if r.Offset >= offset {
				out = append(out, r)
			}
		}
	}
	return out, resp.GetHighWatermark(), nil
}

// ListOffsets devuelve el offset de una posición declarada; -1 si no existe.
func (c *Client) ListOffsets(ctx context.Context, topic string, partition int32, position pb.OffsetPosition,
	timestampMs int64) (int64, error) {
	resp, err := c.Log.ListOffsets(ctx, &pb.ListOffsetsRequest{
		TopicPartition: &pb.TopicPartition{Topic: topic, Partition: partition},
		Position:       position, TimestampUnixMs: timestampMs})
	if err != nil {
		return 0, err
	}
	return resp.GetOffset(), check("ListOffsets", resp.GetError())
}
