// Package record codifica y lee lotes de registros con el formato del contrato
// (docs/contract.md, «Formato del lote»): una cabecera fija de 61 bytes en
// big-endian, con CRC-32C, seguida de los registros.
//
// Lo usan el cliente y las herramientas para fabricar los lotes de Produce y
// leer los de Fetch. El servidor no decodifica registros: valida la cabecera y
// guarda los bytes tal como llegan.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// HeaderSize es el tamaño de la cabecera fija de un lote.
	HeaderSize = 61
	// lengthPrefixSize: baseOffset (8) + batchLength (4), lo mínimo para saber cuánto mide un lote.
	lengthPrefixSize = 12
	// Magic es la versión del formato.
	Magic = 2
	// crcStart: el CRC cubre desde attributes hasta el final del lote.
	crcStart = 21
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt indica un lote que no supera la validación de versión, longitud o CRC.
var ErrCorrupt = errors.New("lote corrupto")

// Record es un registro de un lote. Key o Value nil se codifican con longitud -1.
type Record struct {
	Offset      int64 // al leer: baseOffset del lote + offsetDelta
	TimestampMs int64
	Key         []byte
	Value       []byte
}

// Producer es la identidad del productor en la cabecera; NoProducer va sin identidad.
type Producer struct {
	ID           int64
	Epoch        int16
	BaseSequence int32
}

// NoProducer: los tres campos a -1, como exige el contrato para un lote sin identidad.
var NoProducer = Producer{ID: -1, Epoch: -1, BaseSequence: -1}

// Header son los campos de la cabecera de un lote.
type Header struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	Producer             Producer
	RecordCount          int32
}

// Encode construye un lote como lo envía un productor: baseOffset = 0,
// partitionLeaderEpoch = -1 y el CRC calculado.
func Encode(records []Record, producer Producer) ([]byte, error) {
	if len(records) == 0 {
		return nil, errors.New("un lote necesita al menos un registro")
	}
	baseTs := records[0].TimestampMs
	maxTs := baseTs
	var body []byte
	for i, r := range records {
		maxTs = max(maxTs, r.TimestampMs)
		var rec []byte
		rec = append(rec, 0) // attributes del registro
		rec = binary.AppendVarint(rec, r.TimestampMs-baseTs)
		rec = binary.AppendVarint(rec, int64(i))
		rec = appendField(rec, r.Key)
		rec = appendField(rec, r.Value)
		rec = binary.AppendVarint(rec, 0) // headersCount
		body = binary.AppendVarint(body, int64(len(rec)))
		body = append(body, rec...)
	}

	out := make([]byte, HeaderSize, HeaderSize+len(body))
	binary.BigEndian.PutUint64(out[0:], 0)
	binary.BigEndian.PutUint32(out[8:], uint32(HeaderSize-lengthPrefixSize+len(body)))
	binary.BigEndian.PutUint32(out[12:], 0xFFFFFFFF) // partitionLeaderEpoch = -1
	out[16] = Magic
	binary.BigEndian.PutUint16(out[21:], 0) // attributes
	binary.BigEndian.PutUint32(out[23:], uint32(len(records)-1))
	binary.BigEndian.PutUint64(out[27:], uint64(baseTs))
	binary.BigEndian.PutUint64(out[35:], uint64(maxTs))
	binary.BigEndian.PutUint64(out[43:], uint64(producer.ID))
	binary.BigEndian.PutUint16(out[51:], uint16(producer.Epoch))
	binary.BigEndian.PutUint32(out[53:], uint32(producer.BaseSequence))
	binary.BigEndian.PutUint32(out[57:], uint32(len(records)))
	out = append(out, body...)
	binary.BigEndian.PutUint32(out[17:], crc32.Checksum(out[crcStart:], castagnoli))
	return out, nil
}

func appendField(b, field []byte) []byte {
	if field == nil {
		return binary.AppendVarint(b, -1)
	}
	b = binary.AppendVarint(b, int64(len(field)))
	return append(b, field...)
}

// ParseHeader lee la cabecera de un lote sin comprobar el CRC.
func ParseHeader(data []byte) (Header, error) {
	if len(data) < HeaderSize {
		return Header{}, fmt.Errorf("%w: %d bytes, menos que la cabecera", ErrCorrupt, len(data))
	}
	be := binary.BigEndian
	return Header{
		BaseOffset:           int64(be.Uint64(data[0:])),
		BatchLength:          int32(be.Uint32(data[8:])),
		PartitionLeaderEpoch: int32(be.Uint32(data[12:])),
		Magic:                int8(data[16]),
		CRC:                  be.Uint32(data[17:]),
		Attributes:           int16(be.Uint16(data[21:])),
		LastOffsetDelta:      int32(be.Uint32(data[23:])),
		BaseTimestamp:        int64(be.Uint64(data[27:])),
		MaxTimestamp:         int64(be.Uint64(data[35:])),
		Producer: Producer{
			ID:           int64(be.Uint64(data[43:])),
			Epoch:        int16(be.Uint16(data[51:])),
			BaseSequence: int32(be.Uint32(data[53:])),
		},
		RecordCount: int32(be.Uint32(data[57:])),
	}, nil
}

// Decode valida versión, longitud y CRC de un lote y devuelve sus registros.
func Decode(data []byte) (Header, []Record, error) {
	h, err := ParseHeader(data)
	if err != nil {
		return h, nil, err
	}
	if h.Magic != Magic {
		return h, nil, fmt.Errorf("%w: magic %d", ErrCorrupt, h.Magic)
	}
	if lengthPrefixSize+int(h.BatchLength) != len(data) {
		return h, nil, fmt.Errorf("%w: batchLength %d y %d bytes", ErrCorrupt, h.BatchLength, len(data))
	}
	if got := crc32.Checksum(data[crcStart:], castagnoli); got != h.CRC {
		return h, nil, fmt.Errorf("%w: CRC guardado %08x y calculado %08x", ErrCorrupt, h.CRC, got)
	}

	records := make([]Record, 0, h.RecordCount)
	pos := HeaderSize
	for range h.RecordCount {
		length, n := binary.Varint(data[pos:])
		if n <= 0 || length < 0 || pos+n+int(length) > len(data) {
			return h, nil, fmt.Errorf("%w: registro fuera del lote", ErrCorrupt)
		}
		pos += n
		end := pos + int(length)
		rec := data[pos:end]
		r, err := decodeRecord(rec[1:], h) // rec[0]: attributes
		if err != nil {
			return h, nil, err
		}
		records = append(records, r)
		pos = end
	}
	return h, records, nil
}

func decodeRecord(b []byte, h Header) (Record, error) {
	var r Record
	tsDelta, n := binary.Varint(b)
	if n <= 0 {
		return r, fmt.Errorf("%w: timestampDelta", ErrCorrupt)
	}
	b = b[n:]
	offDelta, n := binary.Varint(b)
	if n <= 0 {
		return r, fmt.Errorf("%w: offsetDelta", ErrCorrupt)
	}
	b = b[n:]
	key, b, err := readField(b)
	if err != nil {
		return r, err
	}
	value, _, err := readField(b)
	if err != nil {
		return r, err
	}
	return Record{
		Offset:      h.BaseOffset + offDelta,
		TimestampMs: h.BaseTimestamp + tsDelta,
		Key:         key,
		Value:       value,
	}, nil
}

func readField(b []byte) ([]byte, []byte, error) {
	length, n := binary.Varint(b)
	if n <= 0 {
		return nil, nil, fmt.Errorf("%w: longitud de campo", ErrCorrupt)
	}
	b = b[n:]
	if length == -1 {
		return nil, b, nil
	}
	if length < 0 || int(length) > len(b) {
		return nil, nil, fmt.Errorf("%w: campo de %d bytes fuera del registro", ErrCorrupt, length)
	}
	return b[:length:length], b[length:], nil
}

// Split separa los lotes concatenados de una respuesta Fetch, sin decodificarlos.
func Split(data []byte) ([][]byte, error) {
	var batches [][]byte
	for pos := 0; pos < len(data); {
		if len(data)-pos < lengthPrefixSize {
			return nil, fmt.Errorf("%w: lote cortado al final de la respuesta", ErrCorrupt)
		}
		end := pos + lengthPrefixSize + int(int32(binary.BigEndian.Uint32(data[pos+8:])))
		if end > len(data) || end-pos < HeaderSize {
			return nil, fmt.Errorf("%w: batchLength fuera de la respuesta", ErrCorrupt)
		}
		batches = append(batches, data[pos:end])
		pos = end
	}
	return batches, nil
}
