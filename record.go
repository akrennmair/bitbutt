package bitbutt

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"time"
)

const (
	maxKeyLen   = 1<<16 - 1
	maxValueLen = 1<<32 - 2
	tombStone   = 1<<32 - 1
)

var (
	crc32Table = crc32.MakeTable(crc32.IEEE)
)

type record struct {
	key       []byte
	value     []byte
	ts        time.Time
	recordLen uint64
	deleted   bool
}

type hintRecord struct {
	ts       time.Time
	valueLen uint64
	valuePos int64
	key      []byte
}

func decodeRecord(data []byte) (*record, error) {
	buf := bytes.NewReader(data)

	var (
		crc      uint32
		ts       int32
		keyLen   uint16
		valueLen uint32
	)

	if err := binary.Read(buf, binary.BigEndian, &crc); err != nil {
		return nil, err
	}

	if err := binary.Read(buf, binary.BigEndian, &ts); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &valueLen); err != nil {
		return nil, err
	}

	r := &record{}

	r.key = make([]byte, int(keyLen))
	if n, err := buf.Read(r.key); err != nil || n != len(r.key) {
		return nil, err
	}
	if valueLen == tombStone {
		r.deleted = true
	} else {
		r.value = make([]byte, int(valueLen))
		if n, err := buf.Read(r.value); err != nil || n != len(r.value) {
			return nil, err
		}
	}
	r.ts = time.Unix(int64(ts), 0)

	return r, nil
}

func (r *record) Bytes() []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, int32(r.ts.Unix()))
	binary.Write(&buf, binary.BigEndian, uint16(len(r.key)))
	binary.Write(&buf, binary.BigEndian, uint32(len(r.value)))
	buf.Write(r.key)
	buf.Write(r.value)

	data := buf.Bytes()

	checksum := crc32.Checksum(data, crc32Table)

	var checksumBuf bytes.Buffer
	binary.Write(&checksumBuf, binary.BigEndian, checksum)

	result := append(checksumBuf.Bytes(), data...)

	return result
}

func getDeleteRecord(key []byte, ts time.Time) []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, int32(ts.Unix()))
	binary.Write(&buf, binary.BigEndian, uint16(len(key)))
	binary.Write(&buf, binary.BigEndian, uint32(tombStone))
	buf.Write(key)

	data := buf.Bytes()

	checksum := crc32.Checksum(data, crc32Table)
	var checksumBuf bytes.Buffer
	binary.Write(&checksumBuf, binary.BigEndian, checksum)

	result := append(checksumBuf.Bytes(), data...)

	return result
}

func readRecord(r io.Reader) (*record, error) {
	var (
		crc      uint32
		ts       int32
		keyLen   uint16
		valueLen uint32
	)

	if err := binary.Read(r, binary.BigEndian, &crc); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &ts); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return nil, err
	}

	key := make([]byte, int(keyLen))
	if n, err := r.Read(key); err != nil || n < len(key) {
		return nil, err
	}

	value := make([]byte, int(valueLen))
	if n, err := r.Read(value); err != nil || n < len(value) {
		return nil, err
	}

	return &record{
		key:       key,
		value:     value,
		ts:        time.Unix(int64(ts), 0),
		recordLen: uint64(14 + len(key) + len(value)),
	}, nil
}

func readHintRecord(r io.Reader) (*hintRecord, error) {
	var (
		ts       int32
		keyLen   uint16
		valueLen uint32
		valuePos uint64
	)

	if err := binary.Read(r, binary.BigEndian, &ts); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &valuePos); err != nil {
		return nil, err
	}
	key := make([]byte, int(keyLen))
	if n, err := r.Read(key); err != nil || n != len(key) {
		return nil, err
	}

	return &hintRecord{
		ts:       time.Unix(int64(ts), 0),
		valueLen: uint64(valueLen),
		valuePos: int64(valuePos),
		key:      key,
	}, nil
}
