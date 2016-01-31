package bitbutt

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
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
	key   []byte
	value []byte
	ts    time.Time
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
	r.value = make([]byte, int(valueLen))
	if n, err := buf.Read(r.value); err != nil || n != len(r.value) {
		return nil, err
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
