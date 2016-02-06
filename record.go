package bitbutt

import (
	"encoding/binary"
	"errors"
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
	errShortWrite = errors.New("short write")
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

func (r *record) Bytes() []byte {
	buf := make([]byte, 4+4+2+4+len(r.key)+len(r.value))

	binary.BigEndian.PutUint32(buf[4:8], uint32(r.ts.Unix()))
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(r.key)))
	binary.BigEndian.PutUint32(buf[10:14], uint32(len(r.value)))
	copy(buf[14:14+len(r.key)], r.key)
	copy(buf[14+len(r.key):], r.value)

	checksum := crc32.ChecksumIEEE(buf[4:])

	binary.BigEndian.PutUint32(buf[0:4], checksum)

	return buf
}

func getDeleteRecord(key []byte, ts time.Time) []byte {
	buf := make([]byte, 4+4+2+4+len(key))

	binary.BigEndian.PutUint32(buf[4:8], uint32(ts.Unix()))
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(key)))
	binary.BigEndian.PutUint32(buf[10:14], uint32(tombStone))
	copy(buf[14:], key)

	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	return buf
}

func readRecord(r io.Reader) (*record, error) {
	var (
		crc        uint32
		ts         int32
		keyLen     uint16
		valueLen   uint32
		key, value []byte
		deleted    bool
	)

	var buf [14]byte

	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}

	crc = binary.BigEndian.Uint32(buf[0:4])
	_ = crc // TODO: verify CRC.
	ts = int32(binary.BigEndian.Uint32(buf[4:8]))
	keyLen = binary.BigEndian.Uint16(buf[8:10])
	valueLen = binary.BigEndian.Uint32(buf[10:14])

	key = make([]byte, int(keyLen))
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}

	if valueLen == tombStone {
		deleted = true
	} else {
		value = make([]byte, int(valueLen))
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, err
		}
	}

	return &record{
		key:       key,
		value:     value,
		ts:        time.Unix(int64(ts), 0),
		recordLen: uint64(14 + len(key) + len(value)),
		deleted:   deleted,
	}, nil
}

func readHintRecord(r io.Reader) (*hintRecord, error) {
	var (
		ts       int32
		keyLen   uint16
		valueLen uint32
		valuePos uint64
	)

	var buf [18]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}

	ts = int32(binary.BigEndian.Uint32(buf[0:4]))
	keyLen = binary.BigEndian.Uint16(buf[4:6])
	valueLen = binary.BigEndian.Uint32(buf[6:10])
	valuePos = binary.BigEndian.Uint64(buf[10:18])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}

	return &hintRecord{
		ts:       time.Unix(int64(ts), 0),
		valueLen: uint64(valueLen),
		valuePos: int64(valuePos),
		key:      key,
	}, nil
}

func (r *hintRecord) WriteTo(w io.Writer) (int64, error) {
	recordSize := 4 + 2 + 4 + 8 + len(r.key)
	buf := make([]byte, recordSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(r.ts.Unix()))
	binary.BigEndian.PutUint16(buf[4:6], uint16(len(r.key)))
	binary.BigEndian.PutUint32(buf[6:10], uint32(r.valueLen))
	binary.BigEndian.PutUint64(buf[10:18], uint64(r.valuePos))
	copy(buf[18:], r.key)

	n, err := w.Write(buf)
	if err != nil {
		return 0, err
	}
	if n != recordSize {
		return int64(n), errShortWrite
	}
	return int64(len(buf)), nil
}
