package bitbutt

import (
	"errors"
	"sync"
	"time"
)

// Begin starts a new transaction.
func (b *BitButt) Begin() *Tx {
	return &Tx{
		b:        b,
		ts:       time.Now(),
		txKeyDir: make(map[string]*keyRecord),
		memStore: make(map[string][]byte),
	}
}

// Tx is a transaction.
//
// A transaction must always be ended with a call to either Commit or Rollback.
// After a Commit or Rollback, all operations on the transaction return an
// error.
type Tx struct {
	b  *BitButt
	ts time.Time // start time for transaction

	mtx sync.RWMutex

	done     bool
	txKeyDir map[string]*keyRecord
	memStore map[string][]byte
}

var (
	errTxDone = errors.New("transaction done")
)

// Get returns the stored value of the specified key. It takes key-value pairs
// that were updated in the current transaction into account.
func (t *Tx) Get(key []byte) ([]byte, error) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.done {
		return nil, errTxDone
	}
	r, ok := t.txKeyDir[string(key)]
	if !ok {
		return t.b.Get(key)
	}
	if r.recordSize != tombStone {
		return t.memStore[string(key)], nil
	}
	return nil, errNotFound
}

// Put stores the specified key-value pair for update in the transaction.
func (t *Tx) Put(key, value []byte) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.done {
		return errTxDone
	}

	if len(key) > maxKeyLen {
		t.done = true
		return errKeyTooLong
	}

	if len(value) > maxValueLen {
		t.done = true
		return errValueTooLong
	}

	t.memStore[string(key)] = value
	r, ok := t.txKeyDir[string(key)]
	if !ok {
		r = &keyRecord{
			ts: time.Now(),
		}
		t.txKeyDir[string(key)] = r
	}

	return nil
}

// Delete marks the record identified by key for deletion. It takes key-value
// pairs that were set or updated in the current transaction into account.
func (t *Tx) Delete(key []byte) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.done {
		return errTxDone
	}

	delete(t.memStore, string(key))
	r, ok := t.txKeyDir[string(key)]
	if !ok {
		r = &keyRecord{
			ts: time.Now(),
		}
		t.txKeyDir[string(key)] = r
	}
	r.recordSize = tombStone

	return nil
}

// Commit commits the transaction. It returns an error if there is an update
// conflict (i.e. at least one of the keys updated in the current transaction
// has been updated since the start of the transaction) or if the write to the
// bitbutt data file fails.
func (t *Tx) Commit() error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	t.done = true

	var (
		buf []byte
		md  []metaData
	)

	for key, r := range t.txKeyDir {
		var recordBuf []byte

		if r.recordSize != tombStone {
			recordBuf = (&record{key: []byte(key), value: t.memStore[key], ts: r.ts}).Bytes()
		} else {
			recordBuf = getDeleteRecord([]byte(key), r.ts)
		}
		buf = append(buf, recordBuf...)

		md = append(md, metaData{
			key:     []byte(key),
			deleted: r.recordSize == tombStone,
			ts:      t.ts,
			length:  uint64(len(recordBuf)),
		})
	}

	if err := t.b.bulkWrite(buf, md); err != nil {
		return err
	}

	return nil
}

// Rollback discards all changes of the current transaction.
func (t *Tx) Rollback() error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.done {
		return errTxDone
	}

	t.txKeyDir = nil
	t.memStore = nil
	t.done = true

	return nil
}
