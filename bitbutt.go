package bitbutt

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type BitButt struct {
	readOnly      bool
	sizeThreshold uint64
	syncOnPut     bool
	dirPerm       os.FileMode
	filePerm      os.FileMode

	keyDir map[string]*keyRecord

	directory string
	dataFiles []*dataFile

	mtx sync.RWMutex

	closed bool
}

type dataFile struct {
	f      *os.File
	name   string
	offset uint64
}

type keyRecord struct {
	fileID    int
	valuePos  int64
	valueSize uint64
	ts        time.Time
}

type Option func(*BitButt)

type Size uint64

const (
	KiB uint64 = 1024
	MiB        = 1024 * KiB
	GiB        = 1024 * MiB

	DefaultSize = 2 * GiB

	defaultDirPerms  = 0700
	defaultFilePerms = 0600

	dataFileSuffix = ".bitcask.data"
	hintFileSuffix = ".bitcask.hint"
)

var (
	errKeyTooLong   = errors.New("key too long")
	errValueTooLong = errors.New("value too long")
	errReadOnly     = errors.New("bitbutt is read-only")
	errNotDirectory = errors.New("bitbutt is not a directory")
	errNotFound     = errors.New("not found")
	errClosed       = errors.New("bitbutt is closed")
)

func Open(directory string, opts ...Option) (*BitButt, error) {
	b := &BitButt{
		sizeThreshold: DefaultSize,
		directory:     directory,
		dirPerm:       defaultDirPerms,
		filePerm:      defaultFilePerms,
		keyDir:        make(map[string]*keyRecord),
	}

	for _, opt := range opts {
		opt(b)
	}

	if fInfo, err := os.Stat(b.directory); err != nil {
		err := os.Mkdir(b.directory, b.dirPerm)
		if err != nil {
			return nil, err
		}
	} else {
		if !fInfo.IsDir() {
			return nil, errNotDirectory
		}
	}

	fDir, err := os.Open(b.directory)
	if err != nil {
		return nil, err
	}

	files, err := fDir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	dataFiles := []string{}
	for _, file := range files {
		if strings.HasSuffix(file, dataFileSuffix) {
			dataFiles = append(dataFiles, file[0:len(file)-len(dataFileSuffix)])
		}
	}

	for _, f := range dataFiles {
		// TODO: is this the right thing to do?
		if err := b.loadDataFile(f); err != nil {
			return nil, err
		}
	}

	df, err := b.newDataFile()
	if err != nil {
		return nil, err
	}

	b.dataFiles = append(b.dataFiles, df)

	return b, nil
}

func (b *BitButt) loadDataFile(file string) error {
	dataFileName := filepath.Join(b.directory, file+dataFileSuffix)
	hintFileName := filepath.Join(b.directory, file+hintFileSuffix)

	dataf, err := os.Open(dataFileName)
	if err != nil {
		return err
	}

	fileID := len(b.dataFiles)

	hintf, err := os.Open(hintFileName)
	if err == nil {
		defer hintf.Close()
		for {
			hint, err := readHintRecord(hintf)
			if err != nil {
				break
			}
			b.keyDir[string(hint.key)] = &keyRecord{fileID: fileID, valuePos: hint.valuePos, valueSize: hint.valueLen, ts: hint.ts}
		}
	} else {
		valuePos := int64(0)
		for {
			r, err := readRecord(dataf)
			if err != nil {
				break
			}

			if r.deleted {
				delete(b.keyDir, string(r.key))
			} else {
				b.keyDir[string(r.key)] = &keyRecord{fileID: fileID, valuePos: valuePos, valueSize: r.recordLen, ts: r.ts}
			}

			valuePos += int64(r.recordLen)
		}
	}

	b.dataFiles = append(b.dataFiles, &dataFile{f: dataf, name: dataFileName, offset: 0})

	return nil
}

func (b *BitButt) newDataFile() (*dataFile, error) {
	fName := filepath.Join(b.directory, strconv.FormatInt(time.Now().UnixNano(), 10)+dataFileSuffix)

	f, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR|os.O_APPEND, b.filePerm)
	if err != nil {
		return nil, err
	}

	return &dataFile{f: f, name: fName}, nil
}

func ReadOnly() Option {
	return func(b *BitButt) {
		b.readOnly = true
	}
}

func SyncOnPut() Option {
	return func(b *BitButt) {
		b.syncOnPut = true
	}
}

func SizeThreshold(size uint64) Option {
	return func(b *BitButt) {
		b.sizeThreshold = size
	}
}

func DirPerms(perm os.FileMode) Option {
	return func(b *BitButt) {
		b.dirPerm = perm
	}
}

func FilePerms(perm os.FileMode) Option {
	return func(b *BitButt) {
		b.filePerm = perm
	}
}

func (b *BitButt) Get(key []byte) ([]byte, error) {
	if b.closed {
		return nil, errClosed
	}

	b.mtx.RLock()
	defer b.mtx.RUnlock()

	keyDirRecord := b.keyDir[string(key)]
	if keyDirRecord == nil {
		return nil, errNotFound
	}

	df := b.dataFiles[keyDirRecord.fileID]
	f := df.f

	data := make([]byte, keyDirRecord.valueSize)
	if _, err := f.ReadAt(data, keyDirRecord.valuePos); err != nil {
		return nil, err
	}

	r, err := decodeRecord(data)
	if err != nil {
		return nil, err
	}

	return r.value, nil
}

func (b *BitButt) Put(key []byte, value []byte) error {
	if b.closed {
		return errClosed
	}
	if b.readOnly {
		return errReadOnly
	}

	if len(key) > maxKeyLen {
		return errKeyTooLong
	}
	if len(value) > maxValueLen {
		return errValueTooLong
	}

	ts := time.Now()
	buf := (&record{key: key, value: value, ts: ts}).Bytes()

	b.mtx.Lock()
	fileID := len(b.dataFiles) - 1

	df := b.dataFiles[fileID]

	_, err := df.f.Write(buf)
	if err != nil {
		b.mtx.Unlock()
		return err
	}

	keyDirRecord, ok := b.keyDir[string(key)]
	if !ok {
		keyDirRecord = &keyRecord{fileID: fileID, valuePos: int64(df.offset), valueSize: uint64(len(buf)), ts: ts}
		b.keyDir[string(key)] = keyDirRecord
	} else {
		keyDirRecord.fileID = fileID
		keyDirRecord.valuePos = int64(df.offset)
		keyDirRecord.valueSize = uint64(len(buf))
		keyDirRecord.ts = ts
	}

	df.offset += uint64(len(buf))

	buildHintFile := false

	if df.offset >= b.sizeThreshold {
		buildHintFile = true

		newDataFile, err := b.newDataFile()
		if err != nil {
			panic(err) // TODO: how do we handle a rollover fail?
		}
		b.dataFiles = append(b.dataFiles, newDataFile)
	}

	b.mtx.Unlock()

	if b.syncOnPut {
		if err := df.f.Sync(); err != nil {
			return err
		}
	}

	if buildHintFile {
		go b.buildHintFile(df, fileID)
	}

	return nil
}

func (b *BitButt) buildHintFile(df *dataFile, fileID int) {
	// TODO: implement.
}

func (b *BitButt) Delete(key []byte) error {
	if b.closed {
		return errClosed
	}

	data := getDeleteRecord(key, time.Now())

	b.mtx.Lock()

	var df *dataFile

	_, ok := b.keyDir[string(key)]
	if ok {
		delete(b.keyDir, string(key))

		fileID := len(b.dataFiles) - 1
		df = b.dataFiles[fileID]
		_, err := df.f.Write(data)
		if err != nil {
			b.mtx.Unlock()
			return err
		}

		df.offset += uint64(len(data))
	}

	b.mtx.Unlock()

	if df != nil && b.syncOnPut {
		if err := df.f.Sync(); err != nil {
			return err
		}
	}

	return nil
}

func (b *BitButt) AllKeys() (chan []byte, error) {
	if b.closed {
		return nil, errClosed
	}
	// TODO: implement
	return nil, errors.New("not implemented")
}

func (b *BitButt) Merge() error {
	if b.closed {
		return errClosed
	}
	// TODO: implement
	return errors.New("not implemented")
}

func (b *BitButt) Sync() error {
	if b.closed {
		return errClosed
	}

	b.mtx.Lock()
	f := b.dataFiles[len(b.dataFiles)-1].f
	b.mtx.Unlock()
	return f.Sync()
}

func (b *BitButt) Close() {
	if b.closed {
		return
	}

	b.mtx.Lock()
	for _, df := range b.dataFiles {
		df.f.Close()
	}
	b.dataFiles = nil
	b.keyDir = nil
	b.closed = true
	b.mtx.Unlock()
}
