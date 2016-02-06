package bitbutt

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BitButt implements a key-value store based on Basho's bitcask log-structured
// hash-table.
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
	fileID     int
	recordPos  int64
	recordSize uint64
	ts         time.Time
}

// Option is a data type to set options when calling Open.
type Option func(*BitButt)

const (
	// KiB is to be used with ThresholdSize to specify kibibytes (1024 bytes).
	KiB uint64 = 1024
	// MiB is to be used with ThresholdSize to specify mibibytes (1024 kibibytes).
	MiB = 1024 * KiB
	// GiB is to be used with ThresholdSize to specify gibibytes (1024 mibibytes).
	GiB = 1024 * MiB

	// DefaultSize is the default threshold size.
	DefaultSize = 2 * GiB

	defaultDirPerms  = 0700
	defaultFilePerms = 0600

	dataFileSuffix = ".bitbutt.data"
	hintFileSuffix = ".bitbutt.hint"
)

var (
	errKeyTooLong      = errors.New("key too long")
	errValueTooLong    = errors.New("value too long")
	errReadOnly        = errors.New("bitbutt is read-only")
	errNotDirectory    = errors.New("bitbutt is not a directory")
	errNotFound        = errors.New("not found")
	errClosed          = errors.New("bitbutt is closed")
	errInvalidDataFile = errors.New("invalid data file name")
)

// Open opens a bitbutt key-value store, found in directory. If directory
// doesn't exist yet, it is created. It returns a BitButt object, or an error
// if opening and loading the key-value store failed.
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
			b.keyDir[string(hint.key)] = &keyRecord{fileID: fileID, recordPos: hint.recordPos, recordSize: hint.recordSize, ts: hint.ts}
		}
	} else {
		recordPos := int64(0)
		for {
			r, err := readRecord(dataf)
			if err != nil {
				break
			}

			if r.deleted {
				delete(b.keyDir, string(r.key))
			} else {
				b.keyDir[string(r.key)] = &keyRecord{fileID: fileID, recordPos: recordPos, recordSize: r.recordLen, ts: r.ts}
			}

			recordPos += int64(r.recordLen)
		}
	}

	b.dataFiles = append(b.dataFiles, &dataFile{f: dataf, name: file, offset: 0})

	return nil
}

func (b *BitButt) newDataFile() (*dataFile, error) {
	file := strconv.FormatInt(time.Now().UnixNano(), 10)
	fName := filepath.Join(b.directory, file+dataFileSuffix)

	f, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR|os.O_APPEND, b.filePerm)
	if err != nil {
		return nil, err
	}

	return &dataFile{f: f, name: file}, nil
}

// ReadOnly sets a bitbutt store to be opened as read-only.
func ReadOnly() Option {
	return func(b *BitButt) {
		b.readOnly = true
	}
}

// SyncOnPut makes a bitbutt store call fsync(2) on every Put call.
func SyncOnPut() Option {
	return func(b *BitButt) {
		b.syncOnPut = true
	}
}

// SizeThreshold sets the size threshold when a bitbutt store shall create a new data file.
func SizeThreshold(size uint64) Option {
	return func(b *BitButt) {
		b.sizeThreshold = size
	}
}

// DirPerms overrides the default permissions to create directories.
func DirPerms(perm os.FileMode) Option {
	return func(b *BitButt) {
		b.dirPerm = perm
	}
}

// FilePerms overrides the default permissions to create files.
func FilePerms(perm os.FileMode) Option {
	return func(b *BitButt) {
		b.filePerm = perm
	}
}

// Get returns the stored value for the specified key, or an error if the key-value pair
// doesn't exist or there was another error when retrieving the value.
func (b *BitButt) Get(key []byte) ([]byte, error) {
	if b.closed {
		return nil, errClosed
	}

	b.mtx.RLock()
	defer b.mtx.RUnlock()

	keyDirRecord := b.keyDir[string(key)]
	if keyDirRecord == nil || keyDirRecord.recordSize == tombStone {
		return nil, errNotFound
	}

	df := b.dataFiles[keyDirRecord.fileID]
	f := df.f
	//log.Printf("Get %q: fileID:%d valuePos:%d valueSize:%d", string(key), keyDirRecord.fileID, keyDirRecord.valuePos, keyDirRecord.valueSize)

	data := make([]byte, keyDirRecord.recordSize)
	if _, err := f.ReadAt(data, keyDirRecord.recordPos); err != nil {
		return nil, err
	}

	r, err := readRecord(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	return r.value, nil
}

// Put stores the specified key-value pair in the bitbutt data store. It
// returns an error if the Put call failed.
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
		keyDirRecord = &keyRecord{fileID: fileID, recordPos: int64(df.offset), recordSize: uint64(len(buf)), ts: ts}
		b.keyDir[string(key)] = keyDirRecord
	} else {
		keyDirRecord.fileID = fileID
		keyDirRecord.recordPos = int64(df.offset)
		keyDirRecord.recordSize = uint64(len(buf))
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
		go func() {
			b.mtx.RLock()
			b.buildHintFile(df, fileID)
			b.mtx.RUnlock()
		}()
	}

	return nil
}

func (b *BitButt) buildHintFile(df *dataFile, fileID int) {
	hintFileName := filepath.Join(b.directory, df.name+hintFileSuffix)
	hintf, err := os.OpenFile(hintFileName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, b.filePerm)
	if err != nil {
		// assume the file exists already.
		return
	}
	defer hintf.Close()

	hints := []hintRecord{}

	for key, r := range b.keyDir {
		if r.fileID == fileID {
			hints = append(hints, hintRecord{
				ts:         r.ts,
				recordSize: r.recordSize,
				recordPos:  r.recordPos,
				key:        []byte(key),
			})
		}
	}

	for _, h := range hints {
		if _, err := h.WriteTo(hintf); err != nil {
			//log.Printf("WriteTo failed: %v", err)
			// TODO: how do we handle this error?
		}
	}
}

// Delete deletes the record identified by key. If the delete operation fails,
// it returns an error.
func (b *BitButt) Delete(key []byte) error {
	if b.closed {
		return errClosed
	}

	data := getDeleteRecord(key, time.Now())

	b.mtx.Lock()

	var df *dataFile

	r, ok := b.keyDir[string(key)]
	if ok {
		r.recordSize = tombStone

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

// AllKeys returns a channel from which all keys within the bitbutt can be
// received. It returns an error if there was a problem retrieving the keys.
func (b *BitButt) AllKeys() (<-chan []byte, error) {
	if b.closed {
		return nil, errClosed
	}

	ch := make(chan []byte, 1)

	go func() {
		defer close(ch)
		b.mtx.RLock()
		defer b.mtx.RUnlock()

		for key, record := range b.keyDir {
			if record.recordSize == tombStone {
				continue
			}
			ch <- []byte(key)
		}
	}()

	return ch, nil
}

// Merge merges existing data files if more than one (in addition to the
// currently active file) exist.
func (b *BitButt) Merge() error {
	if b.closed {
		return errClosed
	}
	if b.readOnly {
		return errReadOnly
	}

	b.mtx.RLock()

	// first, we determine the data files we want to merge. Since every data file
	// but the last one are only being read, we choose all but the last one.
	dataFilesToMerge := b.dataFiles[:len(b.dataFiles)-1]

	b.mtx.RUnlock()

	// we need at least two data files to merge.
	if len(dataFilesToMerge) < 2 {
		// not enough files to merge
		return nil
	}

	mergedHintFile := map[string]*keyRecord{}

	for fileID, df := range dataFilesToMerge {
		dataFileName := filepath.Join(b.directory, df.name+dataFileSuffix)
		hintFileName := filepath.Join(b.directory, df.name+hintFileSuffix)

		// we first check whether a hint file exists. If it does, we try to
		// read it, and fill our merged hint file.
		hintf, err := os.Open(hintFileName)
		if err == nil {
			for {
				hint, err := readHintRecord(hintf)
				if err != nil {
					break
				}

				r, ok := mergedHintFile[string(hint.key)]
				if ok {
					if hint.ts.After(r.ts) {
						r.fileID = fileID
						r.recordPos = hint.recordPos
						r.recordSize = hint.recordSize
						r.ts = hint.ts
					}
				} else {
					r = &keyRecord{fileID: fileID, recordPos: hint.recordPos, recordSize: hint.recordSize, ts: hint.ts}
					mergedHintFile[string(hint.key)] = r
				}
			}
			hintf.Close()
		} else {
			dataf, err := os.Open(dataFileName)
			if err != nil {
				//log.Printf("os.Open %s failed: %v", dataFileName, err)
				// TODO: should we signal error?
				continue
			}

			recordPos := int64(0)
			for {
				r, err := readRecord(dataf)
				if err != nil {
					break
				}

				kr, ok := mergedHintFile[string(r.key)]
				if ok {
					if r.ts.After(kr.ts) {
						kr.fileID = fileID
						kr.recordPos = recordPos
						kr.recordSize = r.recordLen
						kr.ts = r.ts
					}
				} else {
					kr = &keyRecord{fileID: fileID, recordPos: recordPos, recordSize: r.recordLen, ts: r.ts}
					mergedHintFile[string(r.key)] = kr
				}

				recordPos += int64(r.recordLen)
			}
			dataf.Close()
		}
	}

	newDataFile := &dataFile{}

	firstUnmergedName := dataFilesToMerge[len(dataFilesToMerge)-1].name
	nameNum, err := strconv.ParseUint(firstUnmergedName, 10, 64)
	if err != nil {
		return errInvalidDataFile
	}
	newName := strconv.FormatUint(nameNum+1, 10)
	newDataFile.name = newName

	mergedDataFileName := filepath.Join(b.directory, newName+dataFileSuffix)
	mergedHintFileName := filepath.Join(b.directory, newName+hintFileSuffix)

	newf, err := os.OpenFile(mergedDataFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, b.filePerm)
	if err != nil {
		return err
	}

	newHintFile, err := os.OpenFile(mergedHintFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, b.filePerm)
	if err != nil {
		// clean up newf
		newf.Close()
		os.Remove(mergedDataFileName)
		return err
	}

	rollbackOnError := func() {
		newf.Close()
		newHintFile.Close()
		os.Remove(mergedDataFileName)
		os.Remove(mergedHintFileName)
	}

	newDataFile.f = newf

	recordPos := int64(0)

	for key, keyDirRecord := range mergedHintFile {
		// ignore all deleted records.
		if keyDirRecord.recordSize == tombStone {
			continue
		}

		df := dataFilesToMerge[keyDirRecord.fileID]
		f := df.f

		data := make([]byte, keyDirRecord.recordSize)
		if _, err := f.ReadAt(data, keyDirRecord.recordPos); err != nil {
			rollbackOnError()
			return fmt.Errorf("ReadAt failed for key %q from file %s on position %d: %v", string(key), df.name, keyDirRecord.recordPos)
		}

		r, err := readRecord(bytes.NewReader(data))
		if err != nil {
			rollbackOnError()
			return fmt.Errorf("Decoding record failed for key %q from file %s on position %d: %v", string(key), df.name, keyDirRecord.recordPos)
		}

		recordData := r.Bytes()
		_, err = newDataFile.f.Write(recordData)
		if err != nil {
			rollbackOnError()
			return fmt.Errorf("Write to merged data file %s failed: %v", mergedDataFileName, err)
		}

		keyDirRecord.recordPos = recordPos
		keyDirRecord.fileID = 0

		recordPos += int64(len(recordData))

		hr := hintRecord{ts: keyDirRecord.ts, recordSize: keyDirRecord.recordSize, recordPos: keyDirRecord.recordPos, key: []byte(key)}
		if _, err := hr.WriteTo(newHintFile); err != nil {
			rollbackOnError()
			return fmt.Errorf("Write to merged hint file %s failed: %v", mergedHintFileName, err)
		}
	}
	newHintFile.Close()

	b.mtx.Lock()

	mergedDataFileCount := len(dataFilesToMerge)

	// correct b.dataFiles:
	b.dataFiles = append([]*dataFile{newDataFile}, b.dataFiles[mergedDataFileCount:]...)

	// correct valuePos, valueSize, and fileIDs in b.keyDir:
	for key, kr := range b.keyDir {
		if kr.fileID < mergedDataFileCount {
			if mergedRecord, ok := mergedHintFile[key]; ok {
				if mergedRecord.recordSize == tombStone {
					delete(b.keyDir, key) // TODO: is this legal?
				} else {
					//log.Printf("Merge %q: fileID:%d valuePos:%d valueSize:%d", key, kr.fileID, kr.valuePos, kr.valueSize)
					kr.fileID = mergedRecord.fileID
					kr.recordPos = mergedRecord.recordPos
					kr.recordSize = mergedRecord.recordSize
					kr.ts = mergedRecord.ts
				}
			} else {
				//log.Printf("couldn't find record for %s in existing keyDir", key)
				delete(b.keyDir, key) // TODO: is this legal?
			}
		} else {
			kr.fileID -= mergedDataFileCount - 1
		}
	}

	// remove merged data and hint files.
	for _, df := range dataFilesToMerge {
		dataFileName := filepath.Join(b.directory, df.name+dataFileSuffix)
		hintFileName := filepath.Join(b.directory, df.name+hintFileSuffix)
		os.Remove(dataFileName)
		os.Remove(hintFileName)
	}

	b.mtx.Unlock()

	return nil
}

// Sync calls fsync(2) on the latest data file.
func (b *BitButt) Sync() error {
	if b.closed {
		return errClosed
	}

	b.mtx.Lock()
	f := b.dataFiles[len(b.dataFiles)-1].f
	b.mtx.Unlock()
	return f.Sync()
}

// Close creates missing hint files, closes all open data files, and
// invalidates the BitButt object.
func (b *BitButt) Close() {
	if b.closed {
		return
	}

	b.mtx.Lock()
	b.buildHintFile(b.dataFiles[len(b.dataFiles)-1], len(b.dataFiles)-1)
	for _, df := range b.dataFiles {
		df.f.Close()
	}
	b.dataFiles = nil
	b.keyDir = nil
	b.closed = true
	b.mtx.Unlock()
}
