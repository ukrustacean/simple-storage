package datastore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	outFileName = "segment"
	Mb          = int64(1024 * 1024)
)

var NotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type putRequest struct {
	key       string
	value     string
	valueType RecordKind
	respChan  chan error
}

type mergeRequest struct {
	respChan chan error
}

type Db struct {
	dir           string
	segmentSize   int64
	segments      []*Segment
	activeSegment *Segment
	segmentsMutex sync.RWMutex
	putRequests   chan putRequest
	mergeRequests chan mergeRequest
	shutdown      chan struct{}
	wg            sync.WaitGroup
}

type Segment struct {
	file     *os.File
	filePath string
	offset   int64
	index    hashIndex
	idxMu    sync.RWMutex
}

func newSegment(dir string, id int) (*Segment, error) {
	name := fmt.Sprintf("%s-%d", outFileName, id)
	path := filepath.Join(dir, name)
	return &Segment{
		filePath: path,
		index:    make(hashIndex),
	}, nil
}

func Open(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		dir:           dir,
		segmentSize:   segmentSize,
		segments:      []*Segment{},
		putRequests:   make(chan putRequest),
		mergeRequests: make(chan mergeRequest),
		shutdown:      make(chan struct{}),
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	err := db.recover()
	if err != nil {
		return nil, err
	}

	if len(db.segments) == 0 {
		segment, err := newSegment(db.dir, 0)
		if err != nil {
			return nil, err
		}
		db.segments = append(db.segments, segment)
		db.activeSegment = segment
	} else {
		sort.Slice(db.segments, func(i, j int) bool {
			return db.segments[i].filePath < db.segments[j].filePath
		})
		db.activeSegment = db.segments[len(db.segments)-1]
	}

	db.wg.Add(1)
	go db.ioWorker()

	return db, nil
}

func (db *Db) ioWorker() {
	defer db.wg.Done()

	var err error

	db.activeSegment.file, err = os.OpenFile(db.activeSegment.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ioWorker: failed to open active segment %s: %v\n", db.activeSegment.filePath, err)
		return
	}
	stat, err := db.activeSegment.file.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ioWorker: failed to stat active segment %s: %v\n", db.activeSegment.filePath, err)
		db.activeSegment.file.Close()
		return
	}
	db.activeSegment.offset = stat.Size()

	defer func() {
		if db.activeSegment.file != nil {
			db.activeSegment.file.Close()
		}
	}()

	for {
		select {
		case req := <-db.putRequests:
			e := entry{key: req.key, value: req.value, kind: req.valueType}
			encoded := e.Encode()
			n, err := db.activeSegment.file.Write(encoded)
			if err != nil {
				req.respChan <- err
				continue
			}

			db.activeSegment.idxMu.Lock()
			db.activeSegment.index[req.key] = db.activeSegment.offset
			db.activeSegment.idxMu.Unlock()
			db.activeSegment.offset += int64(n)

			req.respChan <- nil
			if db.activeSegment.offset >= db.segmentSize {
				if err := db.activeSegment.file.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "ioWorker: failed to close segment %s: %v\n", db.activeSegment.filePath, err)
				}
				db.activeSegment.file = nil

				db.segmentsMutex.Lock()
				newSegID := len(db.segments)
				newActiveSegment, err := newSegment(db.dir, newSegID)
				if err != nil {
					fmt.Fprintf(os.Stderr, "ioWorker: failed to create new segment: %v\n", err)
					db.segmentsMutex.Unlock()

					continue
				}
				db.segments = append(db.segments, newActiveSegment)
				db.activeSegment = newActiveSegment

				db.activeSegment.file, err = os.OpenFile(db.activeSegment.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o600)
				if err != nil {
					fmt.Fprintf(os.Stderr, "ioWorker: failed to open new active segment %s: %v\n", db.activeSegment.filePath, err)
					db.segmentsMutex.Unlock()
					continue
				}
				db.activeSegment.offset = 0
				db.segmentsMutex.Unlock()
			}

		case req := <-db.mergeRequests:
			if db.activeSegment.file != nil {
				if err := db.activeSegment.file.Close(); err != nil {
					req.respChan <- fmt.Errorf("ioWorker: failed to close active segment before merge: %w", err)
					continue
				}
				db.activeSegment.file = nil
			}

			err := db.performMerge()
			req.respChan <- err
			if db.activeSegment != nil {
				db.activeSegment.file, err = os.OpenFile(db.activeSegment.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o600)
				if err != nil {
					fmt.Fprintf(os.Stderr, "ioWorker: failed to re-open active segment %s post-merge: %v\n", db.activeSegment.filePath, err)
				} else {
					stat, statErr := db.activeSegment.file.Stat()
					if statErr != nil {
						fmt.Fprintf(os.Stderr, "ioWorker: failed to stat re-opened active segment %s post-merge: %v\n", db.activeSegment.filePath, statErr)
					} else {
						db.activeSegment.offset = stat.Size()
					}
				}
			}

		case <-db.shutdown:
			return
		}
	}
}

func (db *Db) recover() error {
	files, err := os.ReadDir(db.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	segmentIDCounter := 0
	for _, file := range files {
		if file.IsDir() || !filepath.HasPrefix(file.Name(), outFileName) {
			continue
		}
		seg, _ := newSegment(db.dir, segmentIDCounter)
		seg.filePath = filepath.Join(db.dir, file.Name())

		f, err := os.OpenFile(seg.filePath, os.O_RDONLY, 0o600)
		if err != nil {
			return fmt.Errorf("recover: could not open segment file %s: %w", seg.filePath, err)
		}

		var currentOffset int64 = 0
		reader := bufio.NewReader(f)
		for {
			var rec entry
			pos := currentOffset
			n, readErr := rec.DecodeFromReader(reader)
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				f.Close()
				return fmt.Errorf("recover: corrupt segment %s: %w", seg.filePath, readErr)
			}
			seg.index[rec.key] = pos
			currentOffset += int64(n)
		}
		f.Close()
		db.segments = append(db.segments, seg)
		segmentIDCounter++
	}
	return nil
}

func (db *Db) Close() error {
	close(db.shutdown)
	db.wg.Wait()
	return nil
}

func (db *Db) Get(key string) (string, error) {
	val, typ, err := db.getRaw(key)
	if err != nil {
		return "", err
	}
	if typ != Text {
		return "", fmt.Errorf("expected string, got type 0x%x", typ)
	}
	return val, nil
}

func (db *Db) Put(key, value string) error {
	respChan := make(chan error)
	db.putRequests <- putRequest{
		key:       key,
		value:     value,
		valueType: Text,
		respChan:  respChan,
	}
	return <-respChan
}

func (db *Db) Size() (int64, error) {
	db.segmentsMutex.RLock()
	activeSegPath := db.activeSegment.filePath
	db.segmentsMutex.RUnlock()
	info, err := os.Stat(activeSegPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

func (db *Db) MergeSegments() error {
	respChan := make(chan error)
	db.mergeRequests <- mergeRequest{respChan: respChan}
	return <-respChan
}

func (db *Db) performMerge() error {
	db.segmentsMutex.Lock()
	defer db.segmentsMutex.Unlock()

	if len(db.segments) <= 1 {
		return nil
	}

	maxID := -1
	for _, seg := range db.segments {
		var id int

		if n, _ := fmt.Sscanf(filepath.Base(seg.filePath), outFileName+"-%d", &id); n == 1 {
			if id > maxID {
				maxID = id
			}
		}
	}
	mergedSegmentID := maxID + 1

	mergedSegment, err := newSegment(db.dir, mergedSegmentID)
	if err != nil {
		return fmt.Errorf("performMerge: failed to create new segment object: %w", err)
	}

	mergedFile, err := os.OpenFile(mergedSegment.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("performMerge: failed to open merged segment file %s: %w", mergedSegment.filePath, err)
	}
	defer mergedFile.Close()
	latestKeyOffsets := make(map[string]struct {
		segmentPath string
		offset      int64
	})

	for i := len(db.segments) - 1; i >= 0; i-- {
		segment := db.segments[i]
		segment.idxMu.RLock()
		for key, offset := range segment.index {
			if _, exists := latestKeyOffsets[key]; !exists {
				latestKeyOffsets[key] = struct {
					segmentPath string
					offset      int64
				}{segment.filePath, offset}
			}
		}
		segment.idxMu.RUnlock()
	}

	var currentMergedOffset int64 = 0
	for key, data := range latestKeyOffsets {
		f, err := os.Open(data.segmentPath)
		if err != nil {
			return fmt.Errorf("performMerge: could not open source segment %s for key %s: %w", data.segmentPath, key, err)
		}
		_, err = f.Seek(data.offset, io.SeekStart)
		if err != nil {
			f.Close()
			return fmt.Errorf("performMerge: could not seek in source segment %s for key %s: %w", data.segmentPath, key, err)
		}

		var record entry
		if _, err := record.DecodeFromReader(bufio.NewReader(f)); err != nil {
			f.Close()
			return fmt.Errorf("performMerge: could not decode record from source segment %s for key %s: %w", data.segmentPath, key, err)
		}
		f.Close()

		encodedEntry := record.Encode()
		n, err := mergedFile.Write(encodedEntry)
		if err != nil {
			return fmt.Errorf("performMerge: could not write entry for key %s to merged segment: %w", key, err)
		}

		mergedSegment.idxMu.Lock()
		mergedSegment.index[key] = currentMergedOffset
		mergedSegment.idxMu.Unlock()
		currentMergedOffset += int64(n)
	}
	mergedSegment.offset = currentMergedOffset

	oldSegmentPaths := make([]string, len(db.segments))
	for i, s := range db.segments {
		oldSegmentPaths[i] = s.filePath
	}

	db.segments = []*Segment{mergedSegment}
	db.activeSegment = mergedSegment

	for _, p := range oldSegmentPaths {
		if p == mergedSegment.filePath {
			continue
		}
		if err := os.Remove(p); err != nil {
			fmt.Fprintf(os.Stderr, "performMerge: failed to remove old segment file %s: %v\n", p, err)
		}
	}
	return nil
}

func (db *Db) PutInt64(key string, value int64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(value))
	respChan := make(chan error)

	db.putRequests <- putRequest{
		key:       key,
		value:     string(buf),
		valueType: Number,
		respChan:  respChan,
	}
	return <-respChan
}

func (db *Db) GetInt64(key string) (int64, error) {
	val, typ, err := db.getRaw(key)
	if err != nil {
		return 0, err
	}
	if typ != Number {
		return 0, fmt.Errorf("expected int64, got type 0x%x", typ)
	}
	if len(val) != 8 {
		return 0, fmt.Errorf("corrupt int64 encoding")
	}
	return int64(binary.LittleEndian.Uint64([]byte(val))), nil
}

func (db *Db) getRaw(key string) (string, RecordKind, error) {
	db.segmentsMutex.RLock()
	segmentsSnapshot := make([]*Segment, len(db.segments))
	copy(segmentsSnapshot, db.segments)
	db.segmentsMutex.RUnlock()

	for i := len(segmentsSnapshot) - 1; i >= 0; i-- {
		segment := segmentsSnapshot[i]

		segment.idxMu.RLock()
		offset, ok := segment.index[key]
		segment.idxMu.RUnlock()
		if !ok {
			continue
		}

		f, err := os.Open(segment.filePath)
		if err != nil {
			return "", 0, fmt.Errorf("could not open segment file %s: %w", segment.filePath, err)
		}
		defer f.Close()

		_, err = f.Seek(offset, io.SeekStart)
		if err != nil {
			return "", 0, fmt.Errorf("could not seek in segment file %s: %w", segment.filePath, err)
		}

		var rec entry
		if _, err := rec.DecodeFromReader(bufio.NewReader(f)); err != nil {
			return "", 0, fmt.Errorf("could not decode record from segment file %s: %w", segment.filePath, err)
		}
		return rec.value, rec.kind, nil
	}
	return "", 0, NotFound
}
