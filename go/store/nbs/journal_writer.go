// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	chunkJournalFileSize = 256 * 1024 * 1024

	// todo(andy): buffer must be able to hold an entire record,
	//   but we don't have a hard limit on record size right now
	journalWriterBuffSize = 1024 * 1024

	chunkJournalAddr = "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"

	journalIndexThreshold = 64 * 1024
)

var (
	journalAddr = addr(hash.Parse(chunkJournalAddr))
)

func isJournalAddr(a addr) bool {
	return a == journalAddr
}

func journalFileExists(path string) (bool, error) {
	var err error
	if path, err = filepath.Abs(path); err != nil {
		return false, err
	}

	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if info.IsDir() {
		return true, fmt.Errorf("expected file %s found directory", chunkJournalName)
	}
	return true, nil
}

func openJournalWriter(ctx context.Context, path string) (wr *journalWriter, exists bool, err error) {
	var f *os.File
	if path, err = filepath.Abs(path); err != nil {
		return nil, false, err
	}

	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	} else if info.IsDir() {
		return nil, true, fmt.Errorf("expected file %s found directory", chunkJournalName)
	}
	if f, err = os.OpenFile(path, os.O_RDWR, 0666); err != nil {
		return nil, true, err
	}

	return &journalWriter{
		buf:   make([]byte, 0, journalWriterBuffSize),
		index: newRangeIndex(),
		file:  f,
		path:  path,
	}, true, nil
}

func createJournalWriter(ctx context.Context, path string) (wr *journalWriter, err error) {
	var f *os.File
	if path, err = filepath.Abs(path); err != nil {
		return nil, err
	}

	_, err = os.Stat(path)
	if err == nil {
		return nil, fmt.Errorf("journal file %s already exists", chunkJournalName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		return nil, err
	}
	const batch = 1024 * 1024
	b := make([]byte, batch)
	for i := 0; i < chunkJournalFileSize; i += batch {
		if _, err = f.Write(b); err != nil { // zero fill |f|
			return nil, err
		}
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if o, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	} else if o != 0 {
		return nil, fmt.Errorf("expected file journalOffset 0, got %d", o)
	}

	return &journalWriter{
		buf:   make([]byte, 0, journalWriterBuffSize),
		index: newRangeIndex(),
		file:  f,
		path:  path,
	}, nil
}

type journalWriter struct {
	buf     []byte
	file    *os.File
	index   rangeIndex
	off     int64
	uncmpSz uint64
	path    string
	lock    sync.RWMutex
}

var _ io.Closer = &journalWriter{}

// bootstrapJournal reads the journal file collecting a recLookup for each record and
// returning the latest committed root hash.
func (wr *journalWriter) bootstrapJournal(ctx context.Context) (last hash.Hash, err error) {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	wr.off, err = processJournalRecords(ctx, wr.file, func(o int64, r journalRec) error {
		switch r.kind {
		case chunkJournalRecKind:
			wr.index.put(r.address, Range{
				Offset: uint64(o) + uint64(r.payloadOffset()),
				Length: uint32(len(r.payload)),
			})
			wr.uncmpSz += r.uncompressedPayloadSize()
		case rootHashJournalRecKind:
			last = hash.Hash(r.address)
		default:
			return fmt.Errorf("unknown journal record kind (%d)", r.kind)
		}
		return nil
	})
	if err != nil {
		return hash.Hash{}, err
	}
	return
}

// hasAddr returns true if the journal contains a chunk with addr |h|.
func (wr *journalWriter) hasAddr(h addr) (ok bool) {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	_, ok = wr.index.get(h)
	return
}

// getCompressedChunk reads the CompressedChunks with addr |h|.
func (wr *journalWriter) getCompressedChunk(h addr) (CompressedChunk, error) {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	r, ok := wr.index.get(h)
	if !ok {
		return CompressedChunk{}, nil
	}

	buf := make([]byte, r.Length)
	if _, err := wr.readAt(buf, int64(r.Offset)); err != nil {
		return CompressedChunk{}, nil
	}
	return NewCompressedChunk(hash.Hash(h), buf)
}

// getRange returns a Range for the chunk with addr |h|.
func (wr *journalWriter) getRange(h addr) (rng Range, ok bool, err error) {
	// callers will use |rng| to read directly from the
	// journal file, so we must flush here
	if err = wr.maybeFlush(); err != nil {
		return
	}
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	rng, ok = wr.index.get(h)
	return
}

// writeCompressedChunk writes |cc| to the journal.
func (wr *journalWriter) writeCompressedChunk(cc CompressedChunk) error {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	recordLen, payloadOff := chunkRecordSize(cc)
	rng := Range{
		Offset: uint64(wr.offset()) + uint64(payloadOff),
		Length: uint32(len(cc.FullCompressedChunk)),
	}
	buf, err := wr.getBytes(int(recordLen))
	if err != nil {
		return err
	}
	_ = writeChunkRecord(buf, cc)
	wr.index.put(addr(cc.H), rng)
	return nil
}

// writeRootHash commits |root| to the journal and syncs the file to disk.
func (wr *journalWriter) writeRootHash(root hash.Hash) error {
	wr.lock.Lock()
	defer wr.lock.Unlock()

	sz := rootHashRecordSize()
	buf, err := wr.getBytes(sz)
	if err != nil {
		return err
	}
	_ = writeRootHashRecord(buf, addr(root))

	if err = wr.flush(); err != nil {
		return err
	}
	if err = wr.file.Sync(); err != nil {
		return err
	}

	if wr.index.atCapacity() {
		// pass pre-commit journal offset
		err = wr.index.persistNovel(root, wr.offset()-int64(sz))
	}
	return err
}

// readAt reads len(p) bytes from the journal at offset |off|.
func (wr *journalWriter) readAt(p []byte, off int64) (n int, err error) {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	var bp []byte
	if off < wr.off {
		// fill some or all of |p| from |wr.file|
		fread := int(wr.off - off)
		if len(p) > fread {
			// straddled read
			bp = p[fread:]
			p = p[:fread]
		}
		if n, err = wr.file.ReadAt(p, off); err != nil {
			return 0, err
		}
		off = 0
	} else {
		// fill all of |p| from |wr.buf|
		bp = p
		off -= wr.off
	}
	n += copy(bp, wr.buf[off:])
	return
}

// getBytes returns a buffer for writers to copy data into.
func (wr *journalWriter) getBytes(n int) (buf []byte, err error) {
	c, l := cap(wr.buf), len(wr.buf)
	if n > c {
		err = fmt.Errorf("requested bytes (%d) exceeds capacity (%d)", n, c)
		return
	} else if n > c-l {
		if err = wr.flush(); err != nil {
			return
		}
	}
	l = len(wr.buf)
	wr.buf = wr.buf[:l+n]
	buf = wr.buf[l : l+n]
	return
}

// flush writes buffered data into the journal file.
func (wr *journalWriter) flush() (err error) {
	if _, err = wr.file.WriteAt(wr.buf, wr.off); err != nil {
		return err
	}
	wr.off += int64(len(wr.buf))
	wr.buf = wr.buf[:0]
	return
}

// maybeFlush flushes buffered data, if any exists.
func (wr *journalWriter) maybeFlush() (err error) {
	wr.lock.RLock()
	empty := len(wr.buf) == 0
	wr.lock.RUnlock()
	if empty {
		return
	}
	wr.lock.Lock()
	defer wr.lock.Unlock()
	return wr.flush()
}

// snapshot returns an io.Reader with a consistent view of
// the current state of the journal file.
func (wr *journalWriter) snapshot() (io.Reader, int64, error) {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	if err := wr.flush(); err != nil {
		return nil, 0, err
	}
	// open a new file descriptor with an
	// independent lifecycle from |wr.file|
	f, err := os.Open(wr.path)
	if err != nil {
		return nil, 0, err
	}
	return io.LimitReader(f, wr.off), wr.off, nil
}

func (wr *journalWriter) offset() int64 {
	return wr.off + int64(len(wr.buf))
}

func (wr *journalWriter) currentSize() int64 {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	return wr.offset()
}

func (wr *journalWriter) uncompressedSize() uint64 {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	return wr.uncmpSz
}

func (wr *journalWriter) recordCount() uint32 {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	return wr.index.count()
}

func (wr *journalWriter) Close() (err error) {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	if err = wr.flush(); err != nil {
		return err
	}
	if cerr := wr.file.Sync(); cerr != nil {
		err = cerr
	}
	if cerr := wr.file.Close(); cerr != nil {
		err = cerr
	}
	return
}

type rangeIndex struct {
	novel  map[addr]Range
	cached map[addr]Range
	f      *os.File
}

func newRangeIndex() rangeIndex {
	return rangeIndex{
		novel:  make(map[addr]Range),
		cached: make(map[addr]Range),
	}
}

func (idx rangeIndex) get(a addr) (rng Range, ok bool) {
	rng, ok = idx.novel[a]
	if !ok {
		rng, ok = idx.cached[a]
	}
	return
}

func (idx rangeIndex) put(a addr, rng Range) {
	idx.novel[a] = rng
}

func (idx rangeIndex) iter(cb func(addr, Range)) {
	for a, r := range idx.novel {
		cb(a, r)
	}
	for a, r := range idx.cached {
		cb(a, r)
	}
}

func (idx rangeIndex) count() uint32 {
	return uint32(len(idx.novel) + len(idx.cached))
}

func (idx rangeIndex) atCapacity() bool {
	return len(idx.novel) > journalIndexThreshold
}

func (idx rangeIndex) persistNovel(root hash.Hash, off int64) error {
	for a, r := range idx.novel {
		idx.cached[a] = r
	}
	idx.novel = make(map[addr]Range)
	// todo: actually persist index
	return nil
}
