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
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dolthub/swiss"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	chunkJournalFileSize = 16 * 1024

	// todo(andy): buffer must be able to hold an entire record,
	//   but we don't have a hard limit on record size right now
	journalWriterBuffSize = 1024 * 1024

	chunkJournalAddr = chunks.JournalFileID

	journalIndexFileName = "journal.idx"

	// journalIndexDefaultMaxNovel determines how often we flush
	// records qto the out-of-band journal index file.
	journalIndexDefaultMaxNovel = 16384
)

var (
	journalAddr = addr(hash.Parse(chunkJournalAddr))
)

func isJournalAddr(a addr) bool {
	return a == journalAddr
}

func fileExists(path string) (bool, error) {
	var err error
	if path, err = filepath.Abs(path); err != nil {
		return false, err
	}

	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if info.IsDir() {
		return true, fmt.Errorf("expected file %s, found directory", path)
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
		buf:     make([]byte, 0, journalWriterBuffSize),
		journal: f,
		path:    path,
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
		buf:     make([]byte, 0, journalWriterBuffSize),
		journal: f,
		path:    path,
	}, nil
}

func deleteJournalAndIndexFiles(ctx context.Context, path string) (err error) {
	if err = os.Remove(path); err != nil {
		return err
	}
	idxPath := filepath.Join(filepath.Dir(path), journalIndexFileName)
	return os.Remove(idxPath)
}

type journalWriter struct {
	buf []byte

	journal *os.File
	off     int64
	indexed int64
	path    string
	uncmpSz uint64

	ranges   rangeIndex
	index    *os.File
	maxNovel int

	lock sync.RWMutex
}

var _ io.Closer = &journalWriter{}

// bootstrapJournal reads in records from the journal file and the journal index file, initializing
// the state of the journalWriter. It returns the most recent root hash for the journal.
func (wr *journalWriter) bootstrapJournal(ctx context.Context) (last hash.Hash, err error) {
	wr.lock.Lock()
	defer wr.lock.Unlock()

	if wr.maxNovel == 0 {
		wr.maxNovel = journalIndexDefaultMaxNovel
	}
	wr.ranges = newRangeIndex()

	p := filepath.Join(filepath.Dir(wr.path), journalIndexFileName)
	var ok bool
	ok, err = fileExists(p)
	if err != nil {
		return
	} else if ok {
		wr.index, err = os.OpenFile(p, os.O_RDWR, 0666)
	} else {
		wr.index, err = os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0666)
	}
	if err != nil {
		return
	}

	if ok {
		var info os.FileInfo
		if info, err = wr.index.Stat(); err != nil {
			return hash.Hash{}, err
		}

		// initialize range index with enough capacity to
		// avoid rehashing during bootstrapping
		cnt := estimateRangeCount(info)
		wr.ranges.cached = swiss.NewMap[addr16, Range](cnt)

		eg, ectx := errgroup.WithContext(ctx)
		ch := make(chan []lookup, 4)

		// process the indexed portion of the journal
		eg.Go(func() error {
			defer close(ch)
			return processIndexRecords(ectx, wr.index, info.Size(), func(o int64, r indexRec) (err error) {
				switch r.kind {
				case tableIndexRecKind:
					// |r.end| is expected to point to a root hash record in |wr.journal|
					// containing a hash equal to |r.lastRoot|, validate this here
					var h hash.Hash
					if h, err = peekRootHashAt(wr.journal, int64(r.end)); err != nil {
						return err
					} else if h != r.lastRoot {
						return fmt.Errorf("invalid index record hash (%s != %s)", h.String(), r.lastRoot.String())
					}
					select {
					case <-ectx.Done():
						return ectx.Err()
					case ch <- deserializeLookups(r.payload):
						// record a high-water-mark for the indexed portion of the journal
						wr.indexed = int64(r.end)
					}
					// todo: uncompressed size
				default:
					return fmt.Errorf("unknown index record kind (%d)", r.kind)
				}
				return nil
			})
		})
		// populate range hashmap
		eg.Go(func() error {
			for {
				select {
				case <-ectx.Done():
					return nil
				case ll, ok := <-ch:
					if !ok {
						return nil
					}
					for _, l := range ll {
						wr.ranges.putCached(l.a, l.r)
					}
				}
			}
		})

		err = eg.Wait()
		if err != nil {
			err = fmt.Errorf("error bootstrapping chunk journal: %s", err.Error())
			if cerr := wr.corruptIndexRecovery(ctx); cerr != nil {
				err = fmt.Errorf("error recovering corrupted chunk journal index: %s", err.Error())
			}
			return hash.Hash{}, err
		}
		wr.ranges = wr.ranges.flatten()
	}

	// process the non-indexed portion of the journal starting at |wr.indexed|,
	// at minimum the non-indexed portion will include a root hash record
	wr.off, err = processJournalRecords(ctx, wr.journal, wr.indexed, func(o int64, r journalRec) error {
		switch r.kind {
		case chunkJournalRecKind:
			wr.ranges.put(r.address, Range{
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

// corruptIndexRecovery handles a corrupted or malformed journal index by truncating
// the index file and restarting the journal bootstrapping process without an index.
// todo: make backup file?
func (wr *journalWriter) corruptIndexRecovery(ctx context.Context) (err error) {
	if _, err = wr.index.Seek(0, io.SeekStart); err != nil {
		return
	}
	if err = wr.index.Truncate(0); err != nil {
		return
	}
	// reset bootstrapping state
	wr.off, wr.indexed, wr.uncmpSz = 0, 0, 0
	wr.ranges = newRangeIndex()
	return
}

// hasAddr returns true if the journal contains a chunk with addr |h|.
func (wr *journalWriter) hasAddr(h addr) (ok bool) {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	_, ok = wr.ranges.get(h)
	return
}

// getCompressedChunk reads the CompressedChunks with addr |h|.
func (wr *journalWriter) getCompressedChunk(h addr) (CompressedChunk, error) {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	r, ok := wr.ranges.get(h)
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
	rng, ok = wr.ranges.get(h)
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
	wr.ranges.put(addr(cc.H), rng)
	return nil
}

// commitRootHash commits |root| to the journal and syncs the file to disk.
func (wr *journalWriter) commitRootHash(root hash.Hash) error {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	buf, err := wr.getBytes(rootHashRecordSize())
	if err != nil {
		return err
	}
	n := writeRootHashRecord(buf, addr(root))
	if err = wr.flush(); err != nil {
		return err
	}
	if err = wr.journal.Sync(); err != nil {
		return err
	}
	if wr.ranges.novelCount() > wr.maxNovel {
		o := wr.offset() - int64(n) // pre-commit journal offset
		err = wr.flushIndexRecord(root, o)
	}
	return err
}

// flushIndexRecord writes a new record to the out-of-band journal index file. Index records
// accelerate journal bootstrapping by reducing the amount of the journal that must be processed.
func (wr *journalWriter) flushIndexRecord(root hash.Hash, end int64) (err error) {
	payload := serializeLookups(wr.ranges.novelLookups())
	buf := make([]byte, journalIndexRecordSize(payload))
	writeJournalIndexRecord(buf, root, uint64(wr.indexed), uint64(end), payload)
	if _, err = wr.index.Write(buf); err != nil {
		return err
	}
	wr.ranges = wr.ranges.flatten()
	// set a new high-water-mark for the indexed portion of the journal
	wr.indexed = end
	return
}

// readAt reads len(p) bytes from the journal at offset |off|.
func (wr *journalWriter) readAt(p []byte, off int64) (n int, err error) {
	var bp []byte
	if off < wr.off {
		// fill some or all of |p| from |wr.file|
		fread := int(wr.off - off)
		if len(p) > fread {
			// straddled read
			bp = p[fread:]
			p = p[:fread]
		}
		if n, err = wr.journal.ReadAt(p, off); err != nil {
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
	if _, err = wr.journal.WriteAt(wr.buf, wr.off); err != nil {
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

type journalWriterSnapshot struct {
	io.Reader
	closer func() error
}

func (s journalWriterSnapshot) Close() error {
	return s.closer()
}

// snapshot returns an io.Reader with a consistent view of
// the current state of the journal file.
func (wr *journalWriter) snapshot() (io.ReadCloser, int64, error) {
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
	return journalWriterSnapshot{
		io.LimitReader(f, wr.off),
		func() error {
			return f.Close()
		},
	}, wr.off, nil
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
	return wr.ranges.count()
}

func (wr *journalWriter) Close() (err error) {
	logrus.Errorf("entering journalWriter::Close() - File: %s", wr.journal.Name())

	wr.lock.Lock()
	defer wr.lock.Unlock()
	if err = wr.flush(); err != nil {
		return err
	}
	if wr.index != nil {
		_ = wr.index.Close()
	}
	if cerr := wr.journal.Sync(); cerr != nil {
		err = cerr
		logrus.Errorf("journalWriter::Close() - Error from wr.journal.Sync(): %s", err, err.Error())
	}

	if cerr := wr.journal.Close(); cerr != nil {
		logrus.Errorf("journalWriter::Close() - ERROR (type: %T): %s", err, err.Error())

		// TODO: If the error contains "file already closed" then just log the error, but don't return it
		//       however... it seems like we'd still have an error from wr.journal.Sync()
		if strings.Contains(cerr.Error(), "file already closed") {
			logrus.Warnf("journalWriter::Close() unable to close journal: %s ", cerr.Error())
			// TODO: enable this else block
			//} else {
			err = cerr
		}
	}
	return
}

// A rangeIndex maps chunk addresses to read Ranges in the chunk journal file.
type rangeIndex struct {
	// novel Ranges represent most recent chunks written to
	// the journal. These Ranges have not yet been writen to
	// a journal index record.
	novel *swiss.Map[addr, Range]

	// cached Ranges are bootstrapped from an out-of-band journal
	// index file. To save memory, these Ranges are keyed by a 16-byte
	// prefix of their addr which is assumed to be globally unique
	cached *swiss.Map[addr16, Range]
}

type addr16 [16]byte

func toAddr16(full addr) (prefix addr16) {
	copy(prefix[:], full[:])
	return
}

func newRangeIndex() rangeIndex {
	return rangeIndex{
		novel:  swiss.NewMap[addr, Range](journalIndexDefaultMaxNovel),
		cached: swiss.NewMap[addr16, Range](0),
	}
}

func estimateRangeCount(info os.FileInfo) uint32 {
	return uint32(info.Size()/32) + journalIndexDefaultMaxNovel
}

func (idx rangeIndex) get(a addr) (rng Range, ok bool) {
	rng, ok = idx.novel.Get(a)
	if !ok {
		rng, ok = idx.cached.Get(toAddr16(a))
	}
	return
}

func (idx rangeIndex) put(a addr, rng Range) {
	idx.novel.Put(a, rng)
}

func (idx rangeIndex) putCached(a addr, rng Range) {
	idx.cached.Put(toAddr16(a), rng)
}

func (idx rangeIndex) count() uint32 {
	return uint32(idx.novel.Count() + idx.cached.Count())
}

func (idx rangeIndex) novelCount() int {
	return idx.novel.Count()
}

func (idx rangeIndex) novelLookups() (lookups []lookup) {
	lookups = make([]lookup, 0, idx.novel.Count())
	idx.novel.Iter(func(a addr, r Range) (stop bool) {
		lookups = append(lookups, lookup{a: a, r: r})
		return
	})
	return
}

func (idx rangeIndex) flatten() rangeIndex {
	idx.novel.Iter(func(a addr, r Range) (stop bool) {
		idx.cached.Put(toAddr16(a), r)
		return
	})
	idx.novel = swiss.NewMap[addr, Range](journalIndexDefaultMaxNovel)
	return idx
}
