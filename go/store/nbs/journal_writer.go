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
	"bufio"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	// chunkJournalFileSize is the size we initialize the journal file to when it is first created. We
	// create a 16KB block of zero-initialized data and then sync the file to the first byte. We do this
	// to ensure that we can write to the journal file and that we have some space for initial records.
	// This probably isn't strictly necessary, but it also doesn't hurt.
	chunkJournalFileSize = 16 * 1024

	// journalWriterBuffSize is the size of the statically allocated buffer where journal records are
	// built before being written to the journal file on disk. There is not a hard limit on the size
	// of records – specifically, some newer data chunking formats (i.e. optimized JSON storage) can
	// produce chunks (and therefore chunk records) that are megabytes in size. The current limit of
	// 5MB should be large enough to cover all but the most extreme cases.
	journalWriterBuffSize = 5 * 1024 * 1024

	chunkJournalAddr = chunks.JournalFileID

	journalIndexFileName = "journal.idx"

	// journalIndexDefaultMaxNovel determines how often we flush
	// records qto the out-of-band journal index file.
	journalIndexDefaultMaxNovel = 16384

	// journalMaybeSyncThreshold determines how much un-syncd written data
	// can be outstanding to the journal before we will sync it.
	journalMaybeSyncThreshold = 64 * 1024 * 1024
)

var (
	journalAddr = hash.Parse(chunkJournalAddr)
)

func isJournalAddr(h hash.Hash) bool {
	return h == journalAddr
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

	// Open the journal file and initialize it with 16KB of zero bytes. This is intended to
	// ensure that we can write to the journal and to allocate space for the first set of
	// records, but probably isn't strictly necessary.
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
	// off indicates the last position that has been written to the journal buffer
	off     int64
	indexed int64
	path    string
	uncmpSz uint64

	unsyncd     uint64
	currentRoot hash.Hash

	ranges      rangeIndex
	index       *os.File
	indexWriter *bufio.Writer
	batchCrc    uint32
	maxNovel    int

	lock sync.RWMutex
}

var _ io.Closer = &journalWriter{}

// bootstrapJournal reads in records from the journal file and the journal index file, initializing
// the state of the journalWriter. Root hashes read from root update records in the journal are written
// to |reflogRingBuffer|, which maintains the most recently updated roots which are used to generate the
// reflog. This function returns the most recent root hash for the journal as well as any error encountered.
// The journal index will bw truncated to the last valid batch of lookups. Lookups with offsets
// larger than the position of the last valid lookup metadata are rewritten to the index as they
// are added to the novel ranges map. If the number of novel lookups exceeds |wr.maxNovel|, we
// extend the journal index with one metadata flush before existing this function to save indexing
// progress.
func (wr *journalWriter) bootstrapJournal(ctx context.Context, reflogRingBuffer *reflogRingBuffer) (last hash.Hash, err error) {
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
	wr.indexWriter = bufio.NewWriterSize(wr.index, journalIndexDefaultMaxNovel)

	if ok {
		var info os.FileInfo
		if info, err = wr.index.Stat(); err != nil {
			return hash.Hash{}, err
		}

		// initialize range index with enough capacity to
		// avoid rehashing during bootstrapping
		cnt := estimateRangeCount(info)
		wr.ranges.cached = make(map[addr16]Range, cnt)

		eg, ectx := errgroup.WithContext(ctx)
		ch := make(chan []lookup, 4)

		// process the indexed portion of the journal
		var safeIndexOffset int64
		var prev int64

		eg.Go(func() error {
			defer close(ch)
			safeIndexOffset, err = processIndexRecords(bufio.NewReader(wr.index), info.Size(), func(m lookupMeta, batch []lookup, batchChecksum uint32) error {
				if m.checkSum != batchChecksum {
					return fmt.Errorf("invalid index checksum (%d != %d)", batchChecksum, m.checkSum)
				}

				if m.batchStart != prev {
					return fmt.Errorf("index records do not cover contiguous region (%d != %d)", m.batchStart, prev)
				}
				prev = m.batchEnd

				// |r.end| is expected to point to a root hash record in |wr.journal|
				// containing a hash equal to |r.lastRoot|, validate this here
				if h, err := peekRootHashAt(wr.journal, int64(m.batchEnd)); err != nil {
					return err
				} else if h != m.latestHash {
					return fmt.Errorf("invalid index record hash (%s != %s)", h.String(), m.latestHash.String())
				}

				select {
				case <-ectx.Done():
					return ectx.Err()
				case ch <- batch:
					// record a high-water-mark for the indexed portion of the journal
					wr.indexed = int64(m.batchEnd)
				}
				return nil
			})
			return err
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
			if cerr := wr.corruptIndexRecovery(); cerr != nil {
				err = fmt.Errorf("error recovering corrupted chunk journal index: %s", err.Error())
			}
			return hash.Hash{}, err
		}

		// rewind index to last safe point. Note that |safeIndexOffset| refers
		// to a location in the index file, while |wr.indexed| refers to a position
		// in the journal file.
		if err := wr.truncateIndex(safeIndexOffset); err != nil {
			return hash.Hash{}, err
		}
		wr.ranges = wr.ranges.flatten(ctx)
	}

	var lastOffset int64

	// process the non-indexed portion of the journal starting at |wr.indexed|,
	// at minimum the non-indexed portion will include a root hash record.
	// Index lookups are added to the ongoing batch to re-synchronize.
	wr.off, err = processJournalRecords(ctx, wr.journal, wr.indexed, func(o int64, r journalRec) error {
		switch r.kind {
		case chunkJournalRecKind:
			rng := Range{
				Offset: uint64(o) + uint64(r.payloadOffset()),
				Length: uint32(len(r.payload)),
			}
			wr.ranges.put(r.address, rng)
			wr.uncmpSz += r.uncompressedPayloadSize()

			a := toAddr16(r.address)
			if err := writeIndexLookup(wr.indexWriter, lookup{a: a, r: rng}); err != nil {
				return err
			}
			wr.batchCrc = crc32.Update(wr.batchCrc, crcTable, a[:])

		case rootHashJournalRecKind:
			lastOffset = o
			last = hash.Hash(r.address)
			if !reflogDisabled && reflogRingBuffer != nil {
				reflogRingBuffer.Push(reflogRootHashEntry{
					root:      r.address.String(),
					timestamp: r.timestamp,
				})
			}

		default:
			return fmt.Errorf("unknown journal record kind (%d)", r.kind)
		}
		return nil
	})
	if err != nil {
		return hash.Hash{}, err
	}

	if wr.ranges.novelCount() > wr.maxNovel {
		// save bootstrap progress
		if err := wr.flushIndexRecord(ctx, last, lastOffset); err != nil {
			return hash.Hash{}, err
		}
	}

	wr.currentRoot = last

	return
}

// corruptIndexRecovery handles a corrupted or malformed journal index by truncating
// the index file and restarting the journal bootstrapping process without an index.
// todo: make backup file?
func (wr *journalWriter) corruptIndexRecovery() error {
	if err := wr.truncateIndex(0); err != nil {
		return err
	}
	// reset bootstrapping state
	wr.off, wr.indexed, wr.uncmpSz = 0, 0, 0
	wr.ranges = newRangeIndex()
	return nil
}

func (wr *journalWriter) truncateIndex(off int64) error {
	if _, err := wr.index.Seek(off, io.SeekStart); err != nil {
		return err
	}
	if err := wr.index.Truncate(off); err != nil {
		return err
	}
	return nil
}

// hasAddr returns true if the journal contains a chunk with addr |h|.
func (wr *journalWriter) hasAddr(h hash.Hash) (ok bool) {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	_, ok = wr.ranges.get(h)
	return
}

// getCompressedChunk reads the CompressedChunks with addr |h|.
func (wr *journalWriter) getCompressedChunk(h hash.Hash) (CompressedChunk, error) {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	r, ok := wr.ranges.get(h)
	if !ok {
		return CompressedChunk{}, nil
	}
	buf := make([]byte, r.Length)
	if _, err := wr.readAt(buf, int64(r.Offset)); err != nil {
		return CompressedChunk{}, err
	}
	return NewCompressedChunk(hash.Hash(h), buf)
}

// getCompressedChunk reads the CompressedChunks with addr |h|.
func (wr *journalWriter) getCompressedChunkAtRange(r Range, h hash.Hash) (CompressedChunk, error) {
	buf := make([]byte, r.Length)
	if _, err := wr.readAt(buf, int64(r.Offset)); err != nil {
		return CompressedChunk{}, err
	}
	return NewCompressedChunk(hash.Hash(h), buf)
}

// getRange returns a Range for the chunk with addr |h|.
func (wr *journalWriter) getRange(ctx context.Context, h hash.Hash) (rng Range, ok bool, err error) {
	// callers will use |rng| to read directly from the
	// journal file, so we must flush here
	if err = wr.maybeFlush(ctx); err != nil {
		return
	}
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	rng, ok = wr.ranges.get(h)
	return
}

// writeCompressedChunk writes |cc| to the journal.
func (wr *journalWriter) writeCompressedChunk(ctx context.Context, cc CompressedChunk) error {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	recordLen, payloadOff := chunkRecordSize(cc)
	rng := Range{
		Offset: uint64(wr.offset()) + uint64(payloadOff),
		Length: uint32(len(cc.FullCompressedChunk)),
	}
	buf, err := wr.getBytes(ctx, int(recordLen))
	if err != nil {
		return err
	}
	wr.unsyncd += uint64(recordLen)
	_ = writeChunkRecord(buf, cc)
	wr.ranges.put(cc.H, rng)

	a := toAddr16(cc.H)
	if err := writeIndexLookup(wr.indexWriter, lookup{a: a, r: rng}); err != nil {
		return err
	}
	wr.batchCrc = crc32.Update(wr.batchCrc, crcTable, a[:])

	// To fulfill our durability guarantees, we technically only need to
	// file.Sync() the journal when we commit a new root chunk. However,
	// allowing an unbounded amount of unflushed dirty pages to accumulate
	// in the OS's page cache makes it possible for small writes which come
	// along during a large non-committing write to block on flushing all
	// of the unflushed data. To minimize interference from large
	// non-committing writes, we cap the amount of unflushed data here.
	//
	// We go through |commitRootHash|, instead of directly |Sync()|ing the
	// file, because we also have accumulating delayed work in the form of
	// journal index records which may need to be serialized and flushed.
	// Assumptions in journal bootstrapping and the contents of the journal
	// index require us to have a newly written root hash record anytime we
	// write index records out. It's perfectly fine to reuse the current
	// root hash, and this will also take care of the |Sync|.
	if wr.unsyncd > journalMaybeSyncThreshold && !wr.currentRoot.IsEmpty() {
		return wr.commitRootHashUnlocked(ctx, wr.currentRoot)
	}

	return nil
}

// commitRootHash commits |root| to the journal and syncs the file to disk.
func (wr *journalWriter) commitRootHash(ctx context.Context, root hash.Hash) error {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	return wr.commitRootHashUnlocked(ctx, root)
}

func (wr *journalWriter) size() int64 {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	return wr.off
}

func (wr *journalWriter) commitRootHashUnlocked(ctx context.Context, root hash.Hash) error {
	defer trace.StartRegion(ctx, "commit-root").End()

	buf, err := wr.getBytes(ctx, rootHashRecordSize())
	if err != nil {
		return err
	}
	wr.currentRoot = root
	n := writeRootHashRecord(buf, root)
	if err = wr.flush(ctx); err != nil {
		return err
	}
	func() {
		defer trace.StartRegion(ctx, "sync").End()

		err = wr.journal.Sync()
	}()
	if err != nil {
		return err
	}

	wr.unsyncd = 0
	if wr.ranges.novelCount() > wr.maxNovel {
		o := wr.offset() - int64(n) // pre-commit journal offset
		if err := wr.flushIndexRecord(ctx, root, o); err != nil {
			return err
		}
	}
	return nil
}

// flushIndexRecord writes metadata for a range of index lookups to the
// out-of-band journal index file. Index records accelerate journal
// bootstrapping by reducing the amount of the journal that must be processed.
func (wr *journalWriter) flushIndexRecord(ctx context.Context, root hash.Hash, end int64) (err error) {
	defer trace.StartRegion(ctx, "flushIndexRecord").End()
	if err := writeJournalIndexMeta(wr.indexWriter, root, wr.indexed, end, wr.batchCrc); err != nil {
		return err
	}
	wr.batchCrc = 0
	wr.ranges = wr.ranges.flatten(ctx)
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
func (wr *journalWriter) getBytes(ctx context.Context, n int) (buf []byte, err error) {
	c, l := cap(wr.buf), len(wr.buf)
	if n > c {
		err = fmt.Errorf("requested bytes (%d) exceeds capacity (%d)", n, c)
		return
	} else if n > c-l {
		if err = wr.flush(ctx); err != nil {
			return
		}
	}
	l = len(wr.buf)
	wr.buf = wr.buf[:l+n]
	buf = wr.buf[l : l+n]
	return
}

// flush writes buffered data into the journal file.
func (wr *journalWriter) flush(ctx context.Context) (err error) {
	defer trace.StartRegion(ctx, "flush journal").End()
	if _, err = wr.journal.WriteAt(wr.buf, wr.off); err != nil {
		return err
	}
	wr.off += int64(len(wr.buf))
	wr.buf = wr.buf[:0]
	return
}

// maybeFlush flushes buffered data, if any exists.
func (wr *journalWriter) maybeFlush(ctx context.Context) (err error) {
	wr.lock.RLock()
	empty := len(wr.buf) == 0
	wr.lock.RUnlock()
	if empty {
		return
	}
	wr.lock.Lock()
	defer wr.lock.Unlock()
	return wr.flush(ctx)
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
func (wr *journalWriter) snapshot(ctx context.Context) (io.ReadCloser, int64, error) {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	if err := wr.flush(ctx); err != nil {
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
	wr.lock.Lock()
	defer wr.lock.Unlock()

	if wr.journal == nil {
		logrus.Warnf("journal writer has already been closed (%s)", wr.path)
		return nil
	}

	if err = wr.flush(context.Background()); err != nil {
		return err
	}
	if wr.index != nil {
		_ = wr.indexWriter.Flush()
		_ = wr.index.Close()
	}
	if cerr := wr.journal.Sync(); cerr != nil {
		err = cerr
	}
	if cerr := wr.journal.Close(); cerr != nil {
		err = cerr
	} else {
		// Nil out the journal after the file has been closed, so that it's obvious it's been closed
		wr.journal = nil
	}

	return err
}

// A rangeIndex maps chunk addresses to read Ranges in the chunk journal file.
type rangeIndex struct {
	// novel Ranges represent most recent chunks written to
	// the journal. These Ranges have not yet been written to
	// a journal index record.
	novel map[hash.Hash]Range

	// cached Ranges are bootstrapped from an out-of-band journal
	// index file. To save memory, these Ranges are keyed by a 16-byte
	// prefix of their addr which is assumed to be globally unique
	cached map[addr16]Range
}

type addr16 [16]byte

func toAddr16(full hash.Hash) (prefix addr16) {
	copy(prefix[:], full[:])
	return
}

func newRangeIndex() rangeIndex {
	return rangeIndex{
		novel:  make(map[hash.Hash]Range, journalIndexDefaultMaxNovel),
		cached: make(map[addr16]Range),
	}
}

func estimateRangeCount(info os.FileInfo) uint32 {
	return uint32(info.Size()/32) + journalIndexDefaultMaxNovel
}

func (idx rangeIndex) get(h hash.Hash) (rng Range, ok bool) {
	rng, ok = idx.novel[h]
	if !ok {
		rng, ok = idx.cached[toAddr16(h)]
	}
	return
}

func (idx rangeIndex) put(h hash.Hash, rng Range) {
	idx.novel[h] = rng
}

func (idx rangeIndex) putCached(a addr16, rng Range) {
	idx.cached[a] = rng
}

func (idx rangeIndex) count() uint32 {
	return uint32(len(idx.novel) + len(idx.cached))
}

func (idx rangeIndex) novelCount() int {
	return len(idx.novel)
}

func (idx rangeIndex) novelLookups() (lookups []lookup) {
	lookups = make([]lookup, 0, len(idx.novel))
	for a, r := range idx.novel {
		lookups = append(lookups, lookup{a: toAddr16(a), r: r})
	}
	return
}

func (idx rangeIndex) flatten(ctx context.Context) rangeIndex {
	defer trace.StartRegion(ctx, "flatten journal index").End()
	trace.Logf(ctx, "map index cached count", "%d", len(idx.cached))
	trace.Logf(ctx, "map index novel count", "%d", len(idx.novel))
	for a, r := range idx.novel {
		idx.cached[toAddr16(a)] = r
	}
	idx.novel = make(map[hash.Hash]Range, journalIndexDefaultMaxNovel)
	return idx
}
