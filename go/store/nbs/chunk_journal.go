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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/golang/snappy"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

var chunkJournalAddr = addr{
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
}

const (
	chunkJournalName = "nbs_chunk_journal"
)

func newChunkJournal(ctx context.Context, mc manifestContents) (chunkJournal, error) {
	panic("unimplemented")
}

type chunkJournal struct {
	manifest manifestContents

	file   *os.File
	offset int64
	dir    string

	mu             sync.RWMutex
	lookups        map[addr]jrecordLookup
	uncompressedSz uint64
	compressedSz   uint64
}

var _ tablePersister = &chunkJournal{}
var _ manifest = &chunkJournal{}

type jrecordLookup struct {
	offset int64
	length uint32
}

// Persist implements tablePersister.
func (j *chunkJournal) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	// todo(andy): what does contention look like here?
	j.mu.Lock()
	defer j.mu.Unlock()

	if haver != nil {
		sort.Sort(hasRecordByPrefix(mt.order)) // hasMany() requires addresses to be sorted.
		if _, err := haver.hasMany(mt.order); err != nil {
			return nil, err
		}
		sort.Sort(hasRecordByOrder(mt.order)) // restore "insertion" order for write
	}

	// todo: allocate based on absent novel chunks
	buf := make([]byte, maxTableSize(uint64(len(mt.order)), mt.totalData))
	for _, record := range mt.order {
		if record.has {
			continue
		}
		c := mt.chunks[*record.a]
		n := writeJournalRecord(buf, chunkKind, *record.a, c)
		j.lookups[*record.a] = jrecordLookup{offset: j.offset, length: n}
		j.offset += int64(n)
		j.compressedSz += uint64(n)
		j.uncompressedSz += uint64(len(c))
		buf = buf[n:]
	}

	n, err := j.file.Write(buf)
	if err != nil {
		return nil, err
	} else if n < len(buf) {
		return nil, fmt.Errorf("incomplete write (%d < %d)", n, len(buf))
	}
	return chunkJournalSource{journal: j}, nil
}

// ConjoinAll implements tablePersister.
func (j *chunkJournal) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	for _, s := range sources {
		if _, ok := s.(chunkJournalSource); !ok {
			panic("expected chunkSource to be chunkJournalSource")
		}
	}
	return chunkJournalSource{journal: j}, nil
}

// Open implements tablePersister.
func (j *chunkJournal) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	d.PanicIfFalse(name == chunkJournalAddr)
	d.PanicIfFalse(j.file == nil && len(j.lookups) == 0)
	d.PanicIfFalse(j.offset == 0)
	d.PanicIfFalse(j.uncompressedSz == 0)
	d.PanicIfFalse(j.compressedSz == 0)

	var err error
	j.file, err = os.Open(filepath.Join(j.dir, name.String()))
	if err != nil {
		return nil, err
	}
	j.lookups = make(map[addr]jrecordLookup, chunkCount)

	j.offset, err = processRecords(ctx, j.file, func(off int64, rec jrecord) error {
		j.lookups[rec.address] = jrecordLookup{offset: off, length: rec.length}
		j.compressedSz += uint64(rec.length)
		// todo(andy): uncompressed size
		return nil
	})
	if err != nil {
		return nil, err
	}
	return chunkJournalSource{journal: j}, nil
}

// PruneTableFiles implements tablePersister.
func (j *chunkJournal) PruneTableFiles(ctx context.Context, contents manifestContents) error {
	panic("unimplemented")
}

// Update implements manifest.
func (j *chunkJournal) Update(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := writeHook(); err != nil {
		return manifestContents{}, err
	}

	curr := j.manifest
	if curr.lock != lastLock {
		return curr, nil // stale
	}
	if curr.manifestVers != next.manifestVers ||
		curr.nbfVers != next.nbfVers ||
		curr.gcGen != next.gcGen {
		panic("manifest metadata does not match")
	}
	j.manifest = next
	return j.manifest, nil
}

// Name implements manifest.
func (j *chunkJournal) Name() string {
	return chunkJournalName
}

// ParseIfExists implements manifest.
func (j *chunkJournal) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (bool, manifestContents, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := readHook(); err != nil {
		return false, manifestContents{}, err
	}
	return true, j.manifest, nil
}

func (j *chunkJournal) getLookup(h addr) (l jrecordLookup, ok bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	l, ok = j.lookups[h]
	return
}

func (j *chunkJournal) readJournalRecord(l jrecordLookup) (jrecord, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	b := make([]byte, l.length)
	if _, err := j.file.ReadAt(b, l.offset); err != nil {
		return jrecord{}, err
	}
	return readJournalRecord(b), nil
}

func (j *chunkJournal) count() uint32 {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return uint32(len(j.lookups))
}

func (j *chunkJournal) uncompressedSize() uint64 {
	return atomic.LoadUint64(&j.uncompressedSz)
}

type chunkJournalSource struct {
	journal *chunkJournal
}

var _ chunkSource = chunkJournalSource{}

func (s chunkJournalSource) has(h addr) (bool, error) {
	_, ok := s.journal.getLookup(h)
	return ok, nil
}

func (s chunkJournalSource) hasMany(addrs []hasRecord) (missing bool, err error) {
	for i := range addrs {
		a := addrs[i].a
		if _, ok := s.journal.getLookup(*a); ok {
			addrs[i].has = true
		} else {
			missing = true
		}
	}
	return
}

func (s chunkJournalSource) getCompressed(ctx context.Context, h addr, stats *Stats) (CompressedChunk, error) {
	e, ok := s.journal.getLookup(h)
	if !ok {
		return CompressedChunk{}, nil
	}
	rec, err := s.journal.readJournalRecord(e)
	if err != nil {
		return CompressedChunk{}, err
	}
	return NewCompressedChunk(hash.Hash(h), rec.cmpData)
}

func (s chunkJournalSource) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	cc, err := s.getCompressed(ctx, h, stats)
	if err != nil {
		return nil, err
	} else if cc.IsEmpty() {
		return nil, nil
	}
	ch, err := cc.ToChunk()
	if err != nil {
		return nil, err
	}
	return ch.Data(), nil
}

func (s chunkJournalSource) getMany(ctx context.Context, _ *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (bool, error) {
	var remaining bool
	// todo: read planning
	for i := range reqs {
		data, err := s.get(ctx, *reqs[i].a, stats)
		if err != nil {
			return false, err
		} else if data != nil {
			ch := chunks.NewChunkWithHash(hash.Hash(*reqs[i].a), data)
			found(ctx, &ch)
		} else {
			remaining = true
		}
	}
	return remaining, nil
}

func (s chunkJournalSource) getManyCompressed(ctx context.Context, _ *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error) {
	var remaining bool
	// todo: read planning
	for i := range reqs {
		cc, err := s.getCompressed(ctx, *reqs[i].a, stats)
		if err != nil {
			return false, err
		} else if cc.IsEmpty() {
			remaining = true
		} else {
			found(ctx, cc)
		}
	}
	return remaining, nil
}

func (s chunkJournalSource) count() (uint32, error) {
	return s.journal.count(), nil
}

func (s chunkJournalSource) uncompressedLen() (uint64, error) {
	return s.journal.uncompressedSize(), nil
}

func (s chunkJournalSource) hash() (addr, error) {
	// todo(andy): |hash()| belongs to the chunkSource interface and exists
	//  due to the duality between chunkSources & table files. chunkJournal
	//  seeks to create many chunkSources that depend on a single file.
	//  |hash()| in particular is relevant to the implementation of Rebase().
	return chunkJournalAddr, nil
}

// reader implements chunkSource.
func (s chunkJournalSource) reader(context.Context) (io.Reader, error) {
	// todo(andy): |reader()| belongs to the chunkSource interface and exists
	//  due to the duality between chunkSources & table files. chunkJournal
	//  seeks to create many chunkSources that depend on a single file.
	//  |reader()| in particular is relevant to conjoin implementations.
	panic("unimplemented")
}

// size implements chunkSource.
// size returns the total size of the chunkSource: chunks, index, and footer
func (s chunkJournalSource) size() (uint64, error) {
	panic("unimplemented")
}

// index implements chunkSource.
func (s chunkJournalSource) index() (tableIndex, error) {
	panic("unimplemented")
}

func (s chunkJournalSource) clone() (chunkSource, error) {
	panic("unimplemented")
}

func (s chunkJournalSource) close() error {
	return nil
}

type jrecordKind uint8

const (
	rootHashKind jrecordKind = 1
	chunkKind    jrecordKind = 2

	minRecordSize = uint32Size + addrSize + checksumSize
	maxRecordSize = 128 * 1024 // todo(andy): less arbitrary
)

type jrecord struct {
	length   uint32
	kind     jrecordKind
	address  addr
	cmpData  []byte
	checksum uint32
}

// todo(andy): manifest updates
func readJournalRecord(buf []byte) (rec jrecord) {
	rec.length = readUint(buf)
	buf = buf[uint32Size:]
	rec.kind = jrecordKind(buf[0])
	buf = buf[1:]
	copy(rec.address[:], buf)
	buf = buf[addrSize:]
	rec.cmpData = buf[:len(buf)-checksumSize]
	tail := buf[len(buf)-checksumSize:]
	rec.checksum = readUint(tail)
	return
}

func writeJournalRecord(buf []byte, kind jrecordKind, a addr, data []byte) uint32 {
	// |length| written last
	var n uint32 = uint32Size
	buf[n] = byte(kind)
	n += 1
	copy(buf[n:], a[:])
	n += addrSize
	compressed := snappy.Encode(buf[n:], data) // todo: zstd
	n += uint32(len(compressed))
	// todo(andy): checksum |compressed| or everything?
	writeUint(buf[n:], crc(compressed))
	n += checksumSize
	writeUint(buf[:uint32Size], n)
	return n
}

func processRecords(ctx context.Context, r io.Reader, cb func(o int64, r jrecord) error) (off int64, err error) {
	var buf []byte
	rdr := bufio.NewReaderSize(r, 1024*1024)

	for {
		// peek to read next record size
		if buf, err = rdr.Peek(uint32Size); err != nil {
			break
		}

		l := readUint(buf)
		if l < minRecordSize || l > maxRecordSize {
			break
		}
		if buf, err = rdr.Peek(int(l)); err != nil {
			break
		}

		rec := readJournalRecord(buf)
		if crc(rec.cmpData) != rec.checksum {
			break // stop if we can't checksum |rec|
		}

		if err = cb(off, rec); err != nil {
			break
		}

		// read from |rdr| into |buf| to advance |rdr| state
		if _, err = io.ReadFull(rdr, buf); err != nil {
			break
		}
		off += int64(len(buf))
	}
	if err == io.EOF {
		err = nil
	}
	return
}

func readUint(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func writeUint(buf []byte, u uint32) {
	binary.BigEndian.PutUint32(buf, u)
}
