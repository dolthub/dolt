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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

func init() {
	if os.Getenv("DOLT_ENABLE_CHUNK_JOURNAL") != "" {
		chunkJournalFeatureFlag = true
	}
}

// var chunkJournalFeatureFlag = false
var chunkJournalFeatureFlag = true

const (
	chunkJournalName = "nbs_chunk_journal"
)

var chunkJournalAddr = addr{
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
}

func newChunkJournal(dir string, m manifest) *chunkJournal {
	return &chunkJournal{
		dir:     dir,
		lookups: make(map[addr]jrecordLookup),
		backing: m,
	}
}

type chunkJournal struct {
	file   *os.File
	offset int64
	dir    string

	lookups        map[addr]jrecordLookup
	uncompressedSz uint64
	compressedSz   uint64

	// todo(andy): need to flush |manifest| to |backing|
	//  on graceful shutdown, but we have no Close()
	manifest manifestContents
	backing  manifest
}

var _ tablePersister = &chunkJournal{}
var _ manifest = &chunkJournal{}

type jrecordLookup struct {
	offset int64
	length uint32
}

// Persist implements tablePersister.
func (j *chunkJournal) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	if haver != nil {
		sort.Sort(hasRecordByPrefix(mt.order)) // hasMany() requires addresses to be sorted.
		if _, err := haver.hasMany(mt.order); err != nil {
			return nil, err
		}
		sort.Sort(hasRecordByOrder(mt.order)) // restore "insertion" order for write
	}

	buf := make([]byte, maxTableSize(uint64(len(mt.order)), mt.totalData))

	var off uint32
	for _, record := range mt.order {
		if record.has {
			continue
		}
		c := chunks.NewChunkWithHash(hash.Hash(*record.a), mt.chunks[*record.a])
		cc := ChunkToCompressedChunk(c)
		n := writeChunkRecord(buf[off:], cc)
		off += n

		j.lookups[*record.a] = jrecordLookup{offset: j.offset, length: n}
		j.compressedSz += uint64(cc.CompressedSize())
		j.uncompressedSz += uint64(c.Size())
		j.offset += int64(n)
	}

	if err := j.openJournal(); err != nil {
		return nil, err
	}
	if err := j.flushBuffer(buf[:off]); err != nil {
		return nil, err
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
	d.PanicIfFalse(name == chunkJournalAddr)
	d.PanicIfFalse(len(j.lookups) == 0)
	d.PanicIfFalse(j.offset == 0)
	d.PanicIfFalse(j.uncompressedSz == 0)
	d.PanicIfFalse(j.compressedSz == 0)

	err := j.openJournal()
	if err != nil {
		return nil, err
	}

	j.offset, _, err = processRecords(ctx, j.file, func(off int64, rec jrecord) error {
		switch rec.kind {
		case chunkKind:
			// todo(andy): uncompressed size
			j.lookups[rec.address] = jrecordLookup{offset: off, length: rec.length}
			j.compressedSz += uint64(rec.length)

		case rootHashKind:
			j.manifest.root = hash.Hash(rec.address)

		default:
			return fmt.Errorf("unknown journal record kind (%d)", rec.kind)
		}
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
	// if out manifest is empty, pass the update to the underlying manifest
	if emptyAddr(j.manifest.lock) {
		mc, err := j.backing.Update(ctx, lastLock, next, stats, writeHook)
		j.manifest = mc
		return mc, err
	}

	if writeHook != nil {
		if err := writeHook(); err != nil {
			return manifestContents{}, err
		}
	}

	if err := j.openJournal(); err != nil {
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

	buf := make([]byte, rootHashRecordSize)
	writeRootHashRecord(buf, addr(next.root))
	if err := j.flushBuffer(buf); err != nil {
		return curr, err
	}
	j.manifest = next
	return j.manifest, nil
}

// Name implements manifest.
func (j *chunkJournal) Name() string {
	return chunkJournalName
}

// ParseIfExists implements manifest.
func (j *chunkJournal) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (ok bool, mc manifestContents, err error) {
	// check if we've seen the manifest
	ok = !emptyAddr(j.manifest.lock)

	if !ok {
		ok, mc, err = j.backing.ParseIfExists(ctx, stats, readHook)
		j.manifest = mc
		return
	}

	if readHook != nil {
		if err := readHook(); err != nil {
			return false, manifestContents{}, err
		}
	}
	return ok, j.manifest, nil
}

func (j *chunkJournal) openJournal() (err error) {
	if j.file != nil {
		return
	}
	p := filepath.Join(j.dir, chunkJournalName)
	if _, err = os.Stat(p); errors.Is(err, os.ErrNotExist) {
		j.file, err = os.Create(p)
	} else {
		j.file, err = os.Open(p)
	}
	return
}

func (j *chunkJournal) flushBuffer(buf []byte) error {
	// todo(andy): pad |buf| to page boundary
	n, err := j.file.Write(buf)
	if err != nil {
		return err
	} else if n < len(buf) {
		return fmt.Errorf("incomplete write (%d < %d)", n, len(buf))
	}
	return j.file.Sync()
}

func (j *chunkJournal) getLookup(h addr) (l jrecordLookup, ok bool) {
	l, ok = j.lookups[h]
	return
}

func (j *chunkJournal) readJournalRecord(l jrecordLookup) (jrecord, error) {
	b := make([]byte, l.length)
	if _, err := j.file.ReadAt(b, l.offset); err != nil {
		return jrecord{}, err
	}
	return readJournalRecord(b), nil
}

func (j *chunkJournal) count() uint32 {
	return uint32(len(j.lookups))
}

func (j *chunkJournal) uncompressedSize() uint64 {
	return atomic.LoadUint64(&j.uncompressedSz)
}

func emptyAddr(a addr) bool {
	var b addr
	return a == b
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
	} else if h != rec.address {
		err = fmt.Errorf("unexpected chunk address (%s != %s)", h.String(), rec.address.String())
		return CompressedChunk{}, err
	}
	return NewCompressedChunk(hash.Hash(h), rec.payload)
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

type jrecord struct {
	length   uint32
	kind     jrecordKind
	address  addr
	payload  []byte
	checksum uint32
}

const (
	chunkKind    jrecordKind = 2
	rootHashKind jrecordKind = 1
	unknownKind  jrecordKind = 0

	recKindSz = 1
	recLenSz  = uint32Size
	recMinSz  = recLenSz + recKindSz + addrSize + checksumSize
	recMaxSz  = 128 * 1024 // todo(andy): less arbitrary

	rootHashRecordSize = recMinSz
)

func writeChunkRecord(buf []byte, c CompressedChunk) (n uint32) {
	l := uint32(len(c.FullCompressedChunk)) + recMinSz
	writeUint(buf[:recLenSz], l)
	n += recLenSz
	buf[n] = byte(chunkKind)
	n += recKindSz
	copy(buf[n:], c.H[:])
	n += addrSize
	copy(buf[n:], c.FullCompressedChunk)
	n += uint32(len(c.FullCompressedChunk))
	writeUint(buf[n:], crc(buf[:n]))
	n += checksumSize
	return
}

func writeRootHashRecord(buf []byte, root addr) (n uint32) {
	writeUint(buf[:recLenSz], rootHashRecordSize)
	n += recLenSz
	buf[n] = byte(rootHashKind)
	n += recKindSz
	copy(buf[n:], root[:])
	n += addrSize
	writeUint(buf[n:], crc(buf[:n]))
	n += checksumSize
	return
}

func readJournalRecord(buf []byte) (rec jrecord) {
	rec.length = readUint(buf)
	buf = buf[recLenSz:]
	rec.kind = jrecordKind(buf[0])
	buf = buf[recKindSz:]
	copy(rec.address[:], buf)
	buf = buf[addrSize:]
	rec.payload = buf[:len(buf)-checksumSize]
	rec.checksum = readUint(buf[len(buf)-checksumSize:])
	return
}

func safeReadJournalRecord(buf []byte) (jrecord, bool) {
	o := len(buf) - checksumSize
	if crc(buf[:o]) != readUint(buf[o:]) {
		return jrecord{}, false
	}

	rec := readJournalRecord(buf)
	switch rec.kind {
	case rootHashKind:
		return rec, true

	case chunkKind:
		_, err := NewCompressedChunk(hash.Hash(rec.address), rec.payload)
		if err != nil {
			return jrecord{}, false
		}
		return rec, true

	default:
		return jrecord{}, false
	}
}

func processRecords(ctx context.Context, r io.Reader, cb func(o int64, r jrecord) error) (off, cnt int64, err error) {
	var buf []byte
	rdr := bufio.NewReaderSize(r, 1024*1024)

	for {
		if cnt >= 1024 {
			fmt.Println("")
		}

		// peek to read next record size
		if buf, err = rdr.Peek(uint32Size); err != nil {
			break
		}

		l := readUint(buf)
		if l < recMinSz || l > recMaxSz {
			break
		} else if buf, err = rdr.Peek(int(l)); err != nil {
			break
		}

		rec, ok := safeReadJournalRecord(buf)
		if !ok {
			break // stop if we can't validate |rec|
		}

		if err = cb(off, rec); err != nil {
			break
		}

		// advance |rdr| state by |l| bytes
		if _, err = io.ReadFull(rdr, buf); err != nil {
			break
		}
		off += int64(len(buf))
		cnt++
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
