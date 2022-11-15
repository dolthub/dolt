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
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var chunkJournalFeatureFlag = false

func init() {
	if os.Getenv("DOLT_ENABLE_CHUNK_JOURNAL") != "" {
		chunkJournalFeatureFlag = true
	}
	os.Getpagesize()
}

const (
	chunkJournalName = "nbs_chunk_journal"
	chunkJournalAddr = "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
	chunkJournalSize = 256 * 1024 * 1024
)

var (
	journal     *chunkJournal // singleton
	journalLock = new(sync.Mutex)
	journalAddr = addr(hash.Parse(chunkJournalAddr))
)

type chunkJournal struct {
	journal *os.File
	offset  int64
	dir     string

	// todo(andy): on graceful shutdown, we need to
	//  flush |manifest| and |rootHash| to |backing|
	//  chunkJournal has no Close() method, ref count?
	rootHash hash.Hash
	manifest manifestContents
	backing  manifest

	sources map[addr]chunkSource
}

var _ tablePersister = &chunkJournal{}
var _ manifest = &chunkJournal{}

type journalChunkSource struct {
	address      addr
	journal      io.ReaderAt
	lookups      map[addr]jrecordLookup
	compressedSz uint64
}

var _ chunkSource = journalChunkSource{}

type jrecordLookup struct {
	offset int64
	length uint32
}

func newChunkJournal(ctx context.Context, dir string, m manifest) (*chunkJournal, error) {
	journalLock.Lock()
	defer journalLock.Unlock()
	if journal != nil {
		return journal, nil
	}

	file, err := openJournal(ctx, filepath.Join(dir, chunkJournalName))
	if err != nil {
		return nil, err
	}

	journal = &chunkJournal{
		journal: file,
		dir:     dir,
		backing: m,
	}

	if err = readJournal(ctx, journal); err != nil {
		return nil, err
	}

	return journal, nil
}

func openJournal(ctx context.Context, path string) (*os.File, error) {
	var create bool
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		create = true
	} else if err != nil {
		return nil, err
	} else if info.IsDir() {
		return nil, fmt.Errorf("expected file %s found directory", chunkJournalName)
	}

	if !create {
		return os.OpenFile(path, os.O_RDWR, 0666)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	// zero file the file
	k := 1024 * 1024
	b := make([]byte, k)
	for i := 0; i < chunkJournalSize; i += k {
		if _, err = f.Write(b); err != nil {
			return nil, err
		}
	}

	if o, err := f.Seek(0, 0); err != nil {
		return nil, err
	} else if o != 0 {
		return nil, fmt.Errorf("expected file offset 0, got %d", o)
	}

	return f, nil
}

func readJournal(ctx context.Context, j *chunkJournal) error {
	src := journalChunkSource{
		journal: j.journal,
		lookups: make(map[addr]jrecordLookup),
	}

	var last hash.Hash
	off, err := processRecords(ctx, j.journal, func(off int64, rec jrecord) error {
		switch rec.kind {
		case chunkKind:
			src.lookups[rec.address] = jrecordLookup{offset: off, length: rec.length}
			src.compressedSz += uint64(rec.length)
			// todo(andy): uncompressed size

		case rootHashKind:
			last = hash.Hash(rec.address)

		default:
			return fmt.Errorf("unknown journal record kind (%d)", rec.kind)
		}
		return nil
	})
	if err != nil {
		return nil
	}

	src.address = journalAddr
	j.sources = map[addr]chunkSource{src.address: src}
	j.offset = off
	j.rootHash = last

	return nil
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

	src := journalChunkSource{
		journal: j.journal,
		lookups: make(map[addr]jrecordLookup, len(mt.order)),
	}
	buf := make([]byte, maxTableSize(uint64(len(mt.order)), mt.totalData))

	var off int64
	for _, record := range mt.order {
		if record.has {
			continue
		}
		c := chunks.NewChunkWithHash(hash.Hash(*record.a), mt.chunks[*record.a])
		cc := ChunkToCompressedChunk(c)
		n := writeChunkRecord(buf[off:], cc)
		rec := jrecordLookup{offset: j.offset + off, length: n}
		off += int64(n)

		src.lookups[*record.a] = rec
		src.compressedSz += uint64(cc.CompressedSize())
	}
	src.address = addr(hash.Of(buf[:off]))

	if err := j.flushBuffer(buf[:off]); err != nil {
		return nil, err
	}
	j.sources[src.address] = src
	j.offset += off

	return src, nil
}

// ConjoinAll implements tablePersister.
func (j *chunkJournal) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	var cnt uint32
	for _, s := range sources {
		c, err := s.count()
		if err != nil {
			return nil, err
		}
		cnt += c
	}

	src := journalChunkSource{
		journal: j.journal,
		lookups: make(map[addr]jrecordLookup, cnt),
	}
	buf := make([]byte, 0, len(sources)*hash.ByteLen)

	for _, s := range sources {
		jcs, ok := s.(journalChunkSource)
		if !ok {
			return nil, fmt.Errorf("unexpected chunk source %v", s)
		}
		name := jcs.address.String()
		if _, ok = j.sources[jcs.address]; !ok {
			return nil, fmt.Errorf("unknown journal chunk source %s", name)
		}
		buf = append(buf, jcs.address[:]...)

		for a, l := range jcs.lookups {
			src.lookups[a] = l
		}
		src.compressedSz += jcs.compressedSz
	}

	// make an arbitrary name for |src|
	src.address = addr(hash.Of(buf))
	j.sources[src.address] = src

	return src, nil
}

// Open implements tablePersister.
func (j *chunkJournal) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	src, ok := j.sources[name]
	if !ok {
		return nil, fmt.Errorf("unknown chunk source %s", name.String())
	}
	return src, nil
}

// PruneTableFiles implements tablePersister.
func (j *chunkJournal) PruneTableFiles(ctx context.Context, contents manifestContents) error {
	panic("unimplemented")
}

// Name implements manifest.
func (j *chunkJournal) Name() string {
	return chunkJournalName
}

// Update implements manifest.
func (j *chunkJournal) Update(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	// check if we've seen the manifest,
	// if not persist |next| to |j.backing|
	if emptyAddr(j.manifest.lock) {
		// we expect |next.specs| to contain one entry for the chunkJournal
		// and additional entries for in-memory journalChunkSources
		for _, s := range next.specs {
			if _, ok := j.sources[s.name]; !ok {
				panic("unknown table spec " + s.name.String())
			}
		}
		// when persisting |next| to |j.backing|, only provide a
		// single tableSpec for the chunkJournal
		cnt, _ := j.sources[journalAddr].count()
		next.specs = []tableSpec{{name: journalAddr, chunkCount: cnt}}

		mc, err := j.backing.Update(ctx, lastLock, next, stats, writeHook)
		j.manifest = mc
		j.rootHash = mc.root
		return mc, err
	}

	if j.manifest.gcGen != next.gcGen {
		panic("chunkJournal cannot update GC generation")
	}

	if writeHook != nil {
		if err := writeHook(); err != nil {
			return manifestContents{}, err
		}
	}

	if j.manifest.lock != lastLock {
		return j.manifest, nil // |next| is stale
	}

	buf := make([]byte, rootHashRecordSize)
	writeRootHashRecord(buf, addr(next.root))
	if err := j.flushBuffer(buf); err != nil {
		return manifestContents{}, err
	}
	j.manifest = next
	j.rootHash = next.root

	return j.manifest, nil
}

// ParseIfExists implements manifest.
func (j *chunkJournal) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (ok bool, mc manifestContents, err error) {
	// check if we've seen the manifest
	ok = !emptyAddr(j.manifest.lock)

	if !ok {
		ok, mc, err = j.backing.ParseIfExists(ctx, stats, readHook)
		// the journal file is the source of truth for the latest root hash.
		// manifest |j.backing| may be stale if the latest contents were not
		// flushed while closing/shutting-down the chunkJournal.
		mc.root = j.rootHash
		j.manifest = mc
		return
	}

	if readHook != nil {
		if err = readHook(); err != nil {
			return false, manifestContents{}, err
		}
	}

	return ok, j.manifest, nil
}

func (j *chunkJournal) flushBuffer(buf []byte) (err error) {
	// todo(andy): pad to page boundary
	if _, err = j.journal.WriteAt(buf, j.offset); err != nil {
		return err
	}
	return j.journal.Sync()
}

func (s journalChunkSource) has(h addr) (bool, error) {
	_, ok := s.lookups[h]
	return ok, nil
}

func (s journalChunkSource) hasMany(addrs []hasRecord) (missing bool, err error) {
	for i := range addrs {
		a := addrs[i].a
		if _, ok := s.lookups[*a]; ok {
			addrs[i].has = true
		} else {
			missing = true
		}
	}
	return
}

func (s journalChunkSource) getCompressed(_ context.Context, h addr, _ *Stats) (cc CompressedChunk, err error) {
	l, ok := s.lookups[h]
	if !ok {
		return CompressedChunk{}, nil
	}

	buf := make([]byte, l.length)
	if _, err = s.journal.ReadAt(buf, l.offset); err != nil {
		return CompressedChunk{}, nil
	}

	rec := readJournalRecord(buf)
	if h != rec.address {
		err = fmt.Errorf("bad chunk get (%s != %s)", h.String(), rec.address.String())
		return
	}

	return NewCompressedChunk(hash.Hash(h), rec.payload)
}

func (s journalChunkSource) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
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

func (s journalChunkSource) getMany(ctx context.Context, _ *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (bool, error) {
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

func (s journalChunkSource) getManyCompressed(ctx context.Context, _ *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error) {
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

func (s journalChunkSource) count() (uint32, error) {
	return uint32(len(s.lookups)), nil
}

func (s journalChunkSource) uncompressedLen() (uint64, error) {
	// todo(andy)
	return s.compressedSz, nil
}

func (s journalChunkSource) hash() (addr, error) {
	return s.address, nil
}

// reader implements chunkSource.
func (s journalChunkSource) reader(context.Context) (io.Reader, error) {
	// todo(andy): |reader()| belongs to the chunkSource interface and exists
	//  due to the duality between chunkSources & table files. chunkJournal
	//  seeks to create many chunkSources that depend on a single file.
	//  |reader()| in particular is relevant to conjoin implementations.
	panic("unimplemented")
}

// size implements chunkSource.
// size returns the total size of the chunkSource: chunks, index, and footer
func (s journalChunkSource) size() (uint64, error) {
	return s.compressedSz, nil // todo(andy)
}

// index implements chunkSource.
func (s journalChunkSource) index() (tableIndex, error) {
	panic("unimplemented")
}

func (s journalChunkSource) clone() (chunkSource, error) {
	return s, nil
}

func (s journalChunkSource) close() error {
	// todo(andy): ref count open/close?
	return nil
}

func emptyAddr(a addr) bool {
	var b addr
	return a == b
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

func processRecords(ctx context.Context, r io.ReadSeeker, cb func(o int64, r jrecord) error) (int64, error) {
	var (
		buf []byte
		off int64
		err error
	)

	// todo(andy): reader buffer must be able to hold an entire record,
	//   but we don't have a hard limit on record size right now
	rdr := bufio.NewReaderSize(r, 1024*1024)
	for {
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
	}
	if err != nil && err != io.EOF {
		return 0, err
	}
	// reset the file pointer to end of the last
	// successfully processed journal record
	if _, err = r.Seek(off, 0); err != nil {
		return 0, err
	}
	return off, nil
}

func readUint(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func writeUint(buf []byte, u uint32) {
	binary.BigEndian.PutUint32(buf, u)
}
