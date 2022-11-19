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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var chunkJournalFeatureFlag = false

func init() {
	if os.Getenv("DOLT_ENABLE_CHUNK_JOURNAL") != "" {
		chunkJournalFeatureFlag = true
	}
}

const (
	chunkJournalName = "nbs_chunk_journal"
)

type chunkJournal struct {
	journal *journalWriter

	sources map[addr]chunkSource

	rootHash hash.Hash
	manifest manifestContents
	backing  manifest
}

var _ tablePersister = &chunkJournal{}
var _ manifest = &chunkJournal{}
var _ io.Closer = &chunkJournal{}

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
	path, err := filepath.Abs(filepath.Join(dir, chunkJournalName))
	if err != nil {
		return nil, err
	}

	wr, err := openJournalWriter(ctx, path)
	if err != nil {
		return nil, err
	}

	root, cs, err := wr.bootstrapJournal(ctx)
	if err != nil {
		return nil, err
	}

	a, err := cs.hash()
	if err != nil {
		return nil, err
	}
	sources := map[addr]chunkSource{a: cs}

	return &chunkJournal{
		journal:  wr,
		rootHash: root,
		backing:  m,
		sources:  sources,
	}, nil
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

	for _, record := range mt.order {
		if record.has {
			continue
		}
		c := chunks.NewChunkWithHash(hash.Hash(*record.a), mt.chunks[*record.a])
		cc := ChunkToCompressedChunk(c)
		lookup, err := j.journal.writeChunk(cc)
		if err != nil {
			return nil, err
		}
		src.lookups[*record.a] = lookup
		src.compressedSz += uint64(cc.CompressedSize())
	}

	// pick an arbitrary name for |src|
	for a := range src.lookups {
		src.address = addr(hash.Of(a[:]))
		break
	}
	j.sources[src.address] = src

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
	// check if we've seen the manifest, if not persist |next| to |j.backing|
	if emptyAddr(j.manifest.lock) {
		var specs []tableSpec
		for _, s := range next.specs {
			if _, ok := j.sources[s.name]; ok {
				// for in-memory journalChunkSources
				// don't write spec addresses to |backing|
				continue
			}
			specs = append(specs, s)
		}
		// add a tableSpec with the special chunkJournal addr
		cnt, _ := j.sources[journalAddr].count()
		specs = append(specs, tableSpec{name: journalAddr, chunkCount: cnt})
		backing := next
		backing.specs = specs

		mc, err := j.backing.Update(ctx, lastLock, backing, stats, writeHook)
		if err != nil {
			return manifestContents{}, err
		}

		// if update succeeded, save full contents of |next|
		if mc.root == next.root {
			j.manifest = next
			j.rootHash = next.root
		}
		return j.manifest, err
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

	if err := j.journal.writeRootHash(next.root); err != nil {
		return manifestContents{}, nil
	}
	j.rootHash = next.root
	j.manifest = next

	return j.manifest, nil
}

// ParseIfExists implements manifest.
func (j *chunkJournal) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (ok bool, mc manifestContents, err error) {
	// check if we've seen the manifest
	ok = !emptyAddr(j.manifest.lock)

	if !ok {
		ok, mc, err = j.backing.ParseIfExists(ctx, stats, readHook)
		if err != nil {
			return false, manifestContents{}, err
		}

		// the journal file is the source of truth for the latest root hash.
		// manifest |j.backing| may be stale if the latest contents were not
		// flushed while closing/shutting-down the chunkJournal.
		mc.root = j.rootHash
		for a, s := range j.sources {
			c, _ := s.count()
			mc.specs = append(mc.specs, tableSpec{
				name:       a,
				chunkCount: c,
			})
		}
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

// Close implements io.Closer
// todo(andy): not currently called (doltdb.DoltDB.Close())
func (j *chunkJournal) Close() (err error) {
	// todo(andy): on graceful shutdown, we need to
	//  flush |manifest| and |rootHash| to |backing|
	return j.journal.Close()
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
	return nil
}

func emptyAddr(a addr) bool {
	var b addr
	return a == b
}
