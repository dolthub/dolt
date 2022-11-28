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
	"time"

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

	source journalChunkSource

	contents manifestContents
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

	root, source, err := wr.bootstrapJournal(ctx)
	if err != nil {
		return nil, err
	}

	ok, contents, err := m.ParseIfExists(ctx, &Stats{}, nil)
	if err != nil {
		return nil, err
	}

	if ok {
		// the journal file is the source of truth for the root hash, true-up persisted manifest
		contents.root = root
		if contents, err = m.Update(ctx, contents.lock, contents, &Stats{}, nil); err != nil {
			return nil, err
		}
	} else if !emptyAddr(addr(root)) {
		// journal file contains root hash, but manifest is missing
		return nil, fmt.Errorf("missing manifest while initializing chunk journal")
	}

	return &chunkJournal{
		journal:  wr,
		source:   source,
		contents: contents,
		backing:  m,
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
		j.source.lookups[*record.a] = lookup
		j.source.compressedSz += uint64(cc.CompressedSize())
	}
	return j.source, nil
}

// ConjoinAll implements tablePersister.
func (j *chunkJournal) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	panic("unimplemented")
}

// Open implements tablePersister.
func (j *chunkJournal) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	if name == j.source.address {
		return j.source, nil
	}
	return nil, fmt.Errorf("unknown chunk source %s", name.String())
}

// Exists implements tablePersister.
func (j *chunkJournal) Exists(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (bool, error) {
	panic("unimplemented")
}

// PruneTableFiles implements tablePersister.
func (j *chunkJournal) PruneTableFiles(ctx context.Context, contents manifestContents, mtime time.Time) error {
	panic("unimplemented")
}

// Name implements manifest.
func (j *chunkJournal) Name() string {
	return j.journal.filepath()
}

// Update implements manifest.
func (j *chunkJournal) Update(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	if j.contents.gcGen != next.gcGen {
		panic("chunkJournal cannot update GC generation")
	} else if j.contents.lock != lastLock {
		return j.contents, nil // |next| is stale
	}

	if writeHook != nil {
		if err := writeHook(); err != nil {
			return manifestContents{}, err
		}
	}

	if emptyAddr(addr(next.root)) {
		panic(next)
	}

	if err := j.journal.writeRootHash(next.root); err != nil {
		return manifestContents{}, err
	}
	j.contents = next

	return j.contents, nil
}

// ParseIfExists implements manifest.
func (j *chunkJournal) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (ok bool, mc manifestContents, err error) {
	if emptyAddr(j.contents.lock) {
		ok, mc, err = j.backing.ParseIfExists(ctx, stats, readHook)
		if err != nil || !ok {
			return false, manifestContents{}, err
		}
		j.contents = mc
		return
	}
	if readHook != nil {
		if err = readHook(); err != nil {
			return false, manifestContents{}, err
		}
	}
	ok, mc = true, j.contents
	return
}

func (j *chunkJournal) flushManifest() error {
	ctx, s := context.Background(), &Stats{}
	_, last, err := j.backing.ParseIfExists(ctx, s, nil)
	if err != nil {
		return err
	}
	if !emptyAddr(j.contents.lock) {
		_, err = j.backing.Update(ctx, last.lock, j.contents, s, nil)
	}
	return err
}

// Close implements io.Closer
func (j *chunkJournal) Close() (err error) {
	if cerr := j.flushManifest(); cerr != nil {
		err = cerr
	}
	if cerr := j.journal.Close(); cerr != nil {
		err = cerr
	}
	return
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

func (s journalChunkSource) hash() addr {
	return s.address
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
