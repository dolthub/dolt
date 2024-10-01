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
	"runtime/trace"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// journalChunkSource is a chunkSource that reads chunks
// from a ChunkJournal. Unlike other NBS chunkSources,
// it is not immutable and its set of chunks grows as
// more commits are made to the ChunkJournal.
type journalChunkSource struct {
	journal *journalWriter
}

var _ chunkSource = journalChunkSource{}

func (s journalChunkSource) has(h hash.Hash) (bool, error) {
	return s.journal.hasAddr(h), nil
}

func (s journalChunkSource) hasMany(addrs []hasRecord) (missing bool, err error) {
	for i := range addrs {
		ok := s.journal.hasAddr(*addrs[i].a)
		if ok {
			addrs[i].has = true
		} else {
			missing = true
		}
	}
	return
}

func (s journalChunkSource) getCompressed(ctx context.Context, h hash.Hash, _ *Stats) (CompressedChunk, error) {
	defer trace.StartRegion(ctx, "journalChunkSource.getCompressed").End()
	return s.journal.getCompressedChunk(h)
}

func (s journalChunkSource) get(ctx context.Context, h hash.Hash, _ *Stats) ([]byte, error) {
	defer trace.StartRegion(ctx, "journalChunkSource.get").End()

	cc, err := s.journal.getCompressedChunk(h)
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

type journalRecord struct {
	// r is the journal range for this chunk
	r Range
	// idx is the array offset into the shared |reqs|
	idx int
}

func (s journalChunkSource) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (bool, error) {
	return s.getManyCompressed(ctx, eg, reqs, func(ctx context.Context, cc CompressedChunk) {
		ch, err := cc.ToChunk()
		if err != nil {
			eg.Go(func() error {
				return err
			})
			return
		}
		chWHash := chunks.NewChunkWithHash(cc.Hash(), ch.Data())
		found(ctx, &chWHash)
	}, stats)
}

// getManyCompressed implements chunkReader. Here we (1) synchronously check
// the journal index for read ranges, (2) record if the source misses any
// needed remaining chunks, (3) sort the lookups for efficient disk access,
// and then (4) asynchronously perform reads. We release the journal read
// lock after returning when all reads are completed, which can be after the
// function returns.
func (s journalChunkSource) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error) {
	defer trace.StartRegion(ctx, "journalChunkSource.getManyCompressed").End()

	var remaining bool
	var jReqs []journalRecord
	var wg sync.WaitGroup
	s.journal.lock.RLock()
	for i, r := range reqs {
		if r.found {
			continue
		}
		rang, ok := s.journal.ranges.get(*r.a)
		if !ok {
			remaining = true
			continue
		}
		jReqs = append(jReqs, journalRecord{r: rang, idx: i})
		reqs[i].found = true
	}

	// sort chunks by journal locality
	sort.Slice(jReqs, func(i, j int) bool {
		return jReqs[i].r.Offset < jReqs[j].r.Offset
	})

	for i := range jReqs {
		// workers populate the parent error group
		// record local workers for releasing lock
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			rec := jReqs[i]
			a := reqs[rec.idx].a
			if cc, err := s.journal.getCompressedChunkAtRange(rec.r, *a); err != nil {
				return err
			} else if cc.IsEmpty() {
				return errors.New("chunk in journal index was empty.")
			} else {
				found(ctx, cc)
				return nil
			}
		})
	}
	go func() {
		wg.Wait()
		s.journal.lock.RUnlock()
	}()
	return remaining, nil
}

func (s journalChunkSource) count() (uint32, error) {
	return s.journal.recordCount(), nil
}

func (s journalChunkSource) uncompressedLen() (uint64, error) {
	return s.journal.uncompressedSize(), nil
}

func (s journalChunkSource) hash() hash.Hash {
	return journalAddr
}

// reader implements chunkSource.
func (s journalChunkSource) reader(ctx context.Context) (io.ReadCloser, uint64, error) {
	rdr, sz, err := s.journal.snapshot(ctx)
	return rdr, uint64(sz), err
}

func (s journalChunkSource) getRecordRanges(ctx context.Context, requests []getRecord) (map[hash.Hash]Range, error) {
	ranges := make(map[hash.Hash]Range, len(requests))
	for _, req := range requests {
		if req.found {
			continue
		}
		rng, ok, err := s.journal.getRange(ctx, *req.a)
		if err != nil {
			return nil, err
		} else if !ok {
			continue
		}
		req.found = true // update |requests|
		ranges[hash.Hash(*req.a)] = rng
	}
	return ranges, nil
}

// size implements chunkSource.
// size returns the total size of the chunkSource: chunks, index, and footer
func (s journalChunkSource) currentSize() uint64 {
	return uint64(s.journal.currentSize())
}

// index implements chunkSource.
func (s journalChunkSource) index() (tableIndex, error) {
	return nil, fmt.Errorf("journalChunkSource cannot be conjoined")
}

func (s journalChunkSource) clone() (chunkSource, error) {
	return s, nil
}

func (s journalChunkSource) close() error {
	// |s.journal| closed via ChunkJournal
	return nil
}

func (s journalChunkSource) getAllChunkHashes(_ context.Context, out chan<- hash.Hash, wg *sync.WaitGroup) int {
	chunkCount := s.journal.recordCount()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.journal.ranges.novel.Iter(func(k hash.Hash, v Range) (stop bool) {
			out <- k
			return false
		})

		s.journal.ranges.cached.Iter(func(k addr16, v Range) (stop bool) {
			// Currently we only have 16bytes of the hash. The value returned here will have 4 0xFF bytes at the end.
			var h hash.Hash
			copy(h[:], k[:])
			out <- h

			return false
		})
	}()

	return int(chunkCount)
}

func equalSpecs(left, right []tableSpec) bool {
	if len(left) != len(right) {
		return false
	}
	l := make(map[hash.Hash]struct{}, len(left))
	for _, s := range left {
		l[s.name] = struct{}{}
	}
	for _, s := range right {
		if _, ok := l[s.name]; !ok {
			return false
		}
	}
	return true
}
