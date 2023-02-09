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
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// recLookup contains journalRec lookup metadata.
type recLookup struct {
	// journalOff is the file offset of the journalRec.
	journalOff int64

	// recordLen is the length of the journalRec.
	recordLen uint32

	// payloadOff is the offset of the payload within the
	// journalRec, it's used for converting to a Range.
	payloadOff uint32
}

// rangeFromLookup converts a recLookup to a Range,
// used when computing GetDownloadLocs.
func rangeFromLookup(l recLookup) Range {
	return Range{
		// see journalRec for serialization format
		Offset: uint64(l.journalOff) + uint64(l.payloadOff),
		Length: l.recordLen - (l.payloadOff + recChecksumSz),
	}
}

// lookupMap is a thread-safe collection of recLookups.
type lookupMap struct {
	data map[addr]recLookup
	lock *sync.RWMutex
}

func newLookupMap() lookupMap {
	return lookupMap{
		data: make(map[addr]recLookup),
		lock: new(sync.RWMutex),
	}
}

func (m lookupMap) get(a addr) (l recLookup, ok bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	l, ok = m.data[a]
	return
}

func (m lookupMap) put(a addr, l recLookup) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.data[a] = l
	return
}

func (m lookupMap) count() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.data)
}

// journalChunkSource is a chunkSource that reads chunks
// from a chunkJournal. Unlike other NBS chunkSources,
// it is not immutable and its set of chunks grows as
// more commits are made to the chunkJournal.
type journalChunkSource struct {
	address        addr
	journal        snapshotReader
	lookups        lookupMap
	uncompressedSz uint64
}

var _ chunkSource = journalChunkSource{}

func (s journalChunkSource) has(h addr) (bool, error) {
	_, ok := s.lookups.get(h)
	return ok, nil
}

func (s journalChunkSource) hasMany(addrs []hasRecord) (missing bool, err error) {
	for i := range addrs {
		a := addrs[i].a
		if _, ok := s.lookups.get(*a); ok {
			addrs[i].has = true
		} else {
			missing = true
		}
	}
	return
}

func (s journalChunkSource) getCompressed(_ context.Context, h addr, _ *Stats) (CompressedChunk, error) {
	l, ok := s.lookups.get(h)
	if !ok {
		return CompressedChunk{}, nil
	}

	buf := make([]byte, l.recordLen)
	if _, err := s.journal.ReadAt(buf, l.journalOff); err != nil {
		return CompressedChunk{}, nil
	}

	rec, err := readJournalRecord(buf)
	if err != nil {
		return CompressedChunk{}, err
	} else if h != rec.address {
		return CompressedChunk{}, fmt.Errorf("chunk record hash does not match lookup hash (%s != %s)",
			h.String(), rec.address.String())
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
	return uint32(s.lookups.count()), nil
}

func (s journalChunkSource) uncompressedLen() (uint64, error) {
	return s.uncompressedSz, nil
}

func (s journalChunkSource) hash() addr {
	return s.address
}

// reader implements chunkSource.
func (s journalChunkSource) reader(context.Context) (io.ReadCloser, uint64, error) {
	rdr, sz, err := s.journal.Snapshot()
	return io.NopCloser(rdr), uint64(sz), err
}

func (s journalChunkSource) getRecordRanges(requests []getRecord) (map[hash.Hash]Range, error) {
	ranges := make(map[hash.Hash]Range, len(requests))
	for _, req := range requests {
		if req.found {
			continue
		}
		l, ok := s.lookups.get(*req.a)
		if !ok {
			continue
		}
		req.found = true // update |requests|
		ranges[hash.Hash(*req.a)] = rangeFromLookup(l)
	}
	return ranges, nil
}

// size implements chunkSource.
// size returns the total size of the chunkSource: chunks, index, and footer
func (s journalChunkSource) currentSize() uint64 {
	return uint64(s.journal.CurrentSize())
}

// index implements chunkSource.
func (s journalChunkSource) index() (tableIndex, error) {
	return nil, fmt.Errorf("journalChunkSource cannot be conjoined")
}

func (s journalChunkSource) clone() (chunkSource, error) {
	return s, nil
}

func (s journalChunkSource) close() error {
	return nil
}

func equalSpecs(left, right []tableSpec) bool {
	if len(left) != len(right) {
		return false
	}
	l := make(map[addr]struct{}, len(left))
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

func emptyAddr(a addr) bool {
	var b addr
	return a == b
}
