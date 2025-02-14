// Copyright 2019 Dolthub, Inc.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// Returned when a chunk with a reference to a non-existence chunk is
// persisted into the ChunkStore. The sanity check is done when we
// flush the memtable, which means that a ChunkStore interaction which
// sees this error is not necessarily responsible for the dangling ref.
// Regardless, all pending writes in the memtable are thrown away when
// any chunk in the memtable has a dangling ref.
var ErrDanglingRef = errors.New("dangling ref")

const concurrentCompactions = 5

func newTableSet(p tablePersister, q MemoryQuotaProvider) tableSet {
	return tableSet{p: p, q: q, rl: make(chan struct{}, concurrentCompactions)}
}

// tableSet is an immutable set of persistable chunkSources.
type tableSet struct {
	novel, upstream chunkSourceSet
	p               tablePersister
	q               MemoryQuotaProvider
	rl              chan struct{}
}

func (ts tableSet) has(h hash.Hash, keeper keeperF) (bool, gcBehavior, error) {
	f := func(css chunkSourceSet) (bool, gcBehavior, error) {
		for _, haver := range css {
			has, gcb, err := haver.has(h, keeper)
			if err != nil {
				return false, gcb, err
			}
			if gcb != gcBehavior_Continue {
				return false, gcb, nil
			}
			if has {
				return true, gcBehavior_Continue, nil
			}
		}
		return false, gcBehavior_Continue, nil
	}

	novelHas, gcb, err := f(ts.novel)
	if err != nil {
		return false, gcb, err
	}
	if gcb != gcBehavior_Continue {
		return false, gcb, nil
	}
	if novelHas {
		return true, gcBehavior_Continue, nil
	}

	return f(ts.upstream)
}

func (ts tableSet) hasMany(addrs []hasRecord, keeper keeperF) (bool, gcBehavior, error) {
	f := func(css chunkSourceSet) (bool, gcBehavior, error) {
		for _, haver := range css {
			has, gcb, err := haver.hasMany(addrs, keeper)
			if err != nil {
				return false, gcb, err
			}
			if gcb != gcBehavior_Continue {
				return false, gcb, nil
			}
			if !has {
				return false, gcBehavior_Continue, nil
			}
		}
		return true, gcBehavior_Continue, nil
	}
	remaining, gcb, err := f(ts.novel)
	if err != nil {
		return false, gcb, err
	}
	if gcb != gcBehavior_Continue {
		return remaining, gcb, err
	}
	if !remaining {
		return false, gcBehavior_Continue, nil
	}

	return f(ts.upstream)
}

// Updates the records in |addrs| for whether they exist in this table set, but
// only consults tables whose names appear in |srcs|, ignoring all other tables
// in the table set. Returns |remaining| as true if all addresses were not
// found in the consulted tables, and false otherwise.
//
// Intended to be exactly like |hasMany|, except filtering for the files
// consulted. Only used for part of the GC workflow where we want to have
// access to all chunks in the store but need to check for existing chunk
// presence in only a subset of its files.
func (ts tableSet) hasManyInSources(srcs []hash.Hash, addrs []hasRecord, keeper keeperF) (bool, gcBehavior, error) {
	var remaining bool
	var err error
	var gcb gcBehavior
	for _, rec := range addrs {
		if !rec.has {
			remaining = true
			break
		}
	}
	if !remaining {
		return false, gcBehavior_Continue, nil
	}
	for _, srcAddr := range srcs {
		src, ok := ts.novel[srcAddr]
		if !ok {
			src, ok = ts.upstream[srcAddr]
			if !ok {
				continue
			}
		}
		remaining, gcb, err = src.hasMany(addrs, keeper)
		if err != nil {
			return false, gcb, err
		}
		if gcb != gcBehavior_Continue {
			return false, gcb, nil
		}
		if !remaining {
			break
		}
	}
	return remaining, gcBehavior_Continue, nil
}

func (ts tableSet) get(ctx context.Context, h hash.Hash, keeper keeperF, stats *Stats) ([]byte, gcBehavior, error) {
	if err := ctx.Err(); err != nil {
		return nil, gcBehavior_Continue, err
	}
	f := func(css chunkSourceSet) ([]byte, gcBehavior, error) {
		for _, haver := range css {
			data, gcb, err := haver.get(ctx, h, keeper, stats)
			if err != nil {
				return nil, gcb, err
			}
			if gcb != gcBehavior_Continue {
				return nil, gcb, nil
			}
			if data != nil {
				return data, gcBehavior_Continue, nil
			}
		}
		return nil, gcBehavior_Continue, nil
	}

	data, gcb, err := f(ts.novel)
	if err != nil {
		return nil, gcb, err
	}
	if gcb != gcBehavior_Continue {
		return nil, gcb, nil
	}
	if data != nil {
		return data, gcBehavior_Continue, nil
	}

	return f(ts.upstream)
}

func (ts tableSet) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	f := func(css chunkSourceSet) (bool, gcBehavior, error) {
		for _, haver := range css {
			remaining, gcb, err := haver.getMany(ctx, eg, reqs, found, keeper, stats)
			if err != nil {
				return true, gcb, err
			}
			if gcb != gcBehavior_Continue {
				return true, gcb, nil
			}
			if !remaining {
				return false, gcb, nil
			}
		}
		return true, gcBehavior_Continue, nil
	}

	remaining, gcb, err := f(ts.novel)
	if err != nil {
		return true, gcb, err
	}
	if gcb != gcBehavior_Continue {
		return true, gcb, nil
	}
	if !remaining {
		return false, gcBehavior_Continue, nil
	}

	return f(ts.upstream)
}

func (ts tableSet) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, ToChunker), keeper keeperF, stats *Stats) (bool, gcBehavior, error) {
	f := func(css chunkSourceSet) (bool, gcBehavior, error) {
		for _, haver := range css {
			remaining, gcb, err := haver.getManyCompressed(ctx, eg, reqs, found, keeper, stats)
			if err != nil {
				return true, gcb, err
			}
			if gcb != gcBehavior_Continue {
				return true, gcb, nil
			}
			if !remaining {
				return false, gcBehavior_Continue, nil
			}
		}
		return true, gcBehavior_Continue, nil
	}

	remaining, gcb, err := f(ts.novel)
	if err != nil {
		return true, gcb, err
	}
	if gcb != gcBehavior_Continue {
		return remaining, gcb, nil
	}
	if !remaining {
		return false, gcBehavior_Continue, nil
	}

	return f(ts.upstream)
}

func (ts tableSet) count() (uint32, error) {
	f := func(css chunkSourceSet) (count uint32, err error) {
		for _, haver := range css {
			thisCount, err := haver.count()

			if err != nil {
				return 0, err
			}

			count += thisCount
		}
		return
	}

	novelCount, err := f(ts.novel)

	if err != nil {
		return 0, err
	}

	upCount, err := f(ts.upstream)

	if err != nil {
		return 0, err
	}

	return novelCount + upCount, nil
}

func (ts tableSet) uncompressedLen() (uint64, error) {
	f := func(css chunkSourceSet) (data uint64, err error) {
		for _, haver := range css {
			uncmpLen, err := haver.uncompressedLen()

			if err != nil {
				return 0, err
			}

			data += uncmpLen
		}
		return
	}

	novelCount, err := f(ts.novel)

	if err != nil {
		return 0, err
	}

	upCount, err := f(ts.upstream)

	if err != nil {
		return 0, err
	}

	return novelCount + upCount, nil
}

func (ts tableSet) physicalLen() (uint64, error) {
	f := func(css chunkSourceSet) (data uint64, err error) {
		for _, haver := range css {
			data += haver.currentSize()
		}
		return
	}

	lenNovel, err := f(ts.novel)
	if err != nil {
		return 0, err
	}

	lenUp, err := f(ts.upstream)
	if err != nil {
		return 0, err
	}

	return lenNovel + lenUp, nil
}

func (ts tableSet) close() error {
	var firstErr error
	setErr := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, t := range ts.novel {
		err := t.close()
		setErr(err)
	}
	for _, t := range ts.upstream {
		err := t.close()
		setErr(err)
	}
	return firstErr
}

// Size returns the number of tables in this tableSet.
func (ts tableSet) Size() int {
	return len(ts.novel) + len(ts.upstream)
}

// append adds a memTable to an existing tableSet, compacting |mt| and
// returning a new tableSet with newly compacted table added.
func (ts tableSet) append(ctx context.Context, mt *memTable, checker refCheck, keeper keeperF, hasCache *lru.TwoQueueCache[hash.Hash, struct{}], stats *Stats) (tableSet, gcBehavior, error) {
	addrs := hash.NewHashSet()
	for _, getAddrs := range mt.getChildAddrs {
		getAddrs(ctx, addrs, func(h hash.Hash) bool { return hasCache.Contains(h) })
	}
	mt.addChildRefs(addrs)

	for i := range mt.pendingRefs {
		if !mt.pendingRefs[i].has && hasCache.Contains(*mt.pendingRefs[i].a) {
			mt.pendingRefs[i].has = true
		}
	}

	sort.Sort(hasRecordByPrefix(mt.pendingRefs))
	absent, err := checker(mt.pendingRefs)
	if err != nil {
		return tableSet{}, gcBehavior_Continue, err
	} else if absent.Size() > 0 {
		return tableSet{}, gcBehavior_Continue, fmt.Errorf("%w: found dangling references to %s", ErrDanglingRef, absent.String())
	}

	cs, gcb, err := ts.p.Persist(ctx, mt, ts, keeper, stats)
	if err != nil {
		return tableSet{}, gcBehavior_Continue, err
	}
	if gcb != gcBehavior_Continue {
		return tableSet{}, gcb, nil
	}

	newTs := tableSet{
		novel:    copyChunkSourceSet(ts.novel),
		upstream: copyChunkSourceSet(ts.upstream),
		p:        ts.p,
		q:        ts.q,
		rl:       ts.rl,
	}
	newTs.novel[cs.hash()] = cs
	return newTs, gcBehavior_Continue, nil
}

// flatten returns a new tableSet with |upstream| set to the union of ts.novel
// and ts.upstream.
func (ts tableSet) flatten(ctx context.Context) (tableSet, error) {
	flattened := tableSet{
		upstream: copyChunkSourceSet(ts.upstream),
		p:        ts.p,
		q:        ts.q,
		rl:       ts.rl,
	}

	for _, src := range ts.novel {
		cnt, err := src.count()
		if err != nil {
			return tableSet{}, err
		} else if cnt > 0 {
			flattened.upstream[src.hash()] = src
		}
	}
	return flattened, nil
}

// openForAdd will attempt to open every file named in |files| with the
// table persister, returning a new chunkSourceSet for all of the files
// if they are all able to be opened. An error will be returned if any
// errors are encountered when opening the files.
//
// For any files which appear in |novel| or |upstream|, this function
// will return clones of the chunk sources, instead of opening them
// anew.
func (ts tableSet) openForAdd(ctx context.Context, files map[hash.Hash]uint32, stats *Stats) (chunkSourceSet, error) {
	ret := make(chunkSourceSet)
	cleanup := func() {
		for _, source := range ret {
			source.close()
		}
	}
	// First add clones of all sources that are already present in
	// ts.novel or ts.upstream.
	for h := range files {
		if s, ok := ts.novel[h]; ok {
			cloned, err := s.clone()
			if err != nil {
				cleanup()
				return nil, err
			}
			ret[h] = cloned
		} else if s, ok := ts.upstream[h]; ok {
			cloned, err := s.clone()
			if err != nil {
				cleanup()
				return nil, err
			}
			ret[h] = cloned
		}
	}
	// Concurrently open all files that are not already
	// in |ret|.
	eg, ctx := errgroup.WithContext(ctx)
	var mu sync.Mutex
	for fileId, chunkCount := range files {
		mu.Lock()
		_, ok := ret[fileId]
		mu.Unlock()
		if ok {
			continue
		}
		eg.Go(func() error {
			cs, err := ts.p.Open(ctx, fileId, chunkCount, stats)
			if err != nil {
				return err
			}
			mu.Lock()
			ret[fileId] = cs
			mu.Unlock()
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		cleanup()
		return nil, err
	}
	return ret, nil
}

// rebase returns a new tableSet holding the novel tables managed by |ts| and
// those specified by |specs|.
func (ts tableSet) rebase(ctx context.Context, specs []tableSpec, srcs chunkSourceSet, stats *Stats) (tableSet, error) {
	// deduplicate |specs|
	orig := specs
	specs = make([]tableSpec, 0, len(orig))
	seen := map[hash.Hash]struct{}{}
	for _, spec := range orig {
		if _, ok := seen[spec.name]; ok {
			continue
		}
		seen[spec.name] = struct{}{}
		// keep specs in order to play nicely with
		// manifest appendix optimization
		specs = append(specs, spec)
	}

	closeAll := func(css chunkSourceSet) {
		for _, cs := range css {
			cs.close()
		}
	}

	// copy |ts.novel|, skipping empty chunkSources
	// (usually due to de-duping during table compaction)
	novel := make(chunkSourceSet, len(ts.novel))
	for _, t := range ts.novel {
		cnt, err := t.count()
		if err != nil {
			closeAll(novel)
			return tableSet{}, err
		} else if cnt == 0 {
			continue
		}
		t2, err := t.clone()
		if err != nil {
			closeAll(novel)
			return tableSet{}, err
		}
		novel[t2.hash()] = t2
	}

	eg, ctx := errgroup.WithContext(ctx)
	mu := new(sync.Mutex)
	upstream := make(chunkSourceSet, len(specs))
	for _, spec := range specs {
		// open missing tables in parallel
		eg.Go(func() error {
			var cs chunkSource
			var err error
			if existing, ok := ts.upstream[spec.name]; ok {
				cs, err = existing.clone()
			} else if existing, ok := srcs[spec.name]; ok {
				cs, err = existing.clone()
			} else {
				cs, err = ts.p.Open(ctx, spec.name, spec.chunkCount, stats)
			}
			if err != nil {
				return err
			}
			mu.Lock()
			upstream[cs.hash()] = cs
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		closeAll(upstream)
		closeAll(novel)
		return tableSet{}, err
	}

	return tableSet{
		novel:    novel,
		upstream: upstream,
		p:        ts.p,
		q:        ts.q,
		rl:       ts.rl,
	}, nil
}

func (ts tableSet) toSpecs() ([]tableSpec, error) {
	tableSpecs := make([]tableSpec, 0, ts.Size())
	for a, src := range ts.novel {
		if _, ok := ts.upstream[a]; ok {
			continue
		}

		cnt, err := src.count()
		if err != nil {
			return nil, err
		} else if cnt > 0 {
			h := src.hash()
			tableSpecs = append(tableSpecs, tableSpec{h, cnt})
		}
	}
	for _, src := range ts.upstream {
		cnt, err := src.count()
		if err != nil {
			return nil, err
		} else if cnt <= 0 {
			return nil, errors.New("no upstream chunks")
		}
		h := src.hash()
		tableSpecs = append(tableSpecs, tableSpec{h, cnt})
	}
	sort.Slice(tableSpecs, func(i, j int) bool {
		return bytes.Compare(tableSpecs[i].name[:], tableSpecs[j].name[:]) < 0
	})
	return tableSpecs, nil
}

func tableSetCalcReads(ts tableSet, reqs []getRecord, blockSize uint64, keeper keeperF) (reads int, split, remaining bool, gcb gcBehavior, err error) {
	all := copyChunkSourceSet(ts.upstream)
	for a, cs := range ts.novel {
		all[a] = cs
	}
	gcb = gcBehavior_Continue
	for _, tbl := range all {
		rdr, ok := tbl.(*fileTableReader)
		if !ok {
			err = fmt.Errorf("chunkSource %s is not a fileTableReader", tbl.hash().String())
			return
		}

		var n int
		var more bool
		n, more, gcb, err = rdr.calcReads(reqs, blockSize, keeper)
		if err != nil {
			return 0, false, false, gcb, err
		}
		if gcb != gcBehavior_Continue {
			return 0, false, false, gcb, nil
		}

		reads += n
		if !more {
			break
		}
		split = true
	}
	return
}
