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
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/chunks"
)

const concurrentCompactions = 5

func newTableSet(p tablePersister, q MemoryQuotaProvider) tableSet {
	return tableSet{p: p, q: q, rl: make(chan struct{}, concurrentCompactions)}
}

// tableSet is an immutable set of persistable chunkSources.
type tableSet struct {
	novel, upstream chunkSources
	p               tablePersister
	q               MemoryQuotaProvider
	rl              chan struct{}
}

func (ts tableSet) has(h addr) (bool, error) {
	f := func(css chunkSources) (bool, error) {
		for _, haver := range css {
			has, err := haver.has(h)

			if err != nil {
				return false, err
			}

			if has {
				return true, nil
			}
		}
		return false, nil
	}

	novelHas, err := f(ts.novel)

	if err != nil {
		return false, err
	}

	if novelHas {
		return true, nil
	}

	return f(ts.upstream)
}

func (ts tableSet) hasMany(addrs []hasRecord) (bool, error) {
	f := func(css chunkSources) (bool, error) {
		for _, haver := range css {
			has, err := haver.hasMany(addrs)

			if err != nil {
				return false, err
			}

			if !has {
				return false, nil
			}
		}
		return true, nil
	}
	remaining, err := f(ts.novel)

	if err != nil {
		return false, err
	}

	if !remaining {
		return false, nil
	}

	return f(ts.upstream)
}

func (ts tableSet) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	f := func(css chunkSources) ([]byte, error) {
		for _, haver := range css {
			data, err := haver.get(ctx, h, stats)

			if err != nil {
				return nil, err
			}

			if data != nil {
				return data, nil
			}
		}

		return nil, nil
	}

	data, err := f(ts.novel)

	if err != nil {
		return nil, err
	}

	if data != nil {
		return data, nil
	}

	return f(ts.upstream)
}

func (ts tableSet) getMany(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (remaining bool, err error) {
	f := func(css chunkSources) bool {
		for _, haver := range css {
			if rp, ok := haver.(chunkReadPlanner); ok {
				offsets, remaining, err := rp.findOffsets(reqs)
				if err != nil {
					return true
				}
				err = rp.getManyAtOffsets(ctx, eg, offsets, found, stats)
				if err != nil {
					return true
				}
				if !remaining {
					return false
				}
				continue
			}
			remaining, err = haver.getMany(ctx, eg, reqs, found, stats)
			if err != nil {
				return true
			}
			if !remaining {
				return false
			}
		}
		return true
	}

	return f(ts.novel) && err == nil && f(ts.upstream), err
}

func (ts tableSet) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (remaining bool, err error) {
	f := func(css chunkSources) bool {
		for _, haver := range css {
			if rp, ok := haver.(chunkReadPlanner); ok {
				offsets, remaining, err := rp.findOffsets(reqs)
				if err != nil {
					return true
				}
				if len(offsets) > 0 {
					err = rp.getManyCompressedAtOffsets(ctx, eg, offsets, found, stats)
					if err != nil {
						return true
					}
				}

				if !remaining {
					return false
				}

				continue
			}

			remaining, err = haver.getManyCompressed(ctx, eg, reqs, found, stats)
			if err != nil {
				return true
			}
			if !remaining {
				return false
			}
		}

		return true
	}

	return f(ts.novel) && err == nil && f(ts.upstream), err
}

func (ts tableSet) calcReads(reqs []getRecord, blockSize uint64) (reads int, split, remaining bool, err error) {
	f := func(css chunkSources) (int, bool, bool, error) {
		reads, split := 0, false
		for _, haver := range css {
			rds, rmn, err := haver.calcReads(reqs, blockSize)

			if err != nil {
				return 0, false, false, err
			}

			reads += rds
			if !rmn {
				return reads, split, false, nil
			}
			split = true
		}
		return reads, split, true, nil
	}
	reads, split, remaining, err = f(ts.novel)

	if err != nil {
		return 0, false, false, err
	}

	if remaining {
		var rds int
		rds, split, remaining, err = f(ts.upstream)

		if err != nil {
			return 0, false, false, err
		}

		reads += rds
	}

	return reads, split, remaining, nil
}

func (ts tableSet) count() (uint32, error) {
	f := func(css chunkSources) (count uint32, err error) {
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
	f := func(css chunkSources) (data uint64, err error) {
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
	f := func(css chunkSources) (data uint64, err error) {
		for _, haver := range css {
			index, err := haver.index()
			if err != nil {
				return 0, err
			}
			data += index.TableFileSize()
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

func (ts tableSet) Close() error {
	var firstErr error
	setErr := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, t := range ts.novel {
		err := t.Close()
		setErr(err)
	}
	for _, t := range ts.upstream {
		err := t.Close()
		setErr(err)
	}
	return firstErr
}

// Size returns the number of tables in this tableSet.
func (ts tableSet) Size() int {
	return len(ts.novel) + len(ts.upstream)
}

// Novel returns the number of tables containing novel chunks in this
// tableSet.
func (ts tableSet) Novel() int {
	return len(ts.novel)
}

// Upstream returns the number of known-persisted tables in this tableSet.
func (ts tableSet) Upstream() int {
	return len(ts.upstream)
}

// Prepend adds a memTable to an existing tableSet, compacting |mt| and
// returning a new tableSet with newly compacted table added.
func (ts tableSet) Prepend(ctx context.Context, mt *memTable, stats *Stats) tableSet {
	newTs := tableSet{
		novel:    make(chunkSources, len(ts.novel)+1),
		upstream: make(chunkSources, len(ts.upstream)),
		p:        ts.p,
		q:        ts.q,
		rl:       ts.rl,
	}
	newTs.novel[0] = newPersistingChunkSource(ctx, mt, ts, ts.p, ts.rl, stats)
	copy(newTs.novel[1:], ts.novel)
	copy(newTs.upstream, ts.upstream)
	return newTs
}

func (ts tableSet) extract(ctx context.Context, chunks chan<- extractRecord) error {
	// Since new tables are _prepended_ to a tableSet, extracting chunks in insertOrder requires iterating ts.upstream back to front, followed by ts.novel.
	for i := len(ts.upstream) - 1; i >= 0; i-- {
		err := ts.upstream[i].extract(ctx, chunks)

		if err != nil {
			return err
		}
	}
	for i := len(ts.novel) - 1; i >= 0; i-- {
		err := ts.novel[i].extract(ctx, chunks)

		if err != nil {
			return err
		}
	}

	return nil
}

// Flatten returns a new tableSet with |upstream| set to the union of ts.novel
// and ts.upstream.
func (ts tableSet) Flatten(ctx context.Context) (tableSet, error) {
	flattened := tableSet{
		upstream: make(chunkSources, 0, ts.Size()),
		p:        ts.p,
		q:        ts.q,
		rl:       ts.rl,
	}

	for _, src := range ts.novel {
		cnt, err := src.count()

		if err != nil {
			return tableSet{}, err
		}

		if cnt > 0 {
			flattened.upstream = append(flattened.upstream, src)
		}
	}

	flattened.upstream = append(flattened.upstream, ts.upstream...)
	return flattened, nil
}

// Rebase returns a new tableSet holding the novel tables managed by |ts| and
// those specified by |specs|.
func (ts tableSet) Rebase(ctx context.Context, specs []tableSpec, stats *Stats) (tableSet, error) {
	merged := tableSet{
		novel:    make(chunkSources, 0, len(ts.novel)),
		upstream: make(chunkSources, 0, len(specs)),
		p:        ts.p,
		q:        ts.q,
		rl:       ts.rl,
	}

	// Rebase the novel tables, skipping those that are actually empty (usually due to de-duping during table compaction)
	for _, t := range ts.novel {
		cnt, err := t.count()

		if err != nil {
			return tableSet{}, err
		}

		if cnt > 0 {
			t2, err := t.Clone()
			if err != nil {
				return tableSet{}, err
			}
			merged.novel = append(merged.novel, t2)
		}
	}

	// Create a list of tables to open so we can open them in parallel.
	tablesToOpen := []tableSpec{} // keep specs in order to play nicely with manifest appendix optimization
	presents := map[addr]tableSpec{}
	for _, spec := range specs {
		if _, present := presents[spec.name]; !present { // Filter out dups
			tablesToOpen = append(tablesToOpen, spec)
			presents[spec.name] = spec
		}
	}

	// Open all the new upstream tables concurrently
	var rp atomic.Value
	ae := atomicerr.New()
	merged.upstream = make(chunkSources, len(tablesToOpen))
	wg := &sync.WaitGroup{}
	wg.Add(len(tablesToOpen))
	for i, spec := range tablesToOpen {
		go func(idx int, spec tableSpec) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					rp.Store(r)
				}
			}()
			if !ae.IsSet() {
				for _, existing := range ts.upstream {
					h, err := existing.hash()
					if err != nil {
						ae.SetIfError(err)
						return
					}
					if spec.name == h {
						c, err := existing.Clone()
						if err != nil {
							ae.SetIfError(err)
							return
						}
						merged.upstream[idx] = c
						return
					}
				}
				err := ts.q.AcquireQuota(ctx, spec.GetMemorySize())
				if err != nil {
					ae.SetIfError(err)
					return
				}
				merged.upstream[idx], err = ts.p.Open(ctx, spec.name, spec.chunkCount, stats)
				ae.SetIfError(err)
			}
		}(i, spec)
	}
	wg.Wait()

	if r := rp.Load(); r != nil {
		panic(r)
	}

	if err := ae.Get(); err != nil {
		return tableSet{}, err
	}

	return merged, nil
}

func (ts tableSet) ToSpecs() ([]tableSpec, error) {
	tableSpecs := make([]tableSpec, 0, ts.Size())
	for _, src := range ts.novel {
		cnt, err := src.count()

		if err != nil {
			return nil, err
		}

		if cnt > 0 {
			h, err := src.hash()

			if err != nil {
				return nil, err
			}

			tableSpecs = append(tableSpecs, tableSpec{h, cnt})
		}
	}
	for _, src := range ts.upstream {
		cnt, err := src.count()

		if err != nil {
			return nil, err
		}

		if cnt <= 0 {
			return nil, errors.New("no upstream chunks")
		}

		h, err := src.hash()

		if err != nil {
			return nil, err
		}

		tableSpecs = append(tableSpecs, tableSpec{h, cnt})
	}
	return tableSpecs, nil
}
