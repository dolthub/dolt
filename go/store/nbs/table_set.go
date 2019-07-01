// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"sync"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

const concurrentCompactions = 5

func newTableSet(persister tablePersister) tableSet {
	return tableSet{p: persister, rl: make(chan struct{}, concurrentCompactions)}
}

// tableSet is an immutable set of persistable chunkSources.
type tableSet struct {
	novel, upstream chunkSources
	p               tablePersister
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

func (ts tableSet) getMany(ctx context.Context, reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, ae *AtomicError, stats *Stats) bool {
	f := func(css chunkSources) bool {
		for _, haver := range css {
			if ae.IsSet() {
				return false
			}

			if rp, ok := haver.(chunkReadPlanner); ok {
				offsets, remaining := rp.findOffsets(reqs)
				go rp.getManyAtOffsets(reqs, offsets, foundChunks, wg, ae, stats)
				if !remaining {
					return false
				}

				continue
			}

			remaining := haver.getMany(ctx, reqs, foundChunks, wg, ae, stats)

			if !remaining {
				return false
			}
		}

		return true
	}

	return f(ts.novel) && f(ts.upstream)
}

func (ts tableSet) calcReads(reqs []getRecord, blockSize uint64) (reads int, split, remaining bool) {
	f := func(css chunkSources) (int, bool, bool) {
		reads, split := 0, false
		for _, haver := range css {
			rds, rmn := haver.calcReads(reqs, blockSize)
			reads += rds
			if !rmn {
				return reads, split, false
			}
			split = true
		}
		return reads, split, true
	}
	reads, split, remaining = f(ts.novel)
	if remaining {
		var rds int
		rds, split, remaining = f(ts.upstream)
		reads += rds
	}
	return reads, split, remaining
}

func (ts tableSet) count() uint32 {
	f := func(css chunkSources) (count uint32) {
		for _, haver := range css {
			count += haver.count()
		}
		return
	}
	return f(ts.novel) + f(ts.upstream)
}

func (ts tableSet) uncompressedLen() uint64 {
	f := func(css chunkSources) (data uint64) {
		for _, haver := range css {
			data += haver.uncompressedLen()
		}
		return
	}
	return f(ts.novel) + f(ts.upstream)
}

func (ts tableSet) physicalLen() uint64 {
	f := func(css chunkSources) (data uint64) {
		for _, haver := range css {
			index := haver.index()
			data += indexSize(index.chunkCount)
			data += index.offsets[index.chunkCount-1] + (uint64(index.lengths[index.chunkCount-1]))
		}
		return
	}
	return f(ts.novel) + f(ts.upstream)
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
func (ts tableSet) Flatten() (flattened tableSet) {
	flattened = tableSet{
		upstream: make(chunkSources, 0, ts.Size()),
		p:        ts.p,
		rl:       ts.rl,
	}
	for _, src := range ts.novel {
		if src.count() > 0 {
			flattened.upstream = append(flattened.upstream, src)
		}
	}
	flattened.upstream = append(flattened.upstream, ts.upstream...)
	return
}

// Rebase returns a new tableSet holding the novel tables managed by |ts| and
// those specified by |specs|.
func (ts tableSet) Rebase(ctx context.Context, specs []tableSpec, stats *Stats) tableSet {
	merged := tableSet{
		novel:    make(chunkSources, 0, len(ts.novel)),
		upstream: make(chunkSources, 0, len(specs)),
		p:        ts.p,
		rl:       ts.rl,
	}

	// Rebase the novel tables, skipping those that are actually empty (usually due to de-duping during table compaction)
	for _, t := range ts.novel {
		if t.count() > 0 {
			merged.novel = append(merged.novel, t)
		}
	}

	// Create a list of tables to open so we can open them in parallel.
	tablesToOpen := map[addr]tableSpec{}
	for _, spec := range specs {
		if _, present := tablesToOpen[spec.name]; !present { // Filter out dups
			tablesToOpen[spec.name] = spec
		}
	}

	// Open all the new upstream tables concurrently
	ae := NewAtomicError()
	merged.upstream = make(chunkSources, len(tablesToOpen))
	wg := &sync.WaitGroup{}
	i := 0
	for _, spec := range tablesToOpen {
		wg.Add(1)
		go func(idx int, spec tableSpec) {
			defer wg.Done()

			if !ae.IsSet() {
				var err error
				merged.upstream[idx], err = ts.p.Open(ctx, spec.name, spec.chunkCount, stats)
				ae.Set(err)
			}
		}(i, spec)
		i++
	}
	wg.Wait()

	// TODO: fix panics
	d.PanicIfError(ae.Get())

	return merged
}

func (ts tableSet) ToSpecs() []tableSpec {
	tableSpecs := make([]tableSpec, 0, ts.Size())
	for _, src := range ts.novel {
		if src.count() > 0 {
			tableSpecs = append(tableSpecs, tableSpec{src.hash(), src.count()})
		}
	}
	for _, src := range ts.upstream {
		d.Chk.True(src.count() > 0)
		tableSpecs = append(tableSpecs, tableSpec{src.hash(), src.count()})
	}
	return tableSpecs
}
