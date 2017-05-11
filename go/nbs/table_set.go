// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"sort"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
)

const concurrentCompactions = 5

func newTableSet(persister tablePersister) tableSet {
	return tableSet{p: persister, rl: make(chan struct{}, concurrentCompactions)}
}

// tableSet is an immutable set of persistable chunkSources.
type tableSet struct {
	// novel chunkSources contain chunks that have not yet been pushed upstream
	novel chunkSources

	// compactees holds precisely the chunkSources that were conjoined to build the members of compacted. If compacted is empty, compactees is also. The members of compactees are split out of upstream during Compact(), though they still represent actual persisted tables.
	compacted  chunkSources
	compactees chunkSources

	// upstream holds the set of already-persisted chunkSources that this tableSet references.
	upstream chunkSources

	p  tablePersister
	rl chan struct{}
}

func (ts tableSet) has(h addr) bool {
	f := func(css chunkSources) bool {
		for _, haver := range css {
			if haver.has(h) {
				return true
			}
		}
		return false
	}
	return f(ts.novel) || f(ts.compacted) || f(ts.upstream)
}

func (ts tableSet) hasMany(addrs []hasRecord) (remaining bool) {
	f := func(css chunkSources) (remaining bool) {
		for _, haver := range css {
			if !haver.hasMany(addrs) {
				return false
			}
		}
		return true
	}
	return f(ts.novel) && f(ts.compacted) && f(ts.upstream)
}

func (ts tableSet) get(h addr, stats *Stats) []byte {
	f := func(css chunkSources) []byte {
		for _, haver := range css {
			if data := haver.get(h, stats); data != nil {
				return data
			}
		}
		return nil
	}
	if data := f(ts.novel); data != nil {
		return data
	}
	if data := f(ts.compacted); data != nil {
		return data
	}
	return f(ts.upstream)
}

func (ts tableSet) getMany(reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup, stats *Stats) (remaining bool) {
	f := func(css chunkSources) (remaining bool) {
		for _, haver := range css {
			if !haver.getMany(reqs, foundChunks, wg, stats) {
				return false
			}
		}
		return true
	}
	return f(ts.novel) && f(ts.compacted) && f(ts.upstream)
}

func (ts tableSet) calcReads(reqs []getRecord, blockSize uint64) (reads int, split, remaining bool) {
	f := func(css chunkSources) (reads int, split, remaining bool) {
		for _, haver := range css {
			rds, remaining := haver.calcReads(reqs, blockSize)
			reads += rds
			if !remaining {
				return reads, split, remaining
			}
			split = true
		}
		return reads, split, remaining
	}
	reads, split, remaining = f(ts.novel)
	if remaining {
		var rds int
		rds, split, remaining = f(ts.compacted)
		reads += rds
	}
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
	return f(ts.novel) + f(ts.compacted) + f(ts.upstream)
}

func (ts tableSet) uncompressedLen() uint64 {
	f := func(css chunkSources) (data uint64) {
		for _, haver := range css {
			data += haver.uncompressedLen()
		}
		return
	}
	return f(ts.novel) + f(ts.compacted) + f(ts.upstream)
}

// Size returns the number of tables in this tableSet.
func (ts tableSet) Size() int {
	return len(ts.novel) + len(ts.compacted) + len(ts.upstream)
}

// Prepend adds a memTable to an existing tableSet, compacting |mt| and
// returning a new tableSet with newly compacted table added.
func (ts tableSet) Prepend(mt *memTable, stats *Stats) tableSet {
	newTs := tableSet{
		novel:      make(chunkSources, len(ts.novel)+1),
		compacted:  make(chunkSources, len(ts.compacted)),
		compactees: make(chunkSources, len(ts.compactees)),
		upstream:   make(chunkSources, len(ts.upstream)),
		p:          ts.p,
		rl:         ts.rl,
	}
	newTs.novel[0] = newPersistingChunkSource(mt, ts, ts.p, ts.rl, stats)
	copy(newTs.novel[1:], ts.novel)
	copy(newTs.compacted, ts.compacted)
	copy(newTs.compactees, ts.compactees)
	copy(newTs.upstream, ts.upstream)
	return newTs
}

// Compact returns a new tableSet that's smaller than |ts|. It chooses to
// compact the N smallest (by number of chunks) tables which can be compacted
// into a new table such that upon replacing the N input tables, the
// resulting table will still have the fewest chunks in the tableSet.
func (ts tableSet) Compact(stats *Stats) tableSet {
	t1 := time.Now()

	ns := tableSet{
		novel:      make(chunkSources, len(ts.novel)),
		compacted:  make(chunkSources, len(ts.compacted)+1),
		compactees: make(chunkSources, len(ts.compactees)),
		p:          ts.p,
		rl:         ts.rl,
	}
	copy(ns.novel, ts.novel)
	copy(ns.compacted[1:], ts.compacted) // leave the first slot for the newly compacted table
	copy(ns.compactees, ts.compactees)

	sortedUpstream := make(chunkSources, len(ts.upstream))
	copy(sortedUpstream, ts.upstream)
	sort.Sort(chunkSourcesByAscendingCount(sortedUpstream))

	partition := 2
	sum := sortedUpstream[0].count() + sortedUpstream[1].count()
	for partition < len(sortedUpstream) && sum > sortedUpstream[partition].count() {
		sum += sortedUpstream[partition].count()
		partition++
	}

	toCompact := sortedUpstream[:partition]

	ns.compacted[0] = ts.p.CompactAll(toCompact, stats)
	ns.upstream = append(ns.upstream, sortedUpstream[partition:]...)
	ns.compactees = append(ns.compactees, toCompact...)

	stats.ConjoinLatency.SampleTime(time.Since(t1))
	stats.TablesPerConjoin.SampleLen(len(toCompact))
	stats.ChunksPerConjoin.Sample(uint64(ns.compacted[0].count()))

	return ns
}

func (ts tableSet) extract(chunks chan<- extractRecord) {
	// Since new tables are _prepended_ to a tableSet, extracting chunks in insertOrder requires iterating ts.upstream, followed by ts.compacted, followed by ts.novel in back to front order.
	for i := len(ts.upstream) - 1; i >= 0; i-- {
		ts.upstream[i].extract(chunks)
	}
	for i := len(ts.compacted) - 1; i >= 0; i-- {
		ts.compacted[i].extract(chunks)
	}
	for i := len(ts.novel) - 1; i >= 0; i-- {
		ts.novel[i].extract(chunks)
	}
}

// Flatten returns a new tableSet with |upstream| set to the union of ts.novel,
// |ts.compacted|, and |ts.upstream|. |ts.compactees| is dropped.
func (ts tableSet) Flatten() (flattened tableSet) {
	flattened = tableSet{
		upstream: make(chunkSources, 0, ts.Size()),
		p:        ts.p,
		rl:       ts.rl,
	}
	f := func(srcs chunkSources) {
		for _, src := range srcs {
			if src.count() > 0 {
				flattened.upstream = append(flattened.upstream, src)
			}
		}
	}
	f(ts.novel)
	flattened.upstream = append(flattened.upstream, ts.compacted...)
	flattened.upstream = append(flattened.upstream, ts.upstream...)
	return
}

// Rebase returns a new tableSet holding the novel tables managed by |ts| and
// those specified by |specs|. If |ts.compacted| is not nil, Rebase runs
// through |ts.compactees| and checks to see if every element is still
// mentioned in |specs|. If all of them are still there, Rebase copies
// compaction state from |ts| to the new tableSet. Specifically,
// |ts.compacted| and |ts.compactees| are copied to the new tableSet, while
// the compactees are dropped from the new set of upstream tables.
func (ts tableSet) Rebase(specs []tableSpec) tableSet {
	merged := tableSet{
		novel: make(chunkSources, 0, len(ts.novel)),
		p:     ts.p,
		rl:    ts.rl,
	}

	// Rebase the novel tables, skipping those that are actually empty (usually due to de-duping during table compaction)
	for _, t := range ts.novel {
		if t.count() > 0 {
			merged.novel = append(merged.novel, t)
		}
	}

	// Only keep locally-performed compactions if ALL compactees are still present upstream
	keepCompactions, compacteeSet := func() (bool, map[addr]struct{}) {
		specsNames := map[addr]struct{}{}
		for _, spec := range specs {
			specsNames[spec.name] = struct{}{}
		}
		set := map[addr]struct{}{}
		for _, compactee := range ts.compactees {
			if _, present := specsNames[compactee.hash()]; !present {
				return false, nil
			}
			set[compactee.hash()] = struct{}{}
		}
		return true, set
	}()
	if keepCompactions {
		merged.compacted = make(chunkSources, len(ts.compacted))
		merged.compactees = make(chunkSources, len(ts.compactees))
		copy(merged.compacted, ts.compacted)
		copy(merged.compactees, ts.compactees)
	}

	// Create a list of tables to open so we can open them in parallel.
	tablesToOpen := map[addr]tableSpec{}
	for _, spec := range specs {
		if !keepCompactions {
			tablesToOpen[spec.name] = spec
			continue
		}
		if _, present := compacteeSet[spec.name]; !present { // Filter out compactees
			tablesToOpen[spec.name] = spec
		}
	}

	// Open all the new upstream tables concurrently
	merged.upstream = make(chunkSources, len(tablesToOpen))
	wg := &sync.WaitGroup{}
	i := 0
	for _, spec := range tablesToOpen {
		wg.Add(1)
		go func(idx int, spec tableSpec) {
			merged.upstream[idx] = ts.p.Open(spec.name, spec.chunkCount)
			wg.Done()
		}(i, spec)
		i++
	}
	wg.Wait()

	return merged
}

func (ts tableSet) ToSpecs() []tableSpec {
	tableSpecs := make([]tableSpec, 0, ts.Size())
	for _, src := range ts.novel {
		if src.count() > 0 {
			tableSpecs = append(tableSpecs, tableSpec{src.hash(), src.count()})
		}
	}
	for _, src := range ts.compacted {
		d.Chk.True(src.count() > 0)
		tableSpecs = append(tableSpecs, tableSpec{src.hash(), src.count()})
	}
	for _, src := range ts.upstream {
		d.Chk.True(src.count() > 0)
		tableSpecs = append(tableSpecs, tableSpec{src.hash(), src.count()})
	}
	return tableSpecs
}
