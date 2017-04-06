// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"sort"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
)

const concurrentCompactions = 5

func newS3TableSet(s3 s3svc, bucket string, indexCache *indexCache, readRl chan struct{}) tableSet {
	return tableSet{
		p:  s3TablePersister{s3, bucket, defaultS3PartSize, indexCache, readRl},
		rl: make(chan struct{}, concurrentCompactions),
	}
}

func newFSTableSet(dir string, indexCache *indexCache) tableSet {
	return tableSet{
		p:  fsTablePersister{dir, indexCache},
		rl: make(chan struct{}, concurrentCompactions),
	}
}

// tableSet is an immutable set of persistable chunkSources.
type tableSet struct {
	novel, upstream chunkSources
	p               tablePersister
	rl              chan struct{}
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
	return f(ts.novel) || f(ts.upstream)
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
	return f(ts.novel) && f(ts.upstream)
}

func (ts tableSet) get(h addr) []byte {
	f := func(css chunkSources) []byte {
		for _, haver := range css {
			if data := haver.get(h); data != nil {
				return data
			}
		}
		return nil
	}
	if data := f(ts.novel); data != nil {
		return data
	}
	return f(ts.upstream)
}

func (ts tableSet) getMany(reqs []getRecord, foundChunks chan *chunks.Chunk, wg *sync.WaitGroup) (remaining bool) {
	f := func(css chunkSources) (remaining bool) {
		for _, haver := range css {
			if !haver.getMany(reqs, foundChunks, wg) {
				return false
			}
		}
		return true
	}
	return f(ts.novel) && f(ts.upstream)
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

// Size returns the number of tables in this tableSet.
func (ts tableSet) Size() int {
	return len(ts.novel) + len(ts.upstream)
}

// Prepend adds a memTable to an existing tableSet, compacting |mt| and
// returning a new tableSet with newly compacted table added.
func (ts tableSet) Prepend(mt *memTable) tableSet {
	newTs := tableSet{
		novel:    make(chunkSources, len(ts.novel)+1),
		upstream: make(chunkSources, len(ts.upstream)),
		p:        ts.p,
		rl:       ts.rl,
	}
	newTs.novel[0] = newCompactingChunkSource(mt, ts, ts.p, ts.rl)
	copy(newTs.novel[1:], ts.novel)
	copy(newTs.upstream, ts.upstream)
	return newTs
}

// Compact returns a new tableSet that's smaller than |ts|. It takes the
// max(2, len(ts)/2) smallest upstream tables, compacts them into a single
// large table, then returns a new tableSet with novel = ts.novel, and
// upstream set to this new large table and the not-compacted members of
// ts.upstream. The compactees are returned separately so that the caller can
// close them if she so chooses.
func (ts tableSet) Compact() (ns tableSet, compactees chunkSources) {
	ns = tableSet{
		novel: make(chunkSources, len(ts.novel)),
		p:     ts.p,
		rl:    ts.rl,
	}
	copy(ns.novel, ts.novel)

	max := func(a, b int) int {
		if a > b {
			return a
		}
		return b
	}

	sortedUpstream := make(chunkSources, len(ts.upstream))
	copy(sortedUpstream, ts.upstream)
	sort.Sort(chunkSourcesByDescendingCount(sortedUpstream))

	partition := len(sortedUpstream) - max(2, len(sortedUpstream)/2)
	toCompact := sortedUpstream[partition:]
	compacted := ts.p.CompactAll(toCompact)
	ns.upstream = append(chunkSources{compacted}, sortedUpstream[:partition]...)

	return ns, toCompact
}

func (ts tableSet) extract(chunks chan<- extractRecord) {
	// Since new tables are _prepended_ to a tableSet, extracting chunks in insertOrder requires iterating ts.upstream back to front, followed by ts.novel.
	for i := len(ts.upstream) - 1; i >= 0; i-- {
		ts.upstream[i].extract(chunks)
	}
	for i := len(ts.novel) - 1; i >= 0; i-- {
		ts.novel[i].extract(chunks)
	}
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
// those specified by |specs|. Tables in |ts.upstream| that are not referenced
// by |specs| are returned in |dropped| so that the caller can close() them
// appropriately.
func (ts tableSet) Rebase(specs []tableSpec) (merged tableSet, dropped chunkSources) {
	merged = tableSet{
		novel:    make(chunkSources, 0, len(ts.novel)),
		upstream: make(chunkSources, 0, len(specs)),
		p:        ts.p,
		rl:       ts.rl,
	}
	dropped = make(chunkSources, len(ts.upstream))
	copy(dropped, ts.upstream)

	// Rebase the novel tables, dropping those that are actually empty (usually due to de-duping during table compaction)
	for _, t := range ts.novel {
		if t.count() > 0 {
			merged.novel = append(merged.novel, t)
		} else {
			dropped = append(dropped, t)
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
	openedTables := make(chunkSources, len(tablesToOpen))
	wg := &sync.WaitGroup{}
	i := 0
	for _, spec := range tablesToOpen {
		wg.Add(1)
		go func(idx int, spec tableSpec) {
			openedTables[idx] = ts.p.Open(spec.name, spec.chunkCount)
			wg.Done()
		}(i, spec)
		i++
	}

	wg.Wait()
	merged.upstream = append(merged.upstream, openedTables...)
	return merged, dropped
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

func (ts tableSet) Close() (err error) {
	err = ts.novel.close()
	if e := ts.upstream.close(); e != nil {
		err = e // TODO: somehow coalesce these errors??
	}
	return
}
