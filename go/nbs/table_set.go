// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"sync"
)

const concurrentCompactions = 5

func newS3TableSet(s3 s3svc, bucket string, indexCache *s3IndexCache) tableSet {
	return tableSet{
		p:  s3TablePersister{s3, bucket, defaultS3PartSize, indexCache},
		rl: make(chan struct{}, concurrentCompactions),
	}
}

func newFSTableSet(dir string) tableSet {
	return tableSet{
		p:  fsTablePersister{dir},
		rl: make(chan struct{}, concurrentCompactions),
	}
}

// tableSet is an immutable set of persistable chunkSources.
type tableSet struct {
	chunkSources
	p  tablePersister
	rl chan struct{}
}

type chunkSources []chunkSource

func (css chunkSources) has(h addr) bool {
	for _, haver := range css {
		if haver.has(h) {
			return true
		}
	}
	return false
}

func (css chunkSources) hasMany(addrs []hasRecord) (remaining bool) {
	for _, haver := range css {
		if !haver.hasMany(addrs) {
			return false
		}
	}
	return true
}

func (css chunkSources) get(h addr) []byte {
	for _, haver := range css {
		if data := haver.get(h); data != nil {
			return data
		}
	}
	return nil
}

func (css chunkSources) getMany(reqs []getRecord) (remaining bool) {
	for _, haver := range css {
		if !haver.getMany(reqs) {
			return false
		}
	}

	return true
}

func (css chunkSources) count() (count uint32) {
	for _, haver := range css {
		count += haver.count()
	}
	return
}

// Prepend adds a memTable to an existing tableSet, compacting |mt| and
// returning a new tableSet with newly compacted table added.
func (ts tableSet) Prepend(mt *memTable) tableSet {
	newTables := make(chunkSources, len(ts.chunkSources)+1)
	newTables[0] = newCompactingChunkSource(mt, ts, ts.p, ts.rl)
	copy(newTables[1:], ts.chunkSources)
	return tableSet{newTables, ts.p, ts.rl}
}

// Union returns a new tableSet holding the union of the tables managed by
// |ts| and those specified by |specs|.
func (ts tableSet) Union(specs []tableSpec) tableSet {
	newTables := make(chunkSources, 0, len(ts.chunkSources))
	known := map[addr]struct{}{}
	for _, t := range ts.chunkSources {
		if t.count() > 0 {
			known[t.hash()] = struct{}{}
			newTables = append(newTables, t)
		}
	}

	// Create a list of tables to open so we can open them in parallel
	tablesToOpen := make([]tableSpec, 0, len(specs))
	for _, t := range specs {
		if _, present := known[t.name]; !present {
			tablesToOpen = append(tablesToOpen, t)
		}
	}

	openedTables := make(chunkSources, len(tablesToOpen))
	wg := &sync.WaitGroup{}

	for i, spec := range tablesToOpen {
		wg.Add(1)
		go func(idx int, spec tableSpec) {
			openedTables[idx] = ts.p.Open(spec.name, spec.chunkCount)
			wg.Done()
		}(i, spec)
	}

	wg.Wait()
	newTables = append(newTables, openedTables...)

	return tableSet{newTables, ts.p, ts.rl}
}

func (ts tableSet) ToSpecs() []tableSpec {
	tableSpecs := make([]tableSpec, 0, len(ts.chunkSources))
	for _, src := range ts.chunkSources {
		if src.count() > 0 {
			tableSpecs = append(tableSpecs, tableSpec{src.hash(), src.count()})
		}
	}
	return tableSpecs
}

func (ts tableSet) Close() (err error) {
	for _, t := range ts.chunkSources {
		err = t.close() // TODO: somehow coalesce these errors??
	}
	close(ts.rl)
	return
}
