// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

func newS3TableSet(s3 s3svc, bucket string) tableSet {
	return tableSet{p: s3TablePersister{s3, bucket, defaultS3PartSize}, rl: make(chan struct{}, 5)}
}

func newFSTableSet(dir string) tableSet {
	return tableSet{p: fsTablePersister{dir}, rl: make(chan struct{}, 5)}
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

	for _, t := range specs {
		if _, present := known[t.name]; !present {
			newTables = append(newTables, ts.p.Open(t.name, t.chunkCount))
		}
	}
	return tableSet{newTables, ts.p, ts.rl}
}

func (ts tableSet) ToSpecs() []tableSpec {
	tableSpecs := make([]tableSpec, len(ts.chunkSources))
	for i, src := range ts.chunkSources {
		tableSpecs[i] = tableSpec{src.hash(), src.count()}
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
