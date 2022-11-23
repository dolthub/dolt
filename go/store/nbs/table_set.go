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

	"golang.org/x/sync/errgroup"

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
			data += index.tableFileSize()
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

// prepend adds a memTable to an existing tableSet, compacting |mt| and
// returning a new tableSet with newly compacted table added.
func (ts tableSet) prepend(ctx context.Context, mt *memTable, stats *Stats) (tableSet, error) {
	cs, err := ts.p.Persist(ctx, mt, ts, stats)
	if err != nil {
		return tableSet{}, err
	}

	newTs := tableSet{
		novel:    make(chunkSources, len(ts.novel)+1),
		upstream: make(chunkSources, len(ts.upstream)),
		p:        ts.p,
		q:        ts.q,
		rl:       ts.rl,
	}
	newTs.novel[0] = cs
	copy(newTs.novel[1:], ts.novel)
	copy(newTs.upstream, ts.upstream)
	return newTs, nil
}

// flatten returns a new tableSet with |upstream| set to the union of ts.novel
// and ts.upstream.
func (ts tableSet) flatten(ctx context.Context) (tableSet, error) {
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

func (ts tableSet) checkAllTablesExist(ctx context.Context, specs []tableSpec, stats *Stats) error {
	for _, spec := range specs {
		exists, err := ts.p.Exists(ctx, spec.name, spec.chunkCount, stats)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("table spec does not exist")
		}
	}
	return nil
}

// rebase returns a new tableSet holding the novel tables managed by |ts| and
// those specified by |specs|.
func (ts tableSet) rebase(ctx context.Context, specs []tableSpec, stats *Stats) (tableSet, error) {
	// deduplicate |specs|
	orig := specs
	specs = make([]tableSpec, 0, len(orig))
	seen := map[addr]struct{}{}
	for _, spec := range orig {
		if _, ok := seen[spec.name]; ok {
			continue
		}
		seen[spec.name] = struct{}{}
		// keep specs in order to play nicely with
		// manifest appendix optimization
		specs = append(specs, spec)
	}

	// copy |ts.novel|, skipping empty chunkSources
	// (usually due to de-duping during table compaction)
	novel := make(chunkSources, 0, len(ts.novel))
	for _, t := range ts.novel {
		cnt, err := t.count()
		if err != nil {
			return tableSet{}, err
		} else if cnt == 0 {
			continue
		}
		t2, err := t.clone()
		if err != nil {
			return tableSet{}, err
		}
		novel = append(novel, t2)
	}

	existing := make(map[addr]chunkSource, len(ts.upstream))
	for _, cs := range ts.upstream {
		existing[cs.hash()] = cs
	}

	// newly opened tables are unowned, we must
	// close them if the rebase operation fails
	opened := new(sync.Map)

	eg, ctx := errgroup.WithContext(ctx)
	upstream := make([]chunkSource, len(specs))
	for i, s := range specs {
		// clone tables that we have already opened
		if cs, ok := existing[s.name]; ok {
			c, err := cs.clone()
			if err != nil {
				return tableSet{}, err
			}
			upstream[i] = c
			continue
		}
		// open missing tables in parallel
		idx, spec := i, s
		eg.Go(func() error {
			cs, err := ts.p.Open(ctx, spec.name, spec.chunkCount, stats)
			if err != nil {
				return err
			}
			upstream[idx] = cs
			opened.Store(spec.name, cs)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		opened.Range(func(_, v any) bool {
			// close any opened chunkSources
			_ = v.(chunkSource).close()
			return true
		})
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
	for _, src := range ts.novel {
		cnt, err := src.count()

		if err != nil {
			return nil, err
		}

		if cnt > 0 {
			h := src.hash()
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

		h := src.hash()
		tableSpecs = append(tableSpecs, tableSpec{h, cnt})
	}
	sort.Slice(tableSpecs, func(i, j int) bool {
		return bytes.Compare(tableSpecs[i].name[:], tableSpecs[j].name[:]) < 0
	})
	return tableSpecs, nil
}

func tableSetCalcReads(ts tableSet, reqs []getRecord, blockSize uint64) (reads int, split, remaining bool, err error) {
	all := append(ts.novel, ts.upstream...)
	for _, tbl := range all {
		rdr, ok := tbl.(*fileTableReader)
		if !ok {
			err = fmt.Errorf("chunkSource %s is not a fileTableReader", tbl.hash().String())
			return
		}

		var n int
		var more bool
		n, more, err = rdr.calcReads(reqs, blockSize)
		if err != nil {
			return 0, false, false, err
		}

		reads += n
		if !more {
			break
		}
		split = true
	}
	return
}
