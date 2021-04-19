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
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/store/atomicerr"
)

type conjoiner interface {
	// ConjoinRequired tells the caller whether or not it's time to request a
	// Conjoin, based upon the contents of |ts| and the conjoiner
	// implementation's policy.
	ConjoinRequired(ts tableSet) bool

	// Conjoin attempts to use |p| to conjoin some number of tables referenced
	// by |upstream|, allowing it to update |mm| with a new, smaller, set of tables
	// that references precisely the same set of chunks. Conjoin() may not
	// actually conjoin any upstream tables, usually because some out-of-
	// process actor has already landed a conjoin of its own. Callers must
	// handle this, likely by rebasing against upstream and re-evaluating the
	// situation.
	Conjoin(ctx context.Context, upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) (manifestContents, error)
}

type inlineConjoiner struct {
	maxTables int
}

func (c inlineConjoiner) ConjoinRequired(ts tableSet) bool {
	return ts.Size() > c.maxTables && len(ts.upstream) >= 2
}

func (c inlineConjoiner) Conjoin(ctx context.Context, upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) (manifestContents, error) {
	return conjoin(ctx, upstream, mm, p, stats)
}

type noopConjoiner struct {
}

func (c noopConjoiner) ConjoinRequired(ts tableSet) bool {
	return false
}

func (c noopConjoiner) Conjoin(ctx context.Context, upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) (manifestContents, error) {
	return manifestContents{}, errors.New("unsupported conjoin operation on noopConjoiner")
}

func conjoin(ctx context.Context, upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) (manifestContents, error) {
	var conjoined tableSpec
	var conjoinees, keepers, appendixSpecs []tableSpec

	for {
		if conjoinees == nil {
			// Appendix table files should never be conjoined
			// so we remove them before conjoining and add them
			// back after
			if upstream.NumAppendixSpecs() != 0 {
				upstream, appendixSpecs = upstream.removeAppendixSpecs()
			}

			var err error
			conjoined, conjoinees, keepers, err = conjoinTables(ctx, p, upstream.specs, stats)

			if err != nil {
				return manifestContents{}, err
			}
		}

		specs := append(make([]tableSpec, 0, len(keepers)+1), conjoined)
		if len(appendixSpecs) > 0 {
			specs = append(make([]tableSpec, 0, len(specs)+len(appendixSpecs)), appendixSpecs...)
			specs = append(specs, conjoined)
		}

		specs = append(specs, keepers...)

		newContents := manifestContents{
			vers:     upstream.vers,
			root:     upstream.root,
			lock:     generateLockHash(upstream.root, specs),
			gcGen:    upstream.gcGen,
			specs:    specs,
			appendix: appendixSpecs,
		}

		var err error
		upstream, err = mm.Update(ctx, upstream.lock, newContents, stats, nil)
		if err != nil {
			return manifestContents{}, err
		}

		if newContents.lock == upstream.lock {
			return upstream, nil
		}

		// Optimistic lock failure. Someone else moved to the root, the set of tables, or both out from under us.
		// If we can re-use the conjoin we already performed, we want to try again. Currently, we will only do so if ALL conjoinees are still present upstream. If we can't re-use...then someone else almost certainly landed a conjoin upstream. In this case, bail and let clients ask again if they think they still can't proceed.

		// If the appendix has changed we simply bail
		// and let the client retry
		if len(appendixSpecs) > 0 {
			if len(upstream.appendix) != len(appendixSpecs) {
				return upstream, nil
			}
			for i := range upstream.appendix {
				if upstream.appendix[i].name != appendixSpecs[i].name {
					return upstream, nil
				}
			}

			// No appendix change occured, so we remove the appendix
			// on the "latest" upstream which will be added back
			// before the conjoin completes
			upstream, appendixSpecs = upstream.removeAppendixSpecs()
		}

		conjoineeSet := map[addr]struct{}{}
		upstreamNames := map[addr]struct{}{}
		for _, spec := range upstream.specs {
			upstreamNames[spec.name] = struct{}{}
		}
		for _, c := range conjoinees {
			if _, present := upstreamNames[c.name]; !present {
				return upstream, nil // Bail!
			}
			conjoineeSet[c.name] = struct{}{}
		}

		// Filter conjoinees out of upstream.specs to generate new set of keepers
		keepers = make([]tableSpec, 0, len(upstream.specs)-len(conjoinees))
		for _, spec := range upstream.specs {
			if _, present := conjoineeSet[spec.name]; !present {
				keepers = append(keepers, spec)
			}
		}
	}
}

func conjoinTables(ctx context.Context, p tablePersister, upstream []tableSpec, stats *Stats) (conjoined tableSpec, conjoinees, keepers []tableSpec, err error) {
	// Open all the upstream tables concurrently
	sources := make(chunkSources, len(upstream))

	ae := atomicerr.New()
	wg := sync.WaitGroup{}
	for i, spec := range upstream {
		wg.Add(1)
		go func(idx int, spec tableSpec) {
			defer wg.Done()
			var err error
			sources[idx], err = p.Open(ctx, spec.name, spec.chunkCount, stats)

			ae.SetIfError(err)
		}(i, spec)
		i++
	}
	wg.Wait()

	if err := ae.Get(); err != nil {
		return tableSpec{}, nil, nil, err
	}

	t1 := time.Now()

	toConjoin, toKeep, err := chooseConjoinees(sources)

	if err != nil {
		return tableSpec{}, nil, nil, err
	}

	conjoinedSrc, err := p.ConjoinAll(ctx, toConjoin, stats)

	if err != nil {
		return tableSpec{}, nil, nil, err
	}

	stats.ConjoinLatency.SampleTimeSince(t1)
	stats.TablesPerConjoin.SampleLen(len(toConjoin))

	cnt, err := conjoinedSrc.count()

	if err != nil {
		return tableSpec{}, nil, nil, err
	}

	stats.ChunksPerConjoin.Sample(uint64(cnt))

	conjoinees, err = toSpecs(toConjoin)

	if err != nil {
		return tableSpec{}, nil, nil, err
	}

	keepers, err = toSpecs(toKeep)

	if err != nil {
		return tableSpec{}, nil, nil, err
	}

	h, err := conjoinedSrc.hash()

	if err != nil {
		return tableSpec{}, nil, nil, err
	}

	cnt, err = conjoinedSrc.count()

	if err != nil {
		return tableSpec{}, nil, nil, err
	}

	return tableSpec{h, cnt}, conjoinees, keepers, nil
}

// Current approach is to choose the smallest N tables which, when removed and replaced with the conjoinment, will leave the conjoinment as the smallest table.
func chooseConjoinees(upstream chunkSources) (toConjoin, toKeep chunkSources, err error) {
	sortedUpstream := make(chunkSources, len(upstream))
	copy(sortedUpstream, upstream)

	csbac := chunkSourcesByAscendingCount{sortedUpstream, nil}
	sort.Sort(csbac)

	if csbac.err != nil {
		return nil, nil, csbac.err
	}

	partition := 2
	upZero, err := sortedUpstream[0].count()

	if err != nil {
		return nil, nil, err
	}

	upOne, err := sortedUpstream[1].count()

	if err != nil {
		return nil, nil, err
	}

	sum := upZero + upOne
	for partition < len(sortedUpstream) {
		partCnt, err := sortedUpstream[partition].count()

		if err != nil {
			return nil, nil, err
		}

		if sum <= partCnt {
			break
		}

		sum += partCnt
		partition++
	}

	return sortedUpstream[:partition], sortedUpstream[partition:], nil
}

func toSpecs(srcs chunkSources) ([]tableSpec, error) {
	specs := make([]tableSpec, len(srcs))
	for i, src := range srcs {
		cnt, err := src.count()

		if err != nil {
			return nil, err
		}

		if cnt <= 0 {
			return nil, errors.New("invalid table spec has no sources")
		}

		h, err := src.hash()

		if err != nil {
			return nil, err
		}

		cnt, err = src.count()

		if err != nil {
			return nil, err
		}

		specs[i] = tableSpec{h, cnt}
	}

	return specs, nil
}
