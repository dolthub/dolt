// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"sort"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
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
	Conjoin(upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) manifestContents
}

type inlineConjoiner struct {
	maxTables int
}

func (c inlineConjoiner) ConjoinRequired(ts tableSet) bool {
	return ts.Size() > c.maxTables
}

func (c inlineConjoiner) Conjoin(upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) manifestContents {
	return conjoin(upstream, mm, p, stats)
}

func conjoin(upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) manifestContents {
	var conjoined tableSpec
	var conjoinees, keepers []tableSpec

	for {
		if conjoinees == nil {
			conjoined, conjoinees, keepers = conjoinTables(p, upstream.specs, stats)
		}

		specs := append(make([]tableSpec, 0, len(keepers)+1), conjoined)
		specs = append(specs, keepers...)

		newContents := manifestContents{
			vers:  constants.NomsVersion,
			root:  upstream.root,
			lock:  generateLockHash(upstream.root, specs),
			specs: specs,
		}
		upstream = mm.Update(upstream.lock, newContents, stats, nil)

		if newContents.lock == upstream.lock {
			return upstream // Success!
		}
		// Optimistic lock failure. Someone else moved to the root, the set of tables, or both out from under us.
		// If we can re-use the conjoin we already performed, we want to try again. Currently, we will only do so if ALL conjoinees are still present upstream. If we can't re-use...then someone else almost certainly landed a conjoin upstream. In this case, bail and let clients ask again if they think they still can't proceed.
		conjoineeSet := map[addr]struct{}{}
		upstreamNames := map[addr]struct{}{}
		for _, spec := range upstream.specs {
			upstreamNames[spec.name] = struct{}{}
		}
		for _, c := range conjoinees {
			if _, present := upstreamNames[c.name]; !present {
				return upstream // Bail!
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

func conjoinTables(p tablePersister, upstream []tableSpec, stats *Stats) (conjoined tableSpec, conjoinees, keepers []tableSpec) {
	// Open all the upstream tables concurrently
	sources := make(chunkSources, len(upstream))
	wg := sync.WaitGroup{}
	for i, spec := range upstream {
		wg.Add(1)
		go func(idx int, spec tableSpec) {
			sources[idx] = p.Open(spec.name, spec.chunkCount, stats)
			wg.Done()
		}(i, spec)
		i++
	}
	wg.Wait()

	t1 := time.Now()

	toConjoin, toKeep := chooseConjoinees(sources)
	conjoinedSrc := p.ConjoinAll(toConjoin, stats)

	stats.ConjoinLatency.SampleTimeSince(t1)
	stats.TablesPerConjoin.SampleLen(len(toConjoin))
	stats.ChunksPerConjoin.Sample(uint64(conjoinedSrc.count()))

	return tableSpec{conjoinedSrc.hash(), conjoinedSrc.count()}, toSpecs(toConjoin), toSpecs(toKeep)
}

// Current approach is to choose the smallest N tables which, when removed and replaced with the conjoinment, will leave the conjoinment as the smallest table.
func chooseConjoinees(upstream chunkSources) (toConjoin, toKeep chunkSources) {
	sortedUpstream := make(chunkSources, len(upstream))
	copy(sortedUpstream, upstream)
	sort.Sort(chunkSourcesByAscendingCount(sortedUpstream))

	partition := 2
	sum := sortedUpstream[0].count() + sortedUpstream[1].count()
	for partition < len(sortedUpstream) && sum > sortedUpstream[partition].count() {
		sum += sortedUpstream[partition].count()
		partition++
	}

	return sortedUpstream[:partition], sortedUpstream[partition:]
}

func toSpecs(srcs chunkSources) []tableSpec {
	specs := make([]tableSpec, len(srcs))
	for i, src := range srcs {
		d.PanicIfFalse(src.count() > 0)
		specs[i] = tableSpec{src.hash(), src.count()}
	}
	return specs
}
