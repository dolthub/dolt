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
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/hash"
)

type conjoinStrategy interface {
	// conjoinRequired returns true if |conjoin| should be called.
	conjoinRequired(ts tableSet) bool

	// chooseConjoinees chooses which chunkSources to conjoin from |sources|
	chooseConjoinees(specs []tableSpec) (conjoinees, keepers []tableSpec, err error)
}

type inlineConjoiner struct {
	maxTables int
}

var _ conjoinStrategy = inlineConjoiner{}

func (c inlineConjoiner) conjoinRequired(ts tableSet) bool {
	return ts.Size() > c.maxTables && len(ts.upstream) >= 2
}

// chooseConjoinees implements conjoinStrategy. Current approach is to choose the smallest N tables which,
// when removed and replaced with the conjoinment, will leave the conjoinment as the smallest table.
func (c inlineConjoiner) chooseConjoinees(upstream []tableSpec) (conjoinees, keepers []tableSpec, err error) {
	sorted := make([]tableSpec, len(upstream))
	copy(sorted, upstream)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].chunkCount < sorted[j].chunkCount
	})

	i := 2
	sum := sorted[0].chunkCount + sorted[1].chunkCount
	for i < len(sorted) {
		next := sorted[i].chunkCount
		if sum <= next {
			break
		}
		sum += next
		i++
	}
	return sorted[:i], sorted[i:], nil
}

type noopConjoiner struct{}

var _ conjoinStrategy = noopConjoiner{}

func (c noopConjoiner) conjoinRequired(ts tableSet) bool {
	return false
}

func (c noopConjoiner) chooseConjoinees(sources []tableSpec) (conjoinees, keepers []tableSpec, err error) {
	keepers = sources
	return
}

// conjoin attempts to use |p| to conjoin some number of tables referenced
// by |upstream|, allowing it to update |mm| with a new, smaller, set of tables
// that references precisely the same set of chunks. Conjoin() may not
// actually conjoin any upstream tables, usually because some out-of-
// process actor has already landed a conjoin of its own. Callers must
// handle this, likely by rebasing against upstream and re-evaluating the
// situation.
func conjoin(ctx context.Context, s conjoinStrategy, upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) (manifestContents, cleanupFunc, error) {
	var conjoined tableSpec
	var conjoinees, keepers, appendixSpecs []tableSpec
	var cleanup cleanupFunc

	for {
		if conjoinees == nil {
			// Appendix table files should never be conjoined
			// so we remove them before conjoining and add them
			// back after
			if upstream.NumAppendixSpecs() != 0 {
				upstream, appendixSpecs = upstream.removeAppendixSpecs()
			}

			var err error
			conjoinees, keepers, err = s.chooseConjoinees(upstream.specs)
			if err != nil {
				return manifestContents{}, nil, err
			}

			conjoined, cleanup, err = conjoinTables(ctx, conjoinees, p, stats)
			if err != nil {
				return manifestContents{}, nil, err
			}
		}

		specs := append(make([]tableSpec, 0, len(keepers)+1), conjoined)
		if len(appendixSpecs) > 0 {
			specs = append(make([]tableSpec, 0, len(specs)+len(appendixSpecs)), appendixSpecs...)
			specs = append(specs, conjoined)
		}

		specs = append(specs, keepers...)

		newContents := manifestContents{
			nbfVers:  upstream.nbfVers,
			root:     upstream.root,
			lock:     generateLockHash(upstream.root, specs, appendixSpecs, nil),
			gcGen:    upstream.gcGen,
			specs:    specs,
			appendix: appendixSpecs,
		}

		var err error
		upstream, err = mm.Update(ctx, upstream.lock, newContents, stats, nil)
		if err != nil {
			return manifestContents{}, nil, err
		}

		if newContents.lock == upstream.lock {
			return upstream, cleanup, nil
		}

		// Optimistic lock failure. Someone else moved to the root, the
		// set of tables, or both out from under us.  If we can re-use
		// the conjoin we already performed, we want to try again.
		// Currently, we will only do so if ALL conjoinees are still
		// present upstream. If we can't re-use...then someone else
		// almost certainly landed a conjoin upstream. In this case,
		// bail and let clients ask again if they think they still
		// can't proceed.

		// If the appendix has changed we simply bail
		// and let the client retry
		if len(appendixSpecs) > 0 {
			if len(upstream.appendix) != len(appendixSpecs) {
				return upstream, func() {}, nil
			}
			for i := range upstream.appendix {
				if upstream.appendix[i].hash != appendixSpecs[i].hash {
					return upstream, func() {}, nil
				}
			}

			// No appendix change occurred, so we remove the appendix
			// on the "latest" upstream which will be added back
			// before the conjoin completes
			upstream, appendixSpecs = upstream.removeAppendixSpecs()
		}

		conjoineeSet := map[hash.Hash]struct{}{}
		upstreamNames := map[hash.Hash]struct{}{}
		for _, spec := range upstream.specs {
			upstreamNames[spec.hash] = struct{}{}
		}
		for _, c := range conjoinees {
			if _, present := upstreamNames[c.hash]; !present {
				return upstream, func() {}, nil // Bail!
			}
			conjoineeSet[c.hash] = struct{}{}
		}

		// Filter conjoinees out of upstream.specs to generate new set of keepers
		keepers = make([]tableSpec, 0, len(upstream.specs)-len(conjoinees))
		for _, spec := range upstream.specs {
			if _, present := conjoineeSet[spec.hash]; !present {
				keepers = append(keepers, spec)
			}
		}
	}
}

func conjoinTables(ctx context.Context, conjoinees []tableSpec, p tablePersister, stats *Stats) (conjoined tableSpec, cleanup cleanupFunc, err error) {
	eg, ectx := errgroup.WithContext(ctx)
	toConjoin := make(chunkSources, len(conjoinees))

	for idx := range conjoinees {
		i, spec := idx, conjoinees[idx]
		eg.Go(func() (err error) {
			toConjoin[i], err = p.Open(ectx, spec.hash, spec.chunkCount, stats)
			return
		})
	}
	defer func() {
		for _, cs := range toConjoin {
			if cs != nil {
				cs.close()
			}
		}
	}()
	if err = eg.Wait(); err != nil {
		return tableSpec{}, nil, err
	}

	t1 := time.Now()

	conjoinedSrc, cleanup, err := p.ConjoinAll(ctx, toConjoin, stats)
	if err != nil {
		return tableSpec{}, nil, err
	}
	defer conjoinedSrc.close()

	stats.ConjoinLatency.SampleTimeSince(t1)
	stats.TablesPerConjoin.SampleLen(len(toConjoin))

	cnt, err := conjoinedSrc.count()
	if err != nil {
		return tableSpec{}, nil, err
	}

	stats.ChunksPerConjoin.Sample(uint64(cnt))

	h := conjoinedSrc.hash()
	cnt, err = conjoinedSrc.count()
	if err != nil {
		return tableSpec{}, nil, err
	}
	return tableSpec{TypeNoms, h, cnt}, cleanup, nil
}

func toSpecs(srcs chunkSources) ([]tableSpec, error) {
	specs := make([]tableSpec, len(srcs))
	for i, src := range srcs {
		cnt, err := src.count()
		if err != nil {
			return nil, err
		} else if cnt <= 0 {
			return nil, errors.New("invalid table spec has no sources")
		}

		h := src.hash()
		cnt, err = src.count()
		if err != nil {
			return nil, err
		}
		specs[i] = tableSpec{TypeNoms, h, cnt}
	}

	return specs, nil
}
