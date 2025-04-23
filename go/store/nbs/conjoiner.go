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
// We also keep taking table files until we get below maxTables.
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
			if c.maxTables == 0 || len(sorted)-i < c.maxTables {
				break
			}
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

// A conjoinOperation is a multi-step process that a NomsBlockStore runs to
// conjoin the table files in the store.
//
// Conjoining the table files in a store involves copying all the data
// from |n| files into a single file, and replacing the entries for
// those table files in the manifest with the single, conjoin table
// file. Conjoining is a periodic maintanence operation which is
// automatically done against NomsBlockStores.
//
// Conjoining a lot of chunks across a number of table files can take
// a long time. On every manifest update, including every Commit,
// NomsBlockStore checks if the store needs conjoining. If it does, it
// starts an ansynchronous process which will create the new table
// file from the table files which have been chosen to be conjoined.
// This process will run in the background until the table file is
// created and in the right place. Then the conjoin finalization will
// take place. When finalizing a conjoin, the manifest contents of the
// store are updated. The conjoin only succeeds if all the table files
// which were conjoined are still in the manifest when we go to update
// it. Otherwise the conjoined table file is deleted and the store can
// try to create a new conjoined file if it is still necessary.
//
// A conjoinOperation is created when a conjoinStrategy |conjoinRequired| returns true.
type conjoinOperation struct {
	// The computed things we conjoined in |conjoin|.
	conjoinees []tableSpec
	// The tableSpec for the conjoined file.
	conjoined tableSpec

	// Anything to run as cleanup after we complete successfully.
	// This comes directly from persister.ConjoinAll, but needs to
	// be run after the manifest update lands successfully.
	cleanup cleanupFunc
}

// Compute what we will conjoin and prepare to do it. This should be
// done synchronously and with the Mutex held by NomsBlockStore.
func (op *conjoinOperation) prepareConjoin(ctx context.Context, strat conjoinStrategy, upstream manifestContents) error {
	if upstream.NumAppendixSpecs() != 0 {
		upstream, _ = upstream.removeAppendixSpecs()
	}
	var err error
	op.conjoinees, _, err = strat.chooseConjoinees(upstream.specs)
	if err != nil {
		return err
	}
	return nil
}

// Actually runs persister.ConjoinAll, after conjoinees are chosen by
// |prepareConjoin|.  This should be done asynchronously by
// NomsBlockStore.
func (op *conjoinOperation) conjoin(ctx context.Context, persister tablePersister, stats *Stats) error {
	var err error
	op.conjoined, op.cleanup, err = conjoinTables(ctx, op.conjoinees, persister, stats)
	if err != nil {
		return err
	}
	return nil
}

// Land the update in the conjoin result in the manifest as an update
// which removes the conjoinees and adds the conjoined. Only updates
// the manifest by adding the conjoined file if all conjoinees are
// still present in the manifest.
//
// Whether the conjoined file lands or not, this returns a nil error
// if it runs to completion successfully and it returns a cleanupFunc
// which should be run.
func (op *conjoinOperation) updateManifest(ctx context.Context, upstream manifestContents, mm manifestUpdater, stats *Stats) (manifestContents, cleanupFunc, error) {
	conjoineeSet := toSpecSet(op.conjoinees)
	for {
		upstreamSet := toSpecSet(upstream.specs)
		canApply := true
		alreadyApplied := false
		for h := range conjoineeSet {
			if _, ok := upstreamSet[h]; !ok {
				canApply = false
				break
			}
		}
		if canApply {
			newSpecs := make([]tableSpec, len(upstream.specs)-len(conjoineeSet)+1)
			ins := 0
			for i, s := range upstream.specs {
				if _, ok := conjoineeSet[s.name]; !ok {
					newSpecs[ins] = s
					ins += 1
				}
				if i == len(upstream.appendix) {
					newSpecs[ins] = op.conjoined
					ins += 1
				}
			}
			newContents := manifestContents{
				nbfVers:  upstream.nbfVers,
				root:     upstream.root,
				lock:     generateLockHash(upstream.root, newSpecs, upstream.appendix, nil),
				gcGen:    upstream.gcGen,
				specs:    newSpecs,
				appendix: upstream.appendix,
			}

			updated, err := mm.Update(ctx, upstream.lock, newContents, stats, nil)
			if err != nil {
				return manifestContents{}, func() {}, err
			}

			if newContents.lock == updated.lock {
				return updated, op.cleanup, nil
			}

			// Go back around the loop, trying to apply against the new upstream.
			upstream = updated
		} else {
			if _, ok := upstreamSet[op.conjoined.name]; ok {
				alreadyApplied = true
			}
			if !alreadyApplied {
				// In theory we could delete the conjoined
				// table file here, since its conjoinees are
				// no longer in the manifest and it itself is
				// not in the manifest either.
				//
				// tablePersister does not expose a
				// functionality to prune it, and it will get
				// picked up by GC anyway, so we do not do
				// that here.
				return upstream, func() {}, nil
			} else {
				return upstream, func() {}, nil
			}
		}
	}
}

// conjoin attempts to use |p| to conjoin some number of tables referenced
// by |upstream|, allowing it to update |mm| with a new, smaller, set of tables
// that references precisely the same set of chunks. Conjoin() may not
// actually conjoin any upstream tables, usually because some out-of-
// process actor has already landed a conjoin of its own. Callers must
// handle this, likely by rebasing against upstream and re-evaluating the
// situation.
func conjoin(ctx context.Context, s conjoinStrategy, upstream manifestContents, mm manifestUpdater, p tablePersister, stats *Stats) (manifestContents, cleanupFunc, error) {
	var op conjoinOperation
	err := op.prepareConjoin(ctx, s, upstream)
	if err != nil {
		return manifestContents{}, nil, err
	}
	err = op.conjoin(ctx, p, stats)
	if err != nil {
		return manifestContents{}, nil, err
	}
	return op.updateManifest(ctx, upstream, mm, stats)
}

func conjoinTables(ctx context.Context, conjoinees []tableSpec, p tablePersister, stats *Stats) (conjoined tableSpec, cleanup cleanupFunc, err error) {
	eg, ectx := errgroup.WithContext(ctx)
	toConjoin := make(chunkSources, len(conjoinees))

	for idx := range conjoinees {
		i, spec := idx, conjoinees[idx]
		eg.Go(func() (err error) {
			toConjoin[i], err = p.Open(ectx, spec.name, spec.chunkCount, stats)
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
	return tableSpec{h, cnt}, cleanup, nil
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
		specs[i] = tableSpec{h, cnt}
	}

	return specs, nil
}
