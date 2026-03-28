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
	"fmt"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/hash"
)

type conjoinStrategy interface {
	// conjoinRequired returns true if |conjoin| should be called.
	conjoinRequired(ts *tableSet) bool

	// chooseConjoinees chooses which chunkSources to conjoin from |sources|
	chooseConjoinees(specs []tableSpec) (conjoinees []tableSpec, err error)
}

type inlineConjoiner struct {
	maxTables int
}

var _ conjoinStrategy = inlineConjoiner{}

func (c inlineConjoiner) conjoinRequired(ts *tableSet) bool {
	return ts.Size() > c.maxTables && len(ts.upstream) >= 2
}

// chooseConjoinees implements conjoinStrategy. Current approach is to choose the smallest N tables which,
// when removed and replaced with the conjoinment, will leave the conjoinment as the smallest table.
// We also keep taking table files until we get below maxTables.
func (c inlineConjoiner) chooseConjoinees(upstream []tableSpec) (conjoinees []tableSpec, err error) {
	if c.maxTables < 2 {
		return nil, fmt.Errorf("runtime error: cannot conjoin with maxTables set to %d", c.maxTables)
	}
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
			if len(sorted)-i < c.maxTables {
				break
			}
		}
		sum += next
		i++
	}
	return sorted[:i], nil
}

type noopConjoiner struct{}

var _ conjoinStrategy = noopConjoiner{}

func (c noopConjoiner) conjoinRequired(ts *tableSet) bool {
	return false
}

func (c noopConjoiner) chooseConjoinees(sources []tableSpec) (conjoinees []tableSpec, err error) {
	return
}

// specificFilesConjoiner is a conjoin strategy that conjoins specific storage files
type specificFilesConjoiner struct {
	targetStorageIds []hash.Hash
}

var _ conjoinStrategy = &specificFilesConjoiner{}

func (s *specificFilesConjoiner) conjoinRequired(ts *tableSet) bool {
	return len(s.targetStorageIds) > 0
}

func (s *specificFilesConjoiner) chooseConjoinees(specs []tableSpec) (conjoinees []tableSpec, err error) {
	targetSet := make(map[hash.Hash]bool)
	for _, id := range s.targetStorageIds {
		targetSet[id] = true
	}
	for _, spec := range specs {
		if targetSet[spec.name] {
			conjoinees = append(conjoinees, spec)
		}
	}
	return conjoinees, nil
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
	// Anything to run as cleanup after we complete successfully.
	// This comes directly from persister.ConjoinAll, but needs to
	// be run after the manifest update lands successfully.
	cleanup cleanupFunc
	// The computed things we conjoined in |conjoin|.
	conjoinees []tableSpec
	// The tableSpec for the conjoined file.
	conjoined tableSpec
	// An open chunkSource for the conjoined file. Held open so that
	// finalizeConjoin can pass it directly to the tableSet rebase as a
	// pre-opened source, avoiding a redundant round-trip through the
	// table persister. The caller of |conjoin| is responsible for closing it.
	conjoinedSrc chunkSource
}

// Compute what we will conjoin and prepare to do it. This should be
// done synchronously and with the Mutex held by NomsBlockStore.
func (op *conjoinOperation) prepareConjoin(ctx context.Context, strat conjoinStrategy, upstream manifestContents) error {
	if upstream.NumAppendixSpecs() != 0 {
		upstream, _ = upstream.removeAppendixSpecs()
	}
	var err error
	op.conjoinees, err = strat.chooseConjoinees(upstream.specs)
	if err != nil {
		return err
	}
	return nil
}

// Actually runs persister.ConjoinAll, after conjoinees are chosen by
// |prepareConjoin|.  This should be done asynchronously by
// NomsBlockStore.
//
// On success, op.conjoinedSrc is set to an open chunkSource for the
// conjoined file. The caller is responsible for closing it.
func (op *conjoinOperation) conjoin(ctx context.Context, behavior dherrors.FatalBehavior, persister tablePersister, stats *Stats) error {
	var err error
	op.conjoined, op.conjoinedSrc, op.cleanup, err = conjoinTables(ctx, behavior, op.conjoinees, persister, stats)
	if err != nil {
		return err
	}
	return nil
}

// computeNewContents returns the manifest contents that would result from applying
// this conjoin operation to |upstream|. Returns (newContents, true) if the conjoin
// can be applied — all conjoinees are present in upstream. Returns (upstream, false)
// if the conjoin cannot be applied (conjoinees are missing or already replaced).
func (op *conjoinOperation) computeNewContents(upstream manifestContents) (manifestContents, bool) {
	if len(op.conjoinees) == 0 {
		return upstream, false
	}
	conjoineeSet := toSpecSet(op.conjoinees)
	upstreamSet := toSpecSet(upstream.specs)
	for h := range conjoineeSet {
		if _, ok := upstreamSet[h]; !ok {
			return upstream, false
		}
	}
	newSpecs := make([]tableSpec, len(upstream.specs)-len(conjoineeSet)+1)
	ins := 0
	for i, s := range upstream.specs {
		if _, ok := conjoineeSet[s.name]; !ok {
			newSpecs[ins] = s
			ins++
		}
		if i == len(upstream.appendix) {
			newSpecs[ins] = op.conjoined
			ins++
		}
	}
	return manifestContents{
		nbfVers:  upstream.nbfVers,
		root:     upstream.root,
		lock:     generateLockHash(upstream.root, newSpecs, upstream.appendix, nil),
		gcGen:    upstream.gcGen,
		specs:    newSpecs,
		appendix: upstream.appendix,
	}, true
}

// apply atomically builds a new tableSet and updates the manifest to replace the
// conjoinees with the conjoined file. It retries on optimistic lock failures,
// re-building the tableSet each time.
//
// Returns (newUpstream, newTables, nil) when the conjoin was applied successfully.
// The caller must call op.cleanup() after assigning the returned newTables to
// nbs.tables and closing the replaced *tableSet.
//
// Returns (upstream, nil, nil) when the conjoin is not applicable — the conjoinees
// are no longer all present in the manifest (e.g. a concurrent writer already
// handled them). No manifest or table-set changes are made.
//
// Returns (manifestContents{}, nil, err) on any other error.
func (op *conjoinOperation) apply(ctx context.Context, behavior dherrors.FatalBehavior, upstream manifestContents, ts *tableSet, mm manifestUpdater, stats *Stats) (newUpstream manifestContents, newTables *tableSet, err error) {
	// If op.conjoin succeeded, conjoinedSrc is an open handle for the new file.
	// Pass it as a pre-opened source so the rebase can clone it rather than
	// calling p.Open() again. The nil guard handles the case where apply is
	// invoked without going through op.conjoin (e.g. from the test harness).
	var sources chunkSourceSet
	if op.conjoinedSrc != nil {
		sources = chunkSourceSet{op.conjoined.name: op.conjoinedSrc}
	}

	for {
		newContents, canApply := op.computeNewContents(upstream)
		if !canApply {
			return upstream, nil, nil
		}

		newTables, err = ts.rebase(ctx, newContents.specs, sources, stats)
		if err != nil {
			return manifestContents{}, nil, err
		}

		var updatedContents manifestContents
		updatedContents, err = mm.Update(ctx, behavior, upstream.lock, newContents, stats, nil)
		if err != nil {
			_ = newTables.close()
			return manifestContents{}, nil, err
		}

		if newContents.lock == updatedContents.lock {
			return updatedContents, newTables, nil
		}

		// Optimistic lock failed: the manifest was moved by a concurrent writer.
		// Discard the tableSet we just built and retry against the new upstream.
		if closeErr := newTables.close(); closeErr != nil {
			return manifestContents{}, nil, closeErr
		}
		newTables = nil
		upstream = updatedContents
	}
}

func conjoinTables(ctx context.Context, behavior dherrors.FatalBehavior, conjoinees []tableSpec, p tablePersister, stats *Stats) (conjoined tableSpec, conjoinedSrc chunkSource, cleanup cleanupFunc, err error) {
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
		return tableSpec{}, nil, nil, err
	}

	t1 := time.Now()

	conjoinedSrc, cleanup, err = p.ConjoinAll(ctx, behavior, toConjoin, stats)
	if err != nil {
		return tableSpec{}, nil, nil, err
	}
	// conjoinedSrc is returned open; the caller is responsible for closing it
	// after the table set rebase has cloned from it.

	stats.ConjoinLatency.SampleTimeSince(t1)
	stats.TablesPerConjoin.SampleLen(len(toConjoin))

	cnt, err := conjoinedSrc.count()
	if err != nil {
		conjoinedSrc.close()
		return tableSpec{}, nil, nil, err
	}

	stats.ChunksPerConjoin.Sample(uint64(cnt))

	h := conjoinedSrc.hash()
	cnt, err = conjoinedSrc.count()
	if err != nil {
		conjoinedSrc.close()
		return tableSpec{}, nil, nil, err
	}
	return tableSpec{h, cnt}, conjoinedSrc, cleanup, nil
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
