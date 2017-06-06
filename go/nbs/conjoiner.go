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
	"github.com/jpillora/backoff"
)

type conjoiner interface {
	// ConjoinRequired tells the caller whether or not it's time to request a
	// Conjoin, based upon the contents of |ts| and the conjoiner
	// implementation's policy. Implementations must be goroutine-safe.
	ConjoinRequired(ts tableSet) bool

	// Conjoin attempts to use |p| to conjoin some number of tables referenced
	// by |mm|, allowing it to update |mm| with a new, smaller, set of tables
	// that references precisely the same set of chunks. Conjoin() may not
	// actually conjoin any upstream tables, usually because some out-of-
	// process actor has already landed a conjoin of its own. Callers must
	// handle this, likely by rebasing against upstream and re-evaluating the
	// situation.
	// Before performing the conjoin, implementations should verify that a
	// conjoin is actually currently needed; callers may be working with an
	// out-of-date notion of upstream state. |novelCount|, the number of new
	// tables the caller is trying to land, may be used in this determination.
	// Implementations must be goroutine-safe.
	Conjoin(mm manifest, p tablePersister, novelCount int, stats *Stats)
}

func newAsyncConjoiner(maxTables int) *asyncConjoiner {
	return &asyncConjoiner{
		waiters:   map[string]chan struct{}{},
		maxTables: maxTables,
	}
}

type asyncConjoiner struct {
	mu        sync.RWMutex
	waiters   map[string]chan struct{}
	maxTables int
}

func (c *asyncConjoiner) ConjoinRequired(ts tableSet) bool {
	return ts.Size() > c.maxTables
}

// Conjoin checks to see if there's already a conjoin underway for the store
// described by |mm|. If so, it blocks until that conjoin completes. If not,
// it starts one and blocks until it completes. Conjoin can be called
// concurrently from many goroutines.
func (c *asyncConjoiner) Conjoin(mm manifest, p tablePersister, novelCount int, stats *Stats) {
	needsConjoin := func(upstreamCount int) bool {
		return upstreamCount+novelCount > c.maxTables
	}
	c.await(mm.Name(), func() { conjoin(mm, p, needsConjoin, stats) }, nil)
	return
}

// await checks to see if there's already something running for |id| and, if
// so, waits for it to complete. If not, it runs f() and waits for it to
// complete. While f() is running, other callers to await that pass in |id|
// will block until f() completes.
func (c *asyncConjoiner) await(id string, f func(), testWg *sync.WaitGroup) {
	wait := func() <-chan struct{} {
		c.mu.Lock()
		defer c.mu.Unlock()

		if ch, present := c.waiters[id]; present {
			return ch
		}
		c.waiters[id] = make(chan struct{})
		go c.runAndNotify(id, f)
		return c.waiters[id]
	}()
	if testWg != nil {
		testWg.Done()
	}
	<-wait
}

// runAndNotify runs f() and, upon completion, signals everyone who called
// await(id).
func (c *asyncConjoiner) runAndNotify(id string, f func()) {
	f()

	c.mu.Lock()
	defer c.mu.Unlock()
	ch, present := c.waiters[id]
	d.PanicIfFalse(present)
	close(ch)
	delete(c.waiters, id)
}

func conjoin(mm manifest, p tablePersister, needsConjoin func(int) bool, stats *Stats) {
	b := &backoff.Backoff{
		Min:    128 * time.Microsecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: true,
	}

	exists, upstream := mm.ParseIfExists(stats, nil)
	d.PanicIfFalse(exists)
	// This conjoin may have been requested by someone with an out-of-date notion of what's upstream. Verify that we actually still believe a conjoin is needed and, if not, return early
	if !needsConjoin(len(upstream.specs)) {
		return
	}

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
			return
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
				return // Bail!
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
		time.Sleep(b.Duration())
	}
}

func conjoinTables(p tablePersister, upstream []tableSpec, stats *Stats) (conjoined tableSpec, conjoinees, keepers []tableSpec) {
	// Open all the upstream tables concurrently
	sources := make(chunkSources, len(upstream))
	wg := sync.WaitGroup{}
	for i, spec := range upstream {
		wg.Add(1)
		go func(idx int, spec tableSpec) {
			sources[idx] = p.Open(spec.name, spec.chunkCount)
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
