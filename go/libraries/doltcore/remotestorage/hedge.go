// Copyright 2020 Dolthub, Inc.
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

package remotestorage

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Work is a description of work that can be hedged. The supplied Work function
// should expect to potentially be called multiple times concurrently, and it
// should respect |ctx| cancellation. |Size| will be passed to the |Strategy|
// as a parameter to compute the potential hedge retry timeout for this Work.
type Work struct {
	Work func(context.Context) (interface{}, error)
	Size int
}

// Hedger can |Do| |Work|, potentially invoking |Work| more than once
// concurrently if it is taking longer than |Strategy| estimated it would.
type Hedger struct {
	sema  *semaphore.Weighted
	strat Strategy
}

// NewHedger returns a new Hedger. |maxOutstanding| is the most hedged requests
// that can be outstanding. If a request would be hedged, but there are already
// maxOutstanding hedged requests, nothing happens instead.
func NewHedger(maxOutstanding int64, strat Strategy) *Hedger {
	return &Hedger{
		semaphore.NewWeighted(maxOutstanding),
		strat,
	}
}

// Stategy provides a way estimate the hedge timeout for |Work| given to a
// |Hedger|.
type Strategy interface {
	// Return the expect Duration of a piece of Work with |Size| |sz|.
	Duration(sz int) time.Duration
	// Called by |Hedger| when work is completed. |sz| is the |Size| of the
	// work. |n| is the nth hedge which completed first, with 1 being the
	// unhedged request. |d| is the duration the |Work| function took for
	// the request that completed. |err| is any |error| returned from
	// |Work|.
	Observe(sz, n int, d time.Duration, err error)
}

func NewPercentileStrategy(low, high int64, sigfigs int, perc float64) *PercentileStrategy {
	return &PercentileStrategy{
		perc,
		hdrhistogram.New(low, high, sigfigs),
		sync.Mutex{},
	}
}

// PercentileStrategy is a hedge timeout streategy which puts all |Observe|
// durations into a histogram and returns the current value of the provided
// |Percentile| in that histogram for the estimated |Duration|. |Size| is
// ignored.
type PercentileStrategy struct {
	Percentile float64
	histogram  *hdrhistogram.Histogram
	mu         sync.Mutex
}

func (ps *PercentileStrategy) Duration(sz int) time.Duration {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return time.Duration(ps.histogram.ValueAtQuantile(ps.Percentile)) * time.Millisecond
}

func (ps *PercentileStrategy) Observe(sz, n int, d time.Duration, err error) {
	if err == nil {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		ps.histogram.RecordValue(int64(d / time.Millisecond))
	}
}

// MinStrategy is a hedge timeout strategy that optionally delegates to
// |delegate| and replaces the estimated timeout with |min| if it would be less
// than |min|. If |delegate| is |nil|, it is treated as if it always returned
// 0.
func NewMinStrategy(min time.Duration, delegate Strategy) *MinStrategy {
	return &MinStrategy{
		min,
		delegate,
	}
}

type MinStrategy struct {
	Min        time.Duration
	underlying Strategy
}

func (ms *MinStrategy) Duration(sz int) time.Duration {
	if ms.underlying == nil {
		return ms.Min
	}
	u := ms.underlying.Duration(sz)
	if u < ms.Min {
		return ms.Min
	}
	return u
}

func (ms *MinStrategy) Observe(sz, n int, d time.Duration, err error) {
	if ms.underlying != nil {
		ms.underlying.Observe(sz, n, d, err)
	}
}

// Run |w| to completion, potentially spawning concurrent hedge runs of it.
// Returns the results from the first invocation that completes, and cancels
// the contexts of all invocations.
func (h *Hedger) Do(ctx context.Context, w Work) (interface{}, error) {
	var cancels []func()
	type res struct {
		v interface{}
		e error
		n int
		d time.Duration
	}
	ch := make(chan res)
	try := func() {
		n := len(cancels) + 1
		finalize := func() {}
		if n > 1 {
			if !h.sema.TryAcquire(1) {
				// Too many outstanding hedges. Do nothing.
				return
			}
			finalize = func() {
				h.sema.Release(1)
			}
		}
		ctx, cancel := context.WithCancel(ctx)
		cancels = append(cancels, cancel)
		start := time.Now()
		go func() {
			defer finalize()
			v, e := w.Work(ctx)
			select {
			case ch <- res{v, e, n, time.Since(start)}:
			case <-ctx.Done():
			}
		}()
	}
	try()
	for {
		nextTry := h.strat.Duration(w.Size) * (1 << len(cancels))
		select {
		case r := <-ch:
			for _, c := range cancels {
				c()
			}
			h.strat.Observe(w.Size, r.n, r.d, r.e)
			return r.v, r.e
		case <-time.After(nextTry):
			try()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
