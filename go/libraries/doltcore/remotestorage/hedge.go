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

// Hedger can |Do| Work, potentially invoking Work |Run|'s more than once
// concurrently if it is taking longer than |strat|'s |NextTry| duration.
// Completed Work gets reported to the DurationObserver.
type Hedger struct {
	sema     *semaphore.Weighted
	strat    HedgeStrategy
	observer DurationObserver
	after    afterFunc
}

// NewHedger returns a new Hedger. |maxOutstanding| is the most hedged |Run|'s
// that can be outstanding. If a |Run| would be hedged, but there are already
// maxOutstanding hedged |Run|'s, nothing happens instead. |strat| is the
// HedgeStrategy to use for this hedger. |observer| is a DurationObserver that
// will receive |Observe|'s when a Work completes.
func NewHedger(maxOutstanding int64, strat HedgeStrategy, observer DurationObserver) *Hedger {
	return &Hedger{
		semaphore.NewWeighted(maxOutstanding),
		strat,
		observer,
		time.After,
	}
}

type afterFunc func(time.Duration) <-chan time.Time

var MaxHedgesPerRequest = 1

// Do runs |w| to completion, potentially spawning concurrent |Run|'s. Returns
// the results from the first invocation that completes, and cancels the
// contexts of all invocations.
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
		if n-1 > MaxHedgesPerRequest {
			return
		}
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
			v, e := w.Run(ctx, n)
			select {
			case ch <- res{v, e, n, time.Since(start)}:
			case <-ctx.Done():
			}
		}()
	}
	start := time.Now()
	try()
	for {
		nextTry := h.strat.NextTry(w.Size, time.Since(start), len(cancels)+1)
		select {
		case r := <-ch:
			for _, c := range cancels {
				c()
			}
			h.observer.Observe(w.Size, r.n, r.d, r.e)
			return r.v, r.e
		case <-h.after(nextTry):
			try()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Work is a description of work that can be hedged. The supplied Run function
// should expect to potentially be called multiple times concurrently, and it
// should respect |ctx| cancellation. |Size| will be used to estimate Run's
// duration. This estimate is also used to determine when a Work's Run should be
// hedged with concurrent Run call(s).
type Work struct {
	// Run is the function that will be called by |Hedger.Do|. It will be
	// called at least once, and possibly multiple times depending on how
	// long it takes and the |Hedger|'s |Strategy|.
	Run func(ctx context.Context, n int) (interface{}, error)

	// Size is an integer representation of the size of the work.
	// Potentially used by |Strategy|, not used by |Hedger|.
	Size uint64
}

// DynamicEstimator returns an estimated |Duration| for Work that is dynamically
// updated by observations from |Observe|.
type DynamicEstimator interface {
	DurationEstimator
	DurationObserver
}

// DurationEstimator returns an estimated duration given the size of a Work
type DurationEstimator interface {
	// Duration returns the expected |time.Duration| of a Work with |Size| |sz|.
	Duration(sz uint64) time.Duration
}

// DurationObserver observes Work completions
type DurationObserver interface {
	// Observe is called by |Hedger| when work is completed. |sz| is the |Size|
	// of the work. |n| specifies which |Run| called by the Hedger completed,
	// with 1 being the first |Run|. |d| is the duration the |Run| function
	// took for the |Run| that completed. |err| is any |error| returned from
	// |Run|.
	Observe(sz uint64, n int, d time.Duration, err error)
}

// NoopObserver is a DurationObserver has a noop |Observe|
type NoopObserver struct {
}

// NewNoopObserver returns a new NoopObserver
func NewNoopObserver() *NoopObserver {
	return &NoopObserver{}
}

// Observe implements DurationObserver
func (*NoopObserver) Observe(sz uint64, n int, d time.Duration, err error) {
}

// PercentileEstimator is an DynamicEstimator which puts all |Observe| durations
// into a histogram and returns the current value of the provided |Percentile|
// in that histogram for the estimated |Duration|. |sz| is ignored.
type PercentileEstimator struct {
	Percentile float64
	histogram  *hdrhistogram.Histogram
	mu         *sync.Mutex
}

// NewPercentileEstimator returns an initialized |PercentileEstimator|.
func NewPercentileEstimator(low, high time.Duration, perc float64) *PercentileEstimator {
	lowi := int64(low / time.Millisecond)
	highi := int64(high / time.Millisecond)
	return &PercentileEstimator{
		perc,
		hdrhistogram.New(lowi, highi, 3),
		new(sync.Mutex),
	}
}

// Duration implements DurationEstimator.
func (ps *PercentileEstimator) Duration(sz uint64) time.Duration {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return time.Duration(ps.histogram.ValueAtQuantile(ps.Percentile)) * time.Millisecond
}

// Observe implements DurationObserver.
func (ps *PercentileEstimator) Observe(sz uint64, n int, d time.Duration, err error) {
	if err == nil {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		_ = ps.histogram.RecordValue(int64(d / time.Millisecond))
	}
}

// HedgeStrategy is used by Hedger to decide when to launch concurrent |Run|
// calls.
type HedgeStrategy interface {
	// NextTry determines how long to wait before hedging a Work's |Run|
	// function. |sz| is the |Size| of the Work, |elapsed| is the time since the
	// first |Run| was called, and |n| is the hedge number starting from 1.
	NextTry(sz uint64, elapsed time.Duration, n int) time.Duration
}

// FixedHedgeStrategy always returns |FixedNextTry| from |NextTry|
type FixedHedgeStrategy struct {
	FixedNextTry time.Duration
}

// NewFixedHedgeStrategy returns a new FixedHedgeStrategy
func NewFixedHedgeStrategy(fixedNextTry time.Duration) *FixedHedgeStrategy {
	return &FixedHedgeStrategy{fixedNextTry}
}

// NextTry implements HedgeStrategy
func (s *FixedHedgeStrategy) NextTry(sz uint64, elapsed time.Duration, n int) time.Duration {
	return s.FixedNextTry
}

// EstimateStrategy wraps a DurationEstimator. It wants to hedge Work |Run|'s
// when it takes longer than the Work estimate.
type EstimateStrategy struct {
	e DurationEstimator
}

// NewEstimateStrategy returns a new EstimateStrategy
func NewEstimateStrategy(e DurationEstimator) *EstimateStrategy {
	return &EstimateStrategy{e}
}

// NextTry implements HedgeStrategy
func (s *EstimateStrategy) NextTry(sz uint64, elapsed time.Duration, n int) time.Duration {
	return s.e.Duration(sz)
}

// ExponentialHedgeStrategy increases the |underlying|'s nextTry by a factor of
// two for every hedge attempt, including the first attempt.
type ExponentialHedgeStrategy struct {
	underlying HedgeStrategy
}

// NewExponentialHedgeStrategy returns a new ExponentialHedgeStrategy
func NewExponentialHedgeStrategy(u HedgeStrategy) *ExponentialHedgeStrategy {
	return &ExponentialHedgeStrategy{u}
}

// NextTry implements HedgeStrategy
func (e *ExponentialHedgeStrategy) NextTry(sz uint64, elapsed time.Duration, n int) time.Duration {
	estimate := e.underlying.NextTry(sz, elapsed, n)
	return estimate * (1 << n)
}

// MinHedgeStrategy bounds an underlying strategy's NextTry duration to be
// above |min|.
type MinHedgeStrategy struct {
	min        time.Duration
	underlying HedgeStrategy
}

// NewMinHedgeStrategy returns a new MinHedgeStrategy
func NewMinHedgeStrategy(min time.Duration, underlying HedgeStrategy) *MinHedgeStrategy {
	return &MinHedgeStrategy{min, underlying}
}

// NextTry implements HedgeStrategy
func (s *MinHedgeStrategy) NextTry(sz uint64, elapsed time.Duration, n int) time.Duration {
	estimate := s.underlying.NextTry(sz, elapsed, n)
	if estimate < s.min {
		return s.min
	}
	return estimate
}

/*
// MaxStrategy is an example of how multiple strategies can be composed.

type MaxStrategy struct {
	first  HedgeStrategy
	second HedgeStrategy
}

func (s *MaxStrategy) NextTry(sz uint64, elapsed time.Duration, n int) time.Duration {
	e1 := s.first.NextTry(sz, elapsed, n)
	e2 := s.second.NextTry(sz, elapsed, n)
	if e1 > e2 {
		return e1
	} else {
		return e2
	}
}
*/
