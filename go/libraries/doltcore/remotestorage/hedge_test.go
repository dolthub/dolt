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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func init() {
	MaxHedgesPerRequest = 1024
}

func TestPercentileStrategy(t *testing.T) {
	s := NewPercentileStrategy(0, 1*time.Hour, 95.0)
	for i := 0; i < 90; i++ {
		s.Observe(1, 1, 1*time.Millisecond, nil)
	}
	for i := 0; i < 10; i++ {
		s.Observe(10, 1, 100*time.Millisecond, nil)
	}
	d := s.Duration(10)
	assert.True(t, d > 90*time.Millisecond, "p95 is greater than 90 milliseconds, is %d", d)
}

func TestMinStrategy(t *testing.T) {
	u := NewPercentileStrategy(0, 1*time.Hour, 95.0)
	s := NewMinStrategy(1*time.Second, u)
	d := s.Duration(10)
	assert.Equal(t, d, 1*time.Second)
	for i := 0; i < 100; i++ {
		s.Observe(10, 1, 15*time.Second, nil)
	}
	d = s.Duration(10)
	assert.NotEqual(t, d, 1*time.Second)
}

func TestHedgerRunsWork(t *testing.T) {
	h := NewHedger(1, NewMinStrategy(1*time.Second, nil))
	ran := false
	i, err := h.Do(context.Background(), Work{
		Work: func(ctx context.Context, n int) (interface{}, error) {
			ran = true
			return true, nil
		},
	})
	assert.NoError(t, err)
	assert.True(t, ran)
	assert.True(t, i.(bool))
}

func TestHedgerHedgesWork(t *testing.T) {
	h := NewHedger(1, NewMinStrategy(10*time.Millisecond, nil))
	ch := make(chan int, 2)
	ch <- 1
	ch <- 2
	i, err := h.Do(context.Background(), Work{
		Work: func(ctx context.Context, n int) (interface{}, error) {
			i := <-ch
			if i == 1 {
				<-ctx.Done()
				close(ch)
				return 1, nil
			} else if i == 2 {
				return 2, nil
			}
			panic("unexpected value in ch")
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, i.(int))
	assert.Equal(t, 0, <-ch)
}

// Behaves a bit like a WaitGroup but allows Done() to be called more than its
// configured count and after Wait() has already returned.
type sloppyWG struct {
	cnt int32
	ch  chan struct{}
}

func newSloppyWG(i int32) *sloppyWG {
	return &sloppyWG{i, make(chan struct{})}
}

func (w *sloppyWG) Done() {
	cur := atomic.AddInt32(&w.cnt, -1)
	if cur == 0 {
		close(w.ch)
	}
}

func (w *sloppyWG) Wait() {
	<-w.ch
}

func TestHedgerObeysMaxHedges(t *testing.T) {
	try := func(max int) {
		h := NewHedger(int64(max), NewMinStrategy(1*time.Millisecond, nil))
		cnt := int32(0)
		wg := newSloppyWG(int32(max + 4))
		h.after = func(d time.Duration) <-chan time.Time {
			wg.Done()
			return time.After(d)
		}
		i, err := h.Do(context.Background(), Work{
			Work: func(ctx context.Context, n int) (interface{}, error) {
				cur := atomic.AddInt32(&cnt, 1)
				if cur == int32(max)+1 {
					wg.Wait()
					return 1, nil
				} else if cur > int32(max)+1 {
					panic("should not hedge more than max")
				} else {
					<-ctx.Done()
					return 2, nil
				}
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, i.(int))
		assert.Equal(t, int32(max)+1, atomic.LoadInt32(&cnt))
	}
	try(1)
	try(2)
	try(3)
}

func TestMaxHedgesPerRequestObeyed(t *testing.T) {
	before := MaxHedgesPerRequest
	defer func() {
		MaxHedgesPerRequest = before
	}()

	MaxHedgesPerRequest = 0
	h := NewHedger(int64(32), NewMinStrategy(1*time.Millisecond, nil))
	cnt := int32(0)
	wg := newSloppyWG(4)
	h.after = func(d time.Duration) <-chan time.Time {
		wg.Done()
		return time.After(d)
	}
	i, err := h.Do(context.Background(), Work{
		Work: func(ctx context.Context, n int) (interface{}, error) {
			cur := atomic.AddInt32(&cnt, 1)
			if cur == int32(1) {
				wg.Wait()
				return 1, nil
			} else {
				panic("should not hedge more than MaxHedgesPerRequest")
			}
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, i.(int))

	MaxHedgesPerRequest = 1
	cnt = int32(0)
	wg = newSloppyWG(7)
	h.after = func(d time.Duration) <-chan time.Time {
		wg.Done()
		return time.After(d)
	}
	i, err = h.Do(context.Background(), Work{
		Work: func(ctx context.Context, n int) (interface{}, error) {
			cur := atomic.AddInt32(&cnt, 1)
			if cur == int32(1) {
				<-ctx.Done()
				return 1, nil
			} else if cur == int32(2) {
				wg.Wait()
				return 2, nil
			} else {
				panic("should not hedge more than MaxHedgesPerRequest")
			}
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, i.(int))
}

func TestHedgerContextCancelObeyed(t *testing.T) {
	h := NewHedger(int64(3), NewMinStrategy(1*time.Millisecond, nil))
	resCh := make(chan struct{})
	canCh := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-canCh
		<-canCh
		<-canCh
		<-canCh
		cancel()
	}()
	_, err := h.Do(ctx, Work{
		Work: func(ctx context.Context, n int) (interface{}, error) {
			canCh <- struct{}{}
			<-ctx.Done()
			resCh <- struct{}{}
			return nil, nil
		},
	})
	assert.Error(t, err, context.Canceled)
	<-resCh
	<-resCh
	<-resCh
	<-resCh
}
