// Copyright 2022 Dolthub, Inc.
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

package sqle

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
)

func TestLimiter(t *testing.T) {
	t.Run("NoConcurrency", func(t *testing.T) {
		l := newLimiter()
		for i := 0; i < 16; i++ {
			l.Run(context.Background(), "key_one", func() (any, error) {
				return 1, nil
			})
			l.Run(context.Background(), "key_two", func() (any, error) {
				return 1, nil
			})
			l.Run(context.Background(), "key_three", func() (any, error) {
				return 1, nil
			})
		}
	})
	t.Run("ConcurrentRuns", func(t *testing.T) {
		l := newLimiter()
		var wg sync.WaitGroup
		sema := semaphore.NewWeighted(16)
		sema.Acquire(context.Background(), 16)
		var numRuns int32
		v, err := l.Run(context.Background(), "one", func() (any, error) {
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sema.Release(1)
					v, err := l.Run(context.Background(), "one", func() (any, error) {
						atomic.AddInt32(&numRuns, 1)
						return 2, nil
					})
					assert.NoError(t, err)
					assert.Equal(t, 2, v)
				}()
			}

			// TODO: This is some jank to reduce flakiness.
			sema.Acquire(context.Background(), 16)
			time.Sleep(10 * time.Millisecond)
			for i := 0; i < 128; i++ {
				runtime.Gosched()
			}
			time.Sleep(10 * time.Millisecond)

			return 1, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, v)
		wg.Wait()
		assert.Equal(t, int32(1), numRuns)
	})
}
