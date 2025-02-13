// Copyright 2025 Dolthub, Inc.
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

package jobqueue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSerialQueue(t *testing.T) {
	t.Run("CanceledRunContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		queue := NewSerialQueue()
		// This  should return.
		queue.Run(ctx)
		// Now all methods should return ErrCompletedQueue.
		assert.ErrorIs(t, queue.Start(), ErrCompletedQueue)
		assert.ErrorIs(t, queue.Pause(), ErrCompletedQueue)
		assert.ErrorIs(t, queue.Stop(), ErrCompletedQueue)
		assert.ErrorIs(t, queue.DoSync(context.Background(), func() {}), ErrCompletedQueue)
		assert.ErrorIs(t, queue.DoAsync(func() {}), ErrCompletedQueue)
		assert.ErrorIs(t, queue.InterruptSync(context.Background(), func() {}), ErrCompletedQueue)
		assert.ErrorIs(t, queue.InterruptAsync(func() {}), ErrCompletedQueue)
	})
	t.Run("StartsRunning", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		var ran bool
		err := queue.DoSync(context.Background(), func() {
			ran = true
		})
		assert.NoError(t, err)
		assert.True(t, ran, "the sync task ran.")
		cancel()
		wg.Wait()
	})
	t.Run("StoppedQueueReturnsError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		assert.NoError(t, queue.Stop())
		err := queue.DoSync(context.Background(), func() {})
		assert.ErrorIs(t, err, ErrStoppedQueue)
		cancel()
		wg.Wait()
	})
	t.Run("PausedQueueDoesNotRun", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		assert.NoError(t, queue.Pause())
		var ran bool
		for i := 0; i < 16; i++ {
			err := queue.DoAsync(func() {
				ran = true
			})
			assert.NoError(t, err)
		}
		cancel()
		wg.Wait()
		assert.False(t, ran, "work did not run on the paused queue.")
	})
	t.Run("StartingPausedQueueRunsIt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		assert.NoError(t, queue.Pause())
		var ran bool
		for i := 0; i < 16; i++ {
			err := queue.DoAsync(func() {
				ran = true
			})
			assert.NoError(t, err)
		}
		assert.NoError(t, queue.Start())
		err := queue.DoSync(context.Background(), func() {})
		assert.NoError(t, err)
		assert.True(t, ran, "work ran after the paused queue was started.")
		cancel()
		wg.Wait()
	})
	t.Run("InterruptWorkRunsFirst", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		assert.NoError(t, queue.Pause())
		var cnt int
		queue.DoAsync(func() {
			assert.Equal(t, cnt, 2)
			cnt += 1
		})
		queue.DoAsync(func() {
			assert.Equal(t, cnt, 3)
			cnt += 1
		})
		queue.InterruptAsync(func() {
			assert.Equal(t, cnt, 0)
			cnt += 1
		})
		queue.InterruptAsync(func() {
			assert.Equal(t, cnt, 1)
			cnt += 1
		})
		assert.NoError(t, queue.Start())
		assert.NoError(t, queue.DoSync(context.Background(), func() {}))
		assert.Equal(t, cnt, 4)
		cancel()
		wg.Wait()
	})
	t.Run("StopFromQueue", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		var cnt int
		for i := 0; i < 16; i++ {
			// Some of these calls my error, since the queue
			// will be stopped asynchronously.
			queue.DoAsync(func() {
				cnt += 1
				assert.NoError(t, queue.Stop())
			})
		}
		assert.Equal(t, cnt, 1)
		cancel()
		wg.Wait()
	})
	t.Run("PauseFromQueue", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		var cnt int
		for i := 0; i < 16; i++ {
			err := queue.DoAsync(func() {
				cnt += 1
				assert.NoError(t, queue.Pause())
			})
			assert.NoError(t, err)
		}
		assert.Equal(t, cnt, 1)
		cancel()
		wg.Wait()
	})
	t.Run("PurgeFromQueue", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		assert.NoError(t, queue.Pause())
		var cnt int
		didRun := make(chan struct{})
		for i := 0; i < 16; i++ {
			err := queue.DoAsync(func() {
				cnt += 1
				assert.NoError(t, queue.Purge())
				close(didRun)
			})
			assert.NoError(t, err)
		}
		assert.NoError(t, queue.Start())
		<-didRun
		assert.NoError(t, queue.DoSync(context.Background(), func() {}))
		assert.Equal(t, cnt, 1)
		cancel()
		wg.Wait()
	})
	t.Run("DoSyncInQueueDeadlockWithContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		var cnt int
		err := queue.DoSync(context.Background(), func() {
			cnt += 1
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err := queue.DoSync(ctx, func() {
				cnt += 1
			})
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})
		assert.NoError(t, err)
		assert.NoError(t, queue.DoSync(context.Background(), func() {}))
		// Both tasks eventually ran...
		assert.Equal(t, cnt, 2)
		cancel()
		wg.Wait()
	})
	t.Run("SyncReturnsErrCompletedQueueAfterWorkAccepted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			queue.Run(ctx)
		}()
		queue.Pause()
		var err error
		var ran bool
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = queue.InterruptSync(context.Background(), func() {
				ran = true
			})
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond)
			queue.Stop()
		}()
		cancel()
		wg.Wait()
		assert.ErrorIs(t, err, ErrCompletedQueue)
		assert.False(t, ran, "the interrupt task never ran.")
	})
}
