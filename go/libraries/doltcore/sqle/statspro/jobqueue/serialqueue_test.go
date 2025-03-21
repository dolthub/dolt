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
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSerialQueue(t *testing.T) {
	if runtime.GOOS == "windows" && os.Getenv("CI") != "" {
		t.Skip("Racy on Windows CI")
	}
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
		assert.ErrorIs(t, queue.DoSync(context.Background(), func() error { return nil }), ErrCompletedQueue)
		assert.ErrorIs(t, queue.DoAsync(func() error { return nil }), ErrCompletedQueue)
		assert.ErrorIs(t, queue.InterruptSync(context.Background(), func() error { return nil }), ErrCompletedQueue)
		assert.ErrorIs(t, queue.InterruptAsync(func() error { return nil }), ErrCompletedQueue)
	})
	t.Run("StartsRunning", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() error {
			defer wg.Done()
			queue.Run(ctx)
			return nil
		}()
		var ran bool
		err := queue.DoSync(context.Background(), func() error {
			ran = true
			return nil
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
		go func() error {
			defer wg.Done()
			queue.Run(ctx)
			return nil
		}()
		assert.NoError(t, queue.Stop())
		err := queue.DoSync(context.Background(), func() error { return nil })
		assert.ErrorIs(t, err, ErrStoppedQueue)
		cancel()
		wg.Wait()
	})
	t.Run("PausedQueueDoesNotRun", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() error {
			defer wg.Done()
			queue.Run(ctx)
			return nil
		}()
		assert.NoError(t, queue.Pause())
		var ran bool
		for i := 0; i < 16; i++ {
			err := queue.DoAsync(func() error {
				ran = true
				return nil
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
		go func() error {
			defer wg.Done()
			queue.Run(ctx)
			return nil
		}()
		assert.NoError(t, queue.Pause())
		var ran bool
		for i := 0; i < 16; i++ {
			err := queue.DoAsync(func() error {
				ran = true
				return nil
			})
			assert.NoError(t, err)
		}
		assert.NoError(t, queue.Start())
		err := queue.DoSync(context.Background(), func() error { return nil })
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
		go func() error {
			defer wg.Done()
			queue.Run(ctx)
			return nil
		}()
		assert.NoError(t, queue.Pause())
		var cnt int
		queue.DoAsync(func() error {
			assert.Equal(t, cnt, 2)
			cnt += 1
			return nil
		})
		queue.DoAsync(func() error {
			assert.Equal(t, cnt, 3)
			cnt += 1
			return nil
		})
		queue.InterruptAsync(func() error {
			assert.Equal(t, cnt, 0)
			cnt += 1
			return nil
		})
		queue.InterruptAsync(func() error {
			assert.Equal(t, cnt, 1)
			cnt += 1
			return nil
		})
		assert.NoError(t, queue.Start())
		assert.NoError(t, queue.DoSync(context.Background(), func() error { return nil }))
		assert.Equal(t, cnt, 4)
		cancel()
		wg.Wait()
	})
	t.Run("StopFromQueue", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() error {
			defer wg.Done()
			queue.Run(ctx)
			return nil
		}()
		// block until queue is running
		assert.NoError(t, queue.DoSync(ctx, func() error {
			return nil
		}))
		var cnt int
		for i := 0; i < 16; i++ {
			// Some of these calls may error, since the queue
			// will be stopped asynchronously.
			queue.DoAsync(func() error {
				cnt += 1
				assert.NoError(t, queue.Stop())
				return nil
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
		go func() error {
			defer wg.Done()
			queue.Run(ctx)
			return nil
		}()
		// block until queue is running
		assert.NoError(t, queue.DoSync(ctx, func() error {
			return nil
		}))

		done := make(chan struct{})
		for i := 0; i < 16; i++ {
			err := queue.DoAsync(func() error {
				close(done)
				assert.NoError(t, queue.Pause())
				return nil
			})
			assert.NoError(t, err)
		}
		<-done
		cancel()
		wg.Wait()
	})
	t.Run("PurgeFromQueue", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		var wg sync.WaitGroup
		wg.Add(1)

		go func() error {
			defer wg.Done()
			queue.Run(ctx)
			return nil
		}()

		assert.NoError(t, queue.Pause())
		var cnt int
		didRun := make(chan struct{})
		for i := 0; i < 16; i++ {
			err := queue.DoAsync(func() error {
				cnt += 1
				assert.NoError(t, queue.Purge())
				close(didRun)
				return nil
			})
			assert.NoError(t, err)
		}
		assert.NoError(t, queue.Start())
		<-didRun
		assert.NoError(t, queue.DoSync(context.Background(), func() error { return nil }))
		assert.Equal(t, cnt, 1)
		cancel()
		wg.Wait()
	})
	t.Run("DoSyncInQueueDeadlockWithContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		start := make(chan struct{})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() error {
			defer wg.Done()
			close(start)
			queue.Run(ctx)
			return nil
		}()
		<-start
		var cnt int
		err := queue.DoSync(context.Background(), func() error {
			cnt += 1
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err := queue.DoSync(ctx, func() error {
				cnt += 1
				return nil
			})
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			return nil
		})
		assert.NoError(t, err)
		assert.NoError(t, queue.DoSync(context.Background(), func() error { return nil }))
		// Both tasks eventually ran...
		assert.Equal(t, cnt, 2)
		cancel()
		wg.Wait()
	})
	t.Run("SyncReturnsErrCompletedQueueAfterWorkAccepted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := NewSerialQueue()
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() error {
			defer wg.Done()
			close(start)
			queue.Run(ctx)
			return nil
		}()
		<-start
		queue.Pause()
		var err error
		var ran bool
		wg.Add(1)
		go func() error {
			defer wg.Done()
			err = queue.InterruptSync(context.Background(), func() error {
				ran = true
				return nil
			})
			return nil
		}()
		wg.Add(1)
		go func() error {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond)
			queue.Stop()
			return nil
		}()
		cancel()
		wg.Wait()
		assert.ErrorIs(t, err, ErrCompletedQueue)
		assert.False(t, ran, "the interrupt task never ran.")
	})
	t.Run("RateLimitWorkThroughput", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		queue := NewSerialQueue()
		running := make(chan struct{})
		go func() {
			close(running)
			queue.Run(ctx)
		}()
		<-running

		// first will run because timeout > job rate
		ran := false
		subCtx, cancel2 := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel2()
		err := queue.DoSync(subCtx, func() error {
			ran = true
			return nil
		})
		assert.NoError(t, err)
		assert.True(t, ran, "the interrupt task never ran.")

		// second timeout < jobrate, will fail
		queue.NewRateLimit(10 * time.Millisecond)
		ran = false
		subCtx, cancel3 := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel3()
		err = queue.DoSync(subCtx, func() error {
			ran = true
			return nil
		})
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, ran, "the interrupt task never ran.")
	})
}
