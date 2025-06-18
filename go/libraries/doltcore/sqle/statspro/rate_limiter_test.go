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

package statspro

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimpleRateLimiter_Execute(t *testing.T) {
	t.Run("ExecuteWithoutCancellation", func(t *testing.T) {
		rl := newSimpleRateLimiter(50 * time.Millisecond)
		defer rl.stop()

		ctx := context.Background()
		executed := false

		err := rl.execute(ctx, func() error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, executed)
	})

	t.Run("ExecuteWithFunctionError", func(t *testing.T) {
		rl := newSimpleRateLimiter(10 * time.Millisecond)
		defer rl.stop()

		ctx := context.Background()
		expectedErr := errors.New("test error")

		err := rl.execute(ctx, func() error {
			return expectedErr
		})

		require.Equal(t, expectedErr, err)
	})

	t.Run("ExecuteWithCanceledContext", func(t *testing.T) {
		rl := newSimpleRateLimiter(100 * time.Millisecond)
		defer rl.stop()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		executed := false
		err := rl.execute(ctx, func() error {
			executed = true
			return nil
		})

		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
		require.False(t, executed)
	})

	t.Run("ExecuteWithContextCanceledDuringWait", func(t *testing.T) {
		rl := newSimpleRateLimiter(200 * time.Millisecond)
		defer rl.stop()

		ctx, cancel := context.WithCancel(context.Background())

		executed := false
		var wg sync.WaitGroup
		wg.Add(1)

		var err error
		go func() {
			defer wg.Done()
			err = rl.execute(ctx, func() error {
				executed = true
				return nil
			})
		}()

		// Cancel context while rate limiter is waiting
		time.Sleep(50 * time.Millisecond)
		cancel()

		wg.Wait()
		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
		require.False(t, executed)
	})

	t.Run("ExecuteRateLimiting", func(t *testing.T) {
		interval := 100 * time.Millisecond
		rl := newSimpleRateLimiter(interval)
		defer rl.stop()

		ctx := context.Background()
		var executionTimes []time.Time
		var mu sync.Mutex

		var wg sync.WaitGroup
		start := time.Now()

		// Execute multiple functions
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := rl.execute(ctx, func() error {
					mu.Lock()
					executionTimes = append(executionTimes, time.Now())
					mu.Unlock()
					return nil
				})
				require.NoError(t, err)
			}()
		}

		wg.Wait()

		require.Equal(t, 3, len(executionTimes))

		// Check that executions are rate limited
		// First execution should be immediate, subsequent ones should be delayed
		timeSinceStart1 := executionTimes[0].Sub(start)
		timeSinceStart2 := executionTimes[1].Sub(start)
		timeSinceStart3 := executionTimes[2].Sub(start)

		// Allow some tolerance for timing
		tolerance := 20 * time.Millisecond

		require.True(t, timeSinceStart1 < interval+tolerance, "First execution should be immediate")
		require.True(t, timeSinceStart2 >= interval-tolerance, "Second execution should be rate limited")
		require.True(t, timeSinceStart3 >= 2*interval-tolerance, "Third execution should be further rate limited")
	})
}

func TestSimpleRateLimiter_Stop(t *testing.T) {
	t.Run("StopPreventsNewExecution", func(t *testing.T) {
		rl := newSimpleRateLimiter(50 * time.Millisecond)

		// Stop the rate limiter
		rl.stop()

		ctx := context.Background()
		executed := false

		err := rl.execute(ctx, func() error {
			executed = true
			return nil
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "rate limiter stopped")
		require.False(t, executed)
	})

	t.Run("StopInterruptsWaitingExecution", func(t *testing.T) {
		rl := newSimpleRateLimiter(200 * time.Millisecond)

		ctx := context.Background()
		executed := false
		var wg sync.WaitGroup
		wg.Add(1)

		var err error
		go func() {
			defer wg.Done()
			err = rl.execute(ctx, func() error {
				executed = true
				return nil
			})
		}()

		// Stop rate limiter while execution is waiting
		time.Sleep(50 * time.Millisecond)
		rl.stop()

		wg.Wait()
		require.Error(t, err)
		require.Contains(t, err.Error(), "rate limiter stopped")
		require.False(t, executed)
	})

	t.Run("StopIsIdempotent", func(t *testing.T) {
		rl := newSimpleRateLimiter(50 * time.Millisecond)

		// Stop multiple times should not panic or cause issues
		rl.stop()
		rl.stop()
		rl.stop()

		ctx := context.Background()
		executed := false

		err := rl.execute(ctx, func() error {
			executed = true
			return nil
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "rate limiter stopped")
		require.False(t, executed)
	})
}

func TestSimpleRateLimiter_SetInterval(t *testing.T) {
	t.Run("SetIntervalChangesRateLimit", func(t *testing.T) {
		rl := newSimpleRateLimiter(100 * time.Millisecond)
		defer rl.stop()

		ctx := context.Background()

		// Execute once to get ticker started
		err := rl.execute(ctx, func() error { return nil })
		require.NoError(t, err)

		// Change interval to shorter
		rl.setInterval(20 * time.Millisecond)

		// Execute again and measure time - this should use new interval
		start := time.Now()
		err = rl.execute(ctx, func() error { return nil })
		require.NoError(t, err)
		elapsed := time.Since(start)

		// Should complete close to new shorter interval (with tolerance)
		require.True(t, elapsed >= 15*time.Millisecond, "Should respect rate limit")
		require.True(t, elapsed < 50*time.Millisecond, "Should use new shorter interval")
	})

	t.Run("SetIntervalAfterStop", func(t *testing.T) {
		rl := newSimpleRateLimiter(50 * time.Millisecond)
		rl.stop()

		// Should not panic or cause issues
		rl.setInterval(100 * time.Millisecond)

		ctx := context.Background()
		executed := false

		err := rl.execute(ctx, func() error {
			executed = true
			return nil
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "rate limiter stopped")
		require.False(t, executed)
	})

	t.Run("ConcurrentSetInterval", func(t *testing.T) {
		rl := newSimpleRateLimiter(100 * time.Millisecond)
		defer rl.stop()

		var wg sync.WaitGroup

		// Concurrently change intervals
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(interval int) {
				defer wg.Done()
				rl.setInterval(time.Duration(interval*10) * time.Millisecond)
			}(i + 1)
		}

		// Also execute some work concurrently
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx := context.Background()
				err := rl.execute(ctx, func() error { return nil })
				require.NoError(t, err)
			}()
		}

		wg.Wait()
	})
}

func TestSimpleRateLimiter_ConcurrentUsage(t *testing.T) {
	t.Run("ConcurrentExecuteAndStop", func(t *testing.T) {
		rl := newSimpleRateLimiter(10 * time.Millisecond)

		var wg sync.WaitGroup
		var executedCount int64

		// Start multiple executions
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				err := rl.execute(ctx, func() error {
					atomic.AddInt64(&executedCount, 1)
					return nil
				})
				// Some may succeed, some may fail due to stop or timeout
				if err != nil {
					// Accept either timeout or stop error
					if !errors.Is(err, context.DeadlineExceeded) {
						require.Contains(t, err.Error(), "rate limiter stopped")
					}
				}
			}()
		}

		// Stop after a short delay
		time.Sleep(20 * time.Millisecond)
		rl.stop()

		wg.Wait()

		// At least one should have executed before stop
		require.GreaterOrEqual(t, executedCount, int64(1))
		require.Less(t, executedCount, int64(5))
	})

	t.Run("ConcurrentExecuteSetIntervalAndStop", func(t *testing.T) {
		rl := newSimpleRateLimiter(20 * time.Millisecond)

		var wg sync.WaitGroup
		var executedCount int64

		// Execute operations
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
				err := rl.execute(ctx, func() error {
					atomic.AddInt64(&executedCount, 1)
					return nil
				})
				if err != nil {
					// Accept either timeout or stop error
					if !errors.Is(err, context.DeadlineExceeded) {
						require.Contains(t, err.Error(), "rate limiter stopped")
					}
				}
			}()
		}

		// Change intervals
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(interval int) {
				defer wg.Done()
				rl.setInterval(time.Duration(interval*10) * time.Millisecond)
			}(i + 1)
		}

		// Stop after a delay
		time.Sleep(40 * time.Millisecond)
		rl.stop()

		wg.Wait()

		// Should have executed at least some operations
		require.GreaterOrEqual(t, executedCount, int64(1))
	})
}

func TestSimpleRateLimiter_EdgeCases(t *testing.T) {
	t.Run("VeryShortInterval", func(t *testing.T) {
		rl := newSimpleRateLimiter(1 * time.Nanosecond)
		defer rl.stop()

		ctx := context.Background()
		executed := false

		err := rl.execute(ctx, func() error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, executed)
	})

	t.Run("ZeroInterval", func(t *testing.T) {
		rl := newSimpleRateLimiter(0)
		defer rl.stop()

		ctx := context.Background()
		executed := false

		err := rl.execute(ctx, func() error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, executed)
	})

	t.Run("NegativeInterval", func(t *testing.T) {
		rl := newSimpleRateLimiter(-128)
		defer rl.stop()

		ctx := context.Background()
		executed := false
		err := rl.execute(ctx, func() error {
			executed = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, executed)

		rl.setInterval(-256)
		executed = false
		err = rl.execute(ctx, func() error {
			executed = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, executed)
	})

	t.Run("LargeInterval", func(t *testing.T) {
		rl := newSimpleRateLimiter(10 * time.Second)
		defer rl.stop()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		executed := false
		err := rl.execute(ctx, func() error {
			executed = true
			return nil
		})

		require.Error(t, err)
		require.Equal(t, context.DeadlineExceeded, err)
		require.False(t, executed)
	})
}
