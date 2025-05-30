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
	"fmt"
	"sync"
	"time"
)

type simpleRateLimiter struct {
	mu      sync.Mutex
	ticker  *time.Ticker
	stopCh  chan struct{}
	stopped bool
}

func newSimpleRateLimiter(interval time.Duration) *simpleRateLimiter {
	// Ensure minimum interval to avoid panic
	if interval <= 0 {
		interval = 1 * time.Nanosecond
	}

	return &simpleRateLimiter{
		ticker: time.NewTicker(interval),
		stopCh: make(chan struct{}),
	}
}

func (rl *simpleRateLimiter) execute(ctx context.Context, f func() error) (err error) {
	defer func() {
		if rerr := recover(); rerr != nil {
			err = errors.Join(err, fmt.Errorf("caught panic running stats work: %v", rerr))
		}
	}()
	rl.mu.Lock()
	if rl.stopped {
		rl.mu.Unlock()
		return errors.New("rate limiter stopped")
	}
	ticker := rl.ticker
	rl.mu.Unlock()

	// Wait for rate limit, respecting the request context
	select {
	case <-ticker.C:
		// Rate limit satisfied, execute the function
		return f()
	case <-ctx.Done():
		// Request context cancelled while waiting for rate limit
		return context.Cause(ctx)
	case <-rl.stopCh:
		// Rate limiter stopped
		return errors.New("rate limiter stopped")
	}
}

func (rl *simpleRateLimiter) setInterval(interval time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.stopped {
		return
	}

	// Ensure minimum interval to avoid panic
	if interval <= 0 {
		interval = 1 * time.Nanosecond
	}

	rl.ticker.Reset(interval)
}

func (rl *simpleRateLimiter) stop() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.stopped {
		return
	}
	rl.stopped = true

	rl.ticker.Stop()
	close(rl.stopCh)
}
