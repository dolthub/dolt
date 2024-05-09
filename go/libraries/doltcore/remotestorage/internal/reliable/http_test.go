// Copyright 2024 Dolthub, Inc.
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

package reliable

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnforceThroughput(t *testing.T) {
	t.Run("CancelShutsDown", func(t *testing.T) {
		var didCancel atomic.Bool
		before := runtime.NumGoroutine()
		c := EnforceThroughput(func() uint64 { return 0 }, MinimumThroughputCheck{
			CheckInterval: time.Hour,
			BytesPerCheck: 1024,
			NumIntervals:  16,
		}, func(error) { didCancel.Store(true) })
		start := time.Now()
		for before >= runtime.NumGoroutine() && time.Since(start) < time.Second {
		}
		assert.Greater(t, runtime.NumGoroutine(), before)
		c()
		start = time.Now()
		for before < runtime.NumGoroutine() && time.Since(start) < time.Second {
		}
		assert.Equal(t, before, runtime.NumGoroutine())
		assert.False(t, didCancel.Load())
	})
	t.Run("DoesNotCancelThenCancels", func(t *testing.T) {
		var i, c int
		cnt := func() uint64 {
			if i > 64 {
				i += 1
				return uint64(c)
			}
			c += 128
			i += 1
			return uint64(c)
		}
		done := make(chan struct{})
		cleanup := EnforceThroughput(cnt, MinimumThroughputCheck{
			CheckInterval: time.Millisecond,
			BytesPerCheck: 64,
			NumIntervals:  16,
		}, func(error) { close(done) })
		t.Cleanup(cleanup)

		select {
		case <-done:
			assert.Greater(t, i, 64)
		case <-time.After(time.Second * 3):
			assert.FailNow(t, "EnforceThroughput did not cancel operation after 3 seconds.")
		}
	})
}
