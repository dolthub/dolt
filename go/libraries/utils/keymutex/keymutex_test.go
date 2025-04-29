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

package keymutex

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapped(t *testing.T) {
	t.Run("Cleanup", func(t *testing.T) {
		mutexes := NewMapped()
		func() {
			for _, s := range []string{"a", "b", "c", "d", "e", "f", "g"} {
				require.NoError(t, mutexes.Lock(context.Background(), s))
				defer mutexes.Unlock(s)
			}
		}()
		assert.Len(t, mutexes.(*mapKeymutex).states, 0)
	})
	t.Run("Exclusion", func(t *testing.T) {
		mutexes := NewMapped()
		var wg sync.WaitGroup
		var fours int
		var eights int
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 512; i++ {
					require.NoError(t, mutexes.Lock(context.Background(), "fours"))
					fours += 1
					mutexes.Unlock("fours")
				}
			}()
		}
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 256; i++ {
					require.NoError(t, mutexes.Lock(context.Background(), "eights"))
					eights += 1
					mutexes.Unlock("eights")
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, fours, 2048)
		assert.Equal(t, eights, 2048)
	})
	t.Run("Canceled", func(t *testing.T) {
		mutexes := NewMapped()
		require.NoError(t, mutexes.Lock(context.Background(), "taken"))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, mutexes.Lock(ctx, "taken"), context.Canceled)
		var cancels []func()
		var wg sync.WaitGroup
		wg.Add(64)
		for i := 0; i < 64; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)
			go func() {
				defer wg.Done()
				require.ErrorIs(t, mutexes.Lock(ctx, "taken"), context.Canceled)
			}()
		}
		var successWg sync.WaitGroup
		successWg.Add(1)
		go func() {
			defer successWg.Done()
			require.NoError(t, mutexes.Lock(context.Background(), "taken"))
			defer mutexes.Unlock("taken")
		}()
		for {
			mutexes.(*mapKeymutex).mu.Lock()
			if mutexes.(*mapKeymutex).states["taken"].waitCnt == 65 {
				mutexes.(*mapKeymutex).mu.Unlock()
				break
			}
			mutexes.(*mapKeymutex).mu.Unlock()
			runtime.Gosched()
		}
		for _, f := range cancels {
			f()
		}
		wg.Wait()
		mutexes.Unlock("taken")
		successWg.Wait()
	})
}
