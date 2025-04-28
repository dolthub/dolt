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
	"sync"

	"golang.org/x/sync/semaphore"
)

// A keymutex allows a caller to gain exclusive access to a critical
// section associated with the provided |key|. No callers will enter
// the critical section concurrently, and a caller which arrives while
// the critical section is occupied will block until it is available.
//
// A keymutex's Lock function should respect Context cancelation.
type Keymutex interface {
	Lock(ctx context.Context, key string) error
	Unlock(key string)
}

// Returns a Keymutex which stores mutexes in a map. This Keymutex has
// relatively high per-lock overhead, but allows all separate |key|s
// to make concurrent progress.
func NewMapped() Keymutex {
	return &mapKeymutex{
		states: make(map[string]*mapKeymutexState),
	}
}

type mapKeymutex struct {
	mu     sync.Mutex
	states map[string]*mapKeymutexState
}

type mapKeymutexState struct {
	sema    *semaphore.Weighted
	waitCnt int
}

func newMapKeymutexState() *mapKeymutexState {
	return &mapKeymutexState{
		sema: semaphore.NewWeighted(1),
	}
}

func (m *mapKeymutex) Lock(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var state *mapKeymutexState
	var ok bool
	if state, ok = m.states[key]; !ok {
		state = newMapKeymutexState()
		m.states[key] = state
	}
	if state.sema.TryAcquire(1) {
		return nil
	}
	state.waitCnt += 1
	m.mu.Unlock()
	err := state.sema.Acquire(ctx, 1)
	m.mu.Lock()
	state.waitCnt -= 1
	return err
}

func (m *mapKeymutex) Unlock(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[key]
	state.sema.Release(1)
	if state.waitCnt == 0 {
		delete(m.states, key)
	}
}
