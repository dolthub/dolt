// Copyright 2026 Dolthub, Inc.
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

package datas

import (
	"context"
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// SimpleSafepointController is a GCSafepointController for non-SQL consumers
// (such as embedded Dolt databases) that don't have a session layer.
//
// It coordinates GC with concurrent writers using a read-write lock:
//   - Writers hold a read lock during write operations
//   - GC safepoints acquire a write lock, ensuring no writers are active
//
// This is simpler than the session-aware controller in sqle/ but provides
// the same safety guarantees: no chunks are written between BeginGC's
// addChunk registration and the safepoint establishment.
type SimpleSafepointController struct {
	mu     sync.RWMutex
	keeper func(h hash.Hash) bool
}

var _ types.GCSafepointController = (*SimpleSafepointController)(nil)

// BeginGC registers the keeper function. After this call, concurrent writers
// that call KeepChunk will forward new chunk hashes to the keeper.
func (s *SimpleSafepointController) BeginGC(_ context.Context, keeper func(h hash.Hash) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keeper = keeper
	return nil
}

// EstablishPreFinalizeSafepoint acquires a write lock, blocking until all
// concurrent writers complete their current operations. While held, no new
// writes can start, ensuring a consistent point for GC finalization.
func (s *SimpleSafepointController) EstablishPreFinalizeSafepoint(_ context.Context) error {
	s.mu.Lock()
	return nil
}

// EstablishPostFinalizeSafepoint releases the write lock acquired by
// EstablishPreFinalizeSafepoint, allowing writers to resume.
func (s *SimpleSafepointController) EstablishPostFinalizeSafepoint(_ context.Context) error {
	s.mu.Unlock()
	return nil
}

// CancelSafepoint releases the write lock if a safepoint was established but
// GC is being cancelled.
func (s *SimpleSafepointController) CancelSafepoint() {
	// Only unlock if we're holding the write lock. Since Go's RWMutex panics
	// on double-unlock, callers must ensure this is only called once per
	// EstablishPreFinalizeSafepoint.
	s.mu.Unlock()
}

// WriteLock should be called by writers before performing chunk-store writes.
// It returns an unlock function that MUST be called when the write completes.
// During a GC safepoint, this blocks until the safepoint is released.
func (s *SimpleSafepointController) WriteLock() (unlock func()) {
	s.mu.RLock()
	return s.mu.RUnlock
}

// KeepChunk should be called by writers for every new chunk hash written
// during a GC window. If no GC is in progress, this is a no-op.
func (s *SimpleSafepointController) KeepChunk(h hash.Hash) {
	// No lock needed: keeper is set under write lock in BeginGC and cleared
	// after EndGC. Writers hold a read lock, so keeper won't change under us.
	if s.keeper != nil {
		s.keeper(h)
	}
}
