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

package dprocedures

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
	"github.com/dolthub/dolt/go/store/hash"
)

// gcRootsProviderStub is a no-op gcctx.GCRootsProvider used to register
// sessions with a GCSafepointController in tests.
type gcRootsProviderStub struct {
	// Unused field so that each value gets a distinct address.
	id int
}

func (*gcRootsProviderStub) VisitGCRoots(ctx context.Context, db string, keep func(hash.Hash) bool) error {
	return nil
}

func TestSessionAwareSafepointMaxWait(t *testing.T) {
	// These subtests mutate the sessionAwareSafepointMaxWait package global, so
	// they cannot be run in parallel with one another.
	noopVisit := func(context.Context, gcctx.GCRootsProvider) error { return nil }

	t.Run("AbortsAfterMaxWait", func(t *testing.T) {
		defer setSessionAwareSafepointMaxWait(200 * time.Millisecond)()

		controller := gcctx.NewGCSafepointController()
		running := &gcRootsProviderStub{}
		require.NoError(t, controller.SessionCommandBegin(running))
		sc := &sessionAwareSafepointController{
			waiter: controller.Waiter(context.Background(), nil, noopVisit),
		}

		// EstablishPreFinalizeSafepoint runs before finalize()/chunk swap, so a
		// session that never ends its command must not stall it indefinitely
		// once a max wait is configured. The upper bound is generous (the
		// running session never quiesces, so an unbounded wait would never
		// return) to stay reliable on loaded CI rather than asserting precise
		// timing.
		start := time.Now()
		err := sc.EstablishPreFinalizeSafepoint(context.Background())
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(start), 5*time.Second)

		controller.SessionCommandEnd(running)
		controller.SessionEnd(running)
	})

	t.Run("UnboundedWhenUnset", func(t *testing.T) {
		defer setSessionAwareSafepointMaxWait(0)()

		controller := gcctx.NewGCSafepointController()
		running := &gcRootsProviderStub{}
		require.NoError(t, controller.SessionCommandBegin(running))
		sc := &sessionAwareSafepointController{
			waiter: controller.Waiter(context.Background(), nil, noopVisit),
		}

		// With no max wait configured, the safepoint keeps the original
		// behavior: it blocks until every running session quiesces.
		done := make(chan error, 1)
		go func() { done <- sc.EstablishPreFinalizeSafepoint(context.Background()) }()
		select {
		case <-done:
			t.Fatal("EstablishPreFinalizeSafepoint returned while a session was still running and no max wait was configured")
		case <-time.After(100 * time.Millisecond):
		}

		controller.SessionCommandEnd(running)
		require.NoError(t, <-done)
		controller.SessionEnd(running)
	})

	t.Run("SucceedsWhenSessionsQuiesce", func(t *testing.T) {
		defer setSessionAwareSafepointMaxWait(time.Minute)()

		controller := gcctx.NewGCSafepointController()
		quiesced := &gcRootsProviderStub{}
		require.NoError(t, controller.SessionCommandBegin(quiesced))
		controller.SessionCommandEnd(quiesced)
		sc := &sessionAwareSafepointController{
			waiter: controller.Waiter(context.Background(), nil, noopVisit),
		}

		// A configured max wait must not interfere with a safepoint that can be
		// established normally.
		require.NoError(t, sc.EstablishPreFinalizeSafepoint(context.Background()))
		controller.SessionEnd(quiesced)
	})
}

// setSessionAwareSafepointMaxWait sets the package global for the duration of a
// test and returns a function that restores its previous value.
func setSessionAwareSafepointMaxWait(d time.Duration) func() {
	prev := sessionAwareSafepointMaxWait
	sessionAwareSafepointMaxWait = d
	return func() { sessionAwareSafepointMaxWait = prev }
}
