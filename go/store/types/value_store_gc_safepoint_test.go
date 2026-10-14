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

package types

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestGCCancellationDrainsItsSafepointVisit(t *testing.T) {
	controller := gcctx.NewGCSafepointController()
	visiting := &gcTestRootsProvider{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	require.NoError(t, controller.SessionCommandBegin(visiting))
	controller.SessionCommandEnd(visiting)

	safepoint := &blockingGCSafepoint{
		controller: controller,
		beginDone:  make(chan struct{}),
	}
	vs := newTestValueStore()
	gcDone := make(chan error, 1)
	go func() {
		gcDone <- vs.GC(context.Background(), chunks.GCConfig{Mode: chunks.GCMode_Full},
			hash.HashSet{}, hash.HashSet{}, safepoint)
	}()
	<-visiting.started

	// The first waiter still owns this visit after the session ends. A second
	// GC on the same SQL engine must not get a waiter while it is in flight.
	controller.SessionEnd(visiting)
	running := &gcTestRootsProvider{}
	require.NoError(t, controller.SessionCommandBegin(running))
	other, err := controller.Waiter(context.Background(), nil, func(context.Context, gcctx.GCRootsProvider) error {
		return nil
	})
	require.ErrorIs(t, err, gcctx.ErrConcurrentSafepoint)
	require.Nil(t, other)

	close(safepoint.beginDone)
	select {
	case err := <-gcDone:
		t.Fatalf("GC returned before its root visit finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(visiting.release)
	require.NoError(t, <-gcDone)
	controller.SessionCommandEnd(running)
}

type gcTestRootsProvider struct {
	started chan struct{}
	release chan struct{}
}

func (s *gcTestRootsProvider) VisitGCRoots(_ context.Context, _ string, keeper func(hash.Hash) bool) error {
	if s.started != nil {
		close(s.started)
		<-s.release
		keeper(hash.Hash{})
	}
	return nil
}

type blockingGCSafepoint struct {
	controller *gcctx.GCSafepointController
	waiter     *gcctx.GCSafepointWaiter
	beginDone  chan struct{}
}

func (s *blockingGCSafepoint) BeginGC(ctx context.Context, keeper func(hash.Hash) bool) error {
	var err error
	s.waiter, err = s.controller.Waiter(ctx, nil, func(ctx context.Context, session gcctx.GCRootsProvider) error {
		return session.VisitGCRoots(ctx, "", keeper)
	})
	if err != nil {
		return err
	}
	<-s.beginDone
	return nil
}

func (s *blockingGCSafepoint) EstablishPreFinalizeSafepoint(ctx context.Context) error {
	return s.waiter.Wait(ctx)
}

func (*blockingGCSafepoint) EstablishPostFinalizeSafepoint(context.Context) error {
	return nil
}

func (s *blockingGCSafepoint) CancelSafepoint() {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	s.waiter.Wait(canceled)
}
