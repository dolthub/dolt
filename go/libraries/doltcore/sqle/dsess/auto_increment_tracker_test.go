// Copyright 2023 Dolthub, Inc.
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

package dsess

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess/mutexmap"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestCoerceAutoIncrementValue(t *testing.T) {
	tests := []struct {
		val interface{}
		exp uint64
		err bool
	}{
		{
			val: nil,
			exp: uint64(0),
		},
		{
			val: int32(0),
			exp: uint64(0),
		},
		{
			val: int32(1),
			exp: uint64(1),
		},
		{
			val: uint32(1),
			exp: uint64(1),
		},
		{
			val: float32(1),
			exp: uint64(1),
		},
		{
			val: float32(1.1),
			exp: uint64(1),
		},
		{
			val: float32(1.9),
			exp: uint64(2),
		},
	}

	ctx := sql.NewEmptyContext()
	for _, test := range tests {
		name := fmt.Sprintf("Coerce %v to %v", test.val, test.exp)
		t.Run(name, func(t *testing.T) {
			act, err := doltdb.CoerceAutoIncrementValue(ctx, test.val)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.exp, act)
		})
	}
}

func TestInitWithRoots(t *testing.T) {
	t.Run("EmptyRoots", func(t *testing.T) {
		ait := AutoIncrementTracker{
			dbName:     "test_database",
			sequences:  &SyncMap[doltdb.TableName, doltdb.AutoIncrementState]{},
			mm:         mutexmap.NewMutexMap(),
			init:       make(chan struct{}),
			cancelInit: make(chan struct{}),
		}
		go ait.initWithRoots(context.Background(), ait.init)
		assert.NoError(t, ait.waitForInit())
	})
	t.Run("CloseCancelsInit", func(t *testing.T) {
		ait := AutoIncrementTracker{
			dbName:     "test_database",
			sequences:  &SyncMap[doltdb.TableName, doltdb.AutoIncrementState]{},
			mm:         mutexmap.NewMutexMap(),
			init:       make(chan struct{}),
			cancelInit: make(chan struct{}),
		}
		go ait.initWithRoots(context.Background(), ait.init, blockingRoot{})
		ait.Close()
		assert.Error(t, ait.waitForInit())
	})
}

// TestConcurrentInitWithRoots reproduces a race where overlapping InitWithRoots
// calls on the same tracker (e.g. from concurrent dolt_reset/dolt_checkout on
// different sessions against the same database) could each observe the prior
// |init| channel already closed and then race to install their own
// replacement channel.
func TestConcurrentInitWithRoots(t *testing.T) {
	ait := AutoIncrementTracker{
		dbName:     "test_database",
		sequences:  &SyncMap[doltdb.TableName, doltdb.AutoIncrementState]{},
		mm:         mutexmap.NewMutexMap(),
		init:       make(chan struct{}),
		cancelInit: make(chan struct{}),
	}
	close(ait.init) // starts "already initialized", like a live tracker between resets

	const numConcurrentCallers = 50
	errs := make([]error, numConcurrentCallers)
	var wg sync.WaitGroup
	for i := 0; i < numConcurrentCallers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = ait.InitWithRoots(context.Background())
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
}

type blockingRoot struct {
}

func (blockingRoot) ResolveRootValue(ctx context.Context) (doltdb.RootValue, error) {
	<-ctx.Done()
	return nil, context.Cause(ctx)
}

func (blockingRoot) HashOf() (hash.Hash, error) {
	return hash.Hash{}, nil
}

// TestNewSequenceTrackerFromRootsSurvivesCallerContextCancellation reproduces a race condition
// with initialization of sequence trackers.
func TestNewSequenceTrackerFromRootsSurvivesCallerContextCancellation(t *testing.T) {
	callerCtx, cancel := context.WithCancel(context.Background())
	root := releasableRoot{release: make(chan struct{})}

	ait, err := NewAutoIncrementTracker(callerCtx, "test_database", root)
	require.NoError(t, err)

	// Simulate the request returning and its context being canceled while our root is still
	// resolving.
	cancel()

	// Now let resolution finish. If initWithRoots is (incorrectly) using the caller's context,
	// this close is redundant: it would have already observed <-ctx.Done() above and bailed out
	// with a canceled error.
	close(root.release)

	// releasableRoot returns a distinguishable sentinel (rather than a real RootValue) once
	// released, so we can assert initialization ran to completion on an uncanceled context
	// instead of short-circuiting on the caller's cancellation.
	require.ErrorIs(t, ait.waitForInit(), errReleasableRootDone)
}

// releasableRoot blocks ResolveRootValue until |release| is closed, independent of the context
// passed in. This lets tests control exactly when resolution completes relative to context
// cancellation.
type releasableRoot struct {
	release chan struct{}
}

func (r releasableRoot) ResolveRootValue(ctx context.Context) (doltdb.RootValue, error) {
	select {
	case <-r.release:
		return nil, errReleasableRootDone
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (releasableRoot) HashOf() (hash.Hash, error) {
	return hash.Hash{}, nil
}

// errReleasableRootDone is returned by releasableRoot once released, so tests can distinguish
// "resolution completed" from "resolution was canceled" without needing a real RootValue.
var errReleasableRootDone = fmt.Errorf("releasableRoot: released")
