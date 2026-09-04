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
	"iter"
	"testing"
	"time"

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
		assert.NoError(t, ait.awaitInit(context.Background()))
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
		assert.Error(t, ait.awaitInit(context.Background()))
	})
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

	// Cancel the caller's context while the root is still resolving.
	cancel()

	// Let resolution finish
	close(root.release)

	require.ErrorIs(t, ait.awaitInit(context.Background()), errReleasableRootDone)
}

// releasableRoot blocks ResolveRootValue until |release| is closed, regardless of ctx.
type releasableRoot struct {
	release chan struct{}
}

var _ doltdb.Rootish = releasableRoot{}

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

// errReleasableRootDone signals that releasableRoot resolved successfully rather than being canceled.
var errReleasableRootDone = fmt.Errorf("releasableRoot: released")

// noRelations is a doltdb.RelationSource that reports no relations at any root, so that
// tests can drive a merge with stub roots that never resolve to a real RootValue.
type noRelations struct{}

var _ doltdb.RelationSource[*doltdb.Table] = noRelations{}

func (noRelations) GetRelation(ctx context.Context, root doltdb.RootValue, tName doltdb.TableName) (*doltdb.Table, string, bool, error) {
	return nil, "", false, nil
}

func (noRelations) IterRelations(ctx context.Context, root doltdb.RootValue) iter.Seq2[doltdb.TableName, *doltdb.Table] {
	return func(yield func(doltdb.TableName, *doltdb.Table) bool) {}
}

// gateRoot resolves successfully once |release| is closed, recording that resolution was
// entered by closing |started|. It honors ctx cancellation, like a real root read.
type gateRoot struct {
	started chan struct{}
	release chan struct{}
}

var _ doltdb.Rootish = (*gateRoot)(nil)

func newGateRoot() *gateRoot {
	return &gateRoot{started: make(chan struct{}), release: make(chan struct{})}
}

func (r *gateRoot) ResolveRootValue(ctx context.Context) (doltdb.RootValue, error) {
	close(r.started)
	select {
	case <-r.release:
		return nil, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (r *gateRoot) HashOf() (hash.Hash, error) {
	return hash.Hash{}, nil
}

// initializedTracker returns a tracker that has finished initializing, like a live
// database between resets.
func initializedTracker() *AutoIncrementTracker {
	ait := &AutoIncrementTracker{
		dbName:         "test_database",
		sequences:      &SyncMap[doltdb.TableName, doltdb.AutoIncrementState]{},
		mm:             mutexmap.NewMutexMap(),
		init:           make(chan struct{}),
		cancelInit:     make(chan struct{}),
		relationSource: noRelations{},
	}
	close(ait.init)
	return ait
}

// TestMergeRootsCallerCancellationIsNotTerminal covers the mechanism behind
// dolthub/dolt#11581. dolt_reset --hard tells the tracker about the working set it just
// moved. When that ran through initialization, a reset whose query context ended while
// roots were still being read latched "context canceled" as the tracker's initialization
// error, and every later use of the database's sequences -- from any session, for the
// rest of the process's life -- failed with it. A merge reports its failure only to the
// caller that asked for it.
func TestMergeRootsCallerCancellationIsNotTerminal(t *testing.T) {
	ait := initializedTracker()
	root := newGateRoot()

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	mergeDone := make(chan error, 1)
	go func() {
		mergeDone <- ait.MergeRoots(callerCtx, root)
	}()

	// The reset's query context ends while the merge is still reading roots.
	<-root.started
	cancelCaller()

	select {
	case err := <-mergeDone:
		require.ErrorIs(t, err, context.Canceled, "the canceled caller should see its own cancellation")
	case <-time.After(30 * time.Second):
		t.Fatal("MergeRoots did not return after its caller's context was canceled")
	}

	// Other sessions are unaffected, and a later merge still works.
	require.NoError(t, ait.awaitInit(context.Background()), "a canceled dolt_reset --hard poisoned the tracker")
	require.NoError(t, ait.MergeRoots(context.Background(), newReleasedGateRoot()))
}

func newReleasedGateRoot() *gateRoot {
	root := newGateRoot()
	close(root.release)
	return root
}

// TestMergeRootsWaitsForInitialization pins that a merge against a tracker that never
// initialized reports the initialization failure rather than silently declaring the
// tracker healthy on the strength of one working set.
func TestMergeRootsWaitsForInitialization(t *testing.T) {
	ait := &AutoIncrementTracker{
		dbName:         "test_database",
		sequences:      &SyncMap[doltdb.TableName, doltdb.AutoIncrementState]{},
		mm:             mutexmap.NewMutexMap(),
		init:           make(chan struct{}),
		cancelInit:     make(chan struct{}),
		relationSource: noRelations{},
	}
	go ait.initWithRoots(context.Background(), ait.init, blockingRoot{})
	ait.Close()

	require.Error(t, ait.MergeRoots(context.Background(), newReleasedGateRoot()))
}

// TestQueryCancellationDuringInitialization pins that a query arriving while the tracker
// is still reading roots answers KILL QUERY. Initialization is deliberately detached from
// any one caller's context, which is why the wait for it needs the waiting caller's own
// context: otherwise a killed session sits in it for up to five minutes.
func TestQueryCancellationDuringInitialization(t *testing.T) {
	ait := &AutoIncrementTracker{
		dbName:         "test_database",
		sequences:      &SyncMap[doltdb.TableName, doltdb.AutoIncrementState]{},
		mm:             mutexmap.NewMutexMap(),
		init:           make(chan struct{}),
		cancelInit:     make(chan struct{}),
		relationSource: noRelations{},
	}
	go ait.initWithRoots(context.Background(), ait.init, blockingRoot{})
	defer ait.Close()

	queryCtx, cancelQuery := context.WithCancel(context.Background())
	queryDone := make(chan error, 1)
	go func() {
		_, err := ait.Current(queryCtx, doltdb.TableName{Name: "t"})
		queryDone <- err
	}()

	select {
	case err := <-queryDone:
		t.Fatalf("Current returned %v while initialization was still in progress", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancelQuery()
	select {
	case err := <-queryDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("Current did not return after its query context was canceled")
	}

	// Giving up is the caller's business only: initialization is still running, and its
	// outcome is still its own.
	select {
	case <-ait.init:
		t.Fatal("a canceled query ended the tracker's initialization")
	default:
	}
}
