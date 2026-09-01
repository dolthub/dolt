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

package dsess

import (
	"context"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess/mutexmap"
	"github.com/dolthub/dolt/go/store/hash"
)

// The types below instantiate the generic SequenceTracker over stub relations, so that
// merge behavior can be exercised without building a real database. gatedState doubles as
// an ordinary counter (when its channels are nil) and as a way to park a Next() call
// exactly between its read of the tracked state and its write-back of the successor.

type gatedState struct {
	v       uint64
	entered chan struct{}
	release chan struct{}
}

func (s gatedState) Next() (uint64, bool, gatedState, error) {
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	return s.v, true, gatedState{v: s.v + 1}, nil
}
func (s gatedState) CurrentValue() uint64          { return s.v }
func (s gatedState) WithValue(v uint64) gatedState { return gatedState{v: v} }
func (s gatedState) GreaterThan(o gatedState) bool { return s.v > o.v }
func (s gatedState) AtEnd() bool                   { return false }
func (s gatedState) Merge(o gatedState) gatedState {
	if o.v > s.v {
		return o
	}
	return s
}
func (s gatedState) WithSQLValue(ctx *sql.Context, v interface{}) (gatedState, error) {
	return gatedState{v: v.(uint64)}, nil
}

type stubRelation struct{ state gatedState }

func (r stubRelation) GetSequenceState(ctx context.Context) (gatedState, error) { return r.state, nil }
func (r stubRelation) HasSequenceState(ctx context.Context) (bool, error)       { return true, nil }
func (r stubRelation) GetSequenceSqlType(ctx context.Context) (sql.Type, bool, error) {
	return nil, false, nil
}
func (r stubRelation) SetSequenceState(ctx context.Context, v gatedState) (stubRelation, error) {
	return stubRelation{state: v}, nil
}
func (r stubRelation) TrySetSequenceState(ctx *sql.Context, v gatedState) (stubRelation, bool, error) {
	return stubRelation{state: v}, true, nil
}

// stubSource reports a single relation, "t", whose recorded value the test controls.
type stubSource struct{ value uint64 }

func (s *stubSource) GetRelation(ctx context.Context, root doltdb.RootValue, n doltdb.TableName) (stubRelation, string, bool, error) {
	return stubRelation{state: gatedState{v: s.value}}, n.Name, true, nil
}
func (s *stubSource) IterRelations(ctx context.Context, root doltdb.RootValue) iter.Seq2[doltdb.TableName, stubRelation] {
	return func(yield func(doltdb.TableName, stubRelation) bool) {
		yield(stubTable, stubRelation{state: gatedState{v: s.value}})
	}
}

var stubTable = doltdb.TableName{Name: "t"}

type nilRoot struct{}

func (nilRoot) ResolveRootValue(ctx context.Context) (doltdb.RootValue, error) { return nil, nil }
func (nilRoot) HashOf() (hash.Hash, error)                                     { return hash.Hash{}, nil }

type stubTracker = SequenceTracker[stubRelation, gatedState, uint64]

func newStubTracker(src *stubSource) *stubTracker {
	trk := &stubTracker{
		dbName:         "db",
		sequences:      &SyncMap[doltdb.TableName, gatedState]{},
		mm:             mutexmap.NewMutexMap(),
		init:           make(chan struct{}),
		cancelInit:     make(chan struct{}),
		relationSource: src,
		lockMode:       LockMode_Interleaved,
	}
	close(trk.init)
	return trk
}

// TestMergeRootsOnlyRaises pins the semantics dolt_reset --hard depends on: global
// sequence state is a high-water mark across every branch, so telling the tracker about a
// working set can raise a sequence but never lower it.
func TestMergeRootsOnlyRaises(t *testing.T) {
	src := &stubSource{}
	trk := newStubTracker(src)
	trk.sequences.Store(stubTable, gatedState{v: 10})

	src.value = 5
	require.NoError(t, trk.MergeRoots(context.Background(), nilRoot{}))
	got, _ := trk.sequences.Load(stubTable)
	require.Equal(t, uint64(10), got.v, "a lower root must not lower the tracked value")

	src.value = 20
	require.NoError(t, trk.MergeRoots(context.Background(), nilRoot{}))
	got, _ = trk.sequences.Load(stubTable)
	require.Equal(t, uint64(20), got.v, "a higher root must raise the tracked value")
}

// TestMergeRootsAddsUnseenRelations covers the case dolt_reset --hard actually exists for:
// a table the tracker has no entry for, because it was dropped and the reset brought it
// back.
func TestMergeRootsAddsUnseenRelations(t *testing.T) {
	trk := newStubTracker(&stubSource{value: 7})
	require.NoError(t, trk.MergeRoots(context.Background(), nilRoot{}))

	got, ok := trk.sequences.Load(stubTable)
	require.True(t, ok)
	require.Equal(t, uint64(7), got.v)
}

// TestMergeRootsDoesNotClobberInFlightAllocation pins the per-relation locking in the
// merge. Without it, an allocation parked between its read of the tracked state and its
// write-back overwrites whatever the merge raised the value to, sending the tracker
// backwards and handing out values another branch has already used.
func TestMergeRootsDoesNotClobberInFlightAllocation(t *testing.T) {
	const highWater = uint64(1 << 40)
	trk := newStubTracker(&stubSource{value: highWater})

	gate := gatedState{v: 1, entered: make(chan struct{}), release: make(chan struct{})}
	trk.sequences.Store(stubTable, gate)

	// An in-flight insert allocates off the tracker and parks mid-allocation.
	nextDone := make(chan struct{})
	go func() {
		defer close(nextDone)
		_, err := trk.Next(sql.NewEmptyContext(), stubTable, nil)
		require.NoError(t, err)
	}()
	<-gate.entered

	// dolt_reset --hard merges a much higher value while that insert is parked. It must
	// block behind the allocation rather than interleave with it.
	mergeDone := make(chan struct{})
	go func() {
		defer close(mergeDone)
		require.NoError(t, trk.MergeRoots(context.Background(), nilRoot{}))
	}()
	select {
	case <-mergeDone:
		t.Fatal("merge did not serialize against the in-flight allocation")
	case <-time.After(100 * time.Millisecond):
	}

	close(gate.release)
	<-nextDone
	<-mergeDone

	got, _ := trk.sequences.Load(stubTable)
	require.GreaterOrEqual(t, got.v, highWater,
		"in-flight allocation clobbered the value raised by the concurrent merge")
}

// TestConcurrentMergeRootsWithReaders runs merges against a database that is being read
// and written at the same time, which is the dolt_reset/dolt_checkout-on-separate-sessions
// shape. Under -race it also pins that a merge does not race readers of the tracker's
// initialization result.
func TestConcurrentMergeRootsWithReaders(t *testing.T) {
	trk := newStubTracker(&stubSource{value: 1})
	trk.sequences.Store(stubTable, gatedState{v: 1})

	stop := make(chan struct{})
	var workers sync.WaitGroup

	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var last uint64
			for {
				select {
				case <-stop:
					return
				default:
				}
				cur, err := trk.Current(stubTable)
				require.NoError(t, err)
				require.GreaterOrEqual(t, cur.v, last, "tracked value went backwards")
				last = cur.v

				_, err = trk.Next(sql.NewEmptyContext(), stubTable, nil)
				require.NoError(t, err)
			}
		}()
	}

	for i := 0; i < 200; i++ {
		require.NoError(t, trk.MergeRoots(context.Background(), nilRoot{}, nilRoot{}))
	}
	close(stop)
	workers.Wait()
}
