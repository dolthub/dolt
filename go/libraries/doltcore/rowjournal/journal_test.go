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

package rowjournal

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/hash"
)

var allModes = []LockMode{LockKeyShare, LockShare, LockNoKeyUpdate, LockUpdate}

var testRel = RelationKey{
	Database: "db",
	Branch:   "workingSets/heads/main",
	Table:    doltdb.TableName{Name: "t"},
}

// outcome is the result of a lock or write request.
type outcome struct {
	res LockResult
	err error
}

// harness wraps a Journal with helpers for driving several transactions from one test goroutine.
type harness struct {
	t *testing.T
	j *Journal
}

func newHarness(t *testing.T) *harness {
	j := NewJournal()
	j.SetDeadlockTimeout(20 * time.Millisecond)
	return &harness{t: t, j: j}
}

func lockReq(rel RelationKey, key string, mode LockMode, policy WaitPolicy) LockRequest {
	return LockRequest{
		Relation:   rel,
		Target:     RowTarget([]byte(key)),
		Compare:    bytes.Compare,
		Mode:       mode,
		WaitPolicy: policy,
	}
}

func writeReq(rel RelationKey, key string, op WriteOp) WriteRequest {
	return WriteRequest{
		Relation: rel,
		Target:   RowTarget([]byte(key)),
		Compare:  bytes.Compare,
		Op:       op,
	}
}

// lock requests a row lock that is expected to complete without waiting.
func (h *harness) lock(txn *Txn, key string, mode LockMode) LockResult {
	h.t.Helper()
	res, err := h.j.Lock(context.Background(), txn, lockReq(testRel, key, mode, WaitError))
	require.NoError(h.t, err)
	return res
}

// tryLock requests a row lock, skipping it if it would have to wait.
func (h *harness) tryLock(txn *Txn, key string, mode LockMode) LockResult {
	h.t.Helper()
	res, err := h.j.Lock(context.Background(), txn, lockReq(testRel, key, mode, WaitSkip))
	require.NoError(h.t, err)
	return res
}

// write records a row write that is expected to complete without waiting.
func (h *harness) write(txn *Txn, key string, op WriteOp) LockResult {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := h.j.Write(ctx, txn, writeReq(testRel, key, op))
	require.NoError(h.t, err)
	return res
}

// lockBlocked starts a row lock request on another goroutine and waits until it is blocked.
func (h *harness) lockBlocked(txn *Txn, key string, mode LockMode) <-chan outcome {
	h.t.Helper()
	return h.blocked(txn, func() (LockResult, error) {
		return h.j.Lock(context.Background(), txn, lockReq(testRel, key, mode, WaitBlock))
	})
}

// writeBlocked starts a row write on another goroutine and waits until it is blocked.
func (h *harness) writeBlocked(txn *Txn, req WriteRequest) <-chan outcome {
	h.t.Helper()
	return h.blocked(txn, func() (LockResult, error) {
		return h.j.Write(context.Background(), txn, req)
	})
}

func (h *harness) blocked(txn *Txn, request func() (LockResult, error)) <-chan outcome {
	h.t.Helper()
	before := len(h.waitsOf(txn))
	done := make(chan outcome, 1)
	go func() {
		res, err := request()
		done <- outcome{res: res, err: err}
	}()

	require.Eventually(h.t, func() bool {
		return len(h.waitsOf(txn)) > before
	}, 5*time.Second, time.Millisecond, "request did not block")

	select {
	case o := <-done:
		h.t.Fatalf("request finished instead of blocking: %v, %v", o.res, o.err)
	default:
	}

	return done
}

func (h *harness) waitsOf(txn *Txn) []LockInfo {
	var waits []LockInfo
	for _, info := range h.j.Locks() {
		if info.Txn == txn.ID() && !info.Granted {
			waits = append(waits, info)
		}
	}

	return waits
}

// await returns the outcome of a request started with lockBlocked or writeBlocked.
func (h *harness) await(done <-chan outcome) outcome {
	h.t.Helper()
	select {
	case o := <-done:
		return o
	case <-time.After(5 * time.Second):
		h.t.Fatal("request did not finish")
		return outcome{}
	}
}

func (h *harness) requireStillBlocked(done <-chan outcome) {
	h.t.Helper()
	select {
	case o := <-done:
		h.t.Fatalf("request finished instead of staying blocked: %v, %v", o.res, o.err)
	case <-time.After(20 * time.Millisecond):
	}
}

// requireEmpty asserts that the journal holds no state at all.
func (h *harness) requireEmpty() {
	h.t.Helper()
	h.j.mu.Lock()
	defer h.j.mu.Unlock()
	require.Empty(h.t, h.j.active)
	require.Empty(h.t, h.j.relations)
	require.Empty(h.t, h.j.retired)
}

func TestLockModeConflicts(t *testing.T) {
	conflicts := map[[2]LockMode]bool{
		{LockKeyShare, LockUpdate}:         true,
		{LockShare, LockNoKeyUpdate}:       true,
		{LockShare, LockUpdate}:            true,
		{LockNoKeyUpdate, LockNoKeyUpdate}: true,
		{LockNoKeyUpdate, LockUpdate}:      true,
		{LockUpdate, LockUpdate}:           true,
	}

	for _, a := range allModes {
		for _, b := range allModes {
			expected := conflicts[[2]LockMode{a, b}] || conflicts[[2]LockMode{b, a}]
			assert.Equal(t, expected, a.ConflictsWith(b), "%s vs %s", a, b)
			assert.Equal(t, a >= b, a.Covers(b), "%s covers %s", a, b)
		}
	}
}

func TestSnapshotIncludes(t *testing.T) {
	s := Snapshot{OldestRunning: 5, NextTxn: 10, Running: []TxnID{5, 7}}
	for id, expected := range map[TxnID]bool{1: true, 4: true, 5: false, 6: true, 7: false, 8: true, 9: true, 10: false, 11: false} {
		assert.Equal(t, expected, s.Includes(id), "txn %d", id)
	}
}

func TestBeginAssignsAscendingIDsAndSnapshots(t *testing.T) {
	h := newHarness(t)
	a := h.j.Begin(1)
	b := h.j.Begin(2)
	c := h.j.Begin(3)
	require.Less(t, a.ID(), b.ID())
	require.Less(t, b.ID(), c.ID())
	assert.Equal(t, uint32(2), b.SessionID())

	assert.Equal(t, Snapshot{OldestRunning: a.ID(), NextTxn: a.ID() + 1}, a.Snapshot())
	assert.Equal(t, Snapshot{OldestRunning: a.ID(), NextTxn: c.ID() + 1, Running: []TxnID{a.ID(), b.ID()}}, c.Snapshot())

	h.j.Commit(a)
	assert.Equal(t, TxnCommitted, a.Status())
	h.j.Abort(a)
	assert.Equal(t, TxnCommitted, a.Status(), "ending a transaction twice does nothing")

	d := h.j.Begin(4)
	assert.Equal(t, Snapshot{OldestRunning: b.ID(), NextTxn: d.ID() + 1, Running: []TxnID{b.ID(), c.ID()}}, d.Snapshot())

	active := h.j.ActiveTxns()
	require.Len(t, active, 3)
	assert.Equal(t, b.ID(), active[0].ID)

	h.j.Abort(b)
	h.j.Abort(c)
	h.j.Abort(d)
	assert.Equal(t, TxnAborted, d.Status())
	h.requireEmpty()

	_, err := h.j.Lock(context.Background(), d, lockReq(testRel, "k", LockShare, WaitBlock))
	assert.ErrorIs(t, err, ErrTxnNotInProgress)
}

func TestLockConflictMatrix(t *testing.T) {
	for _, held := range allModes {
		for _, requested := range allModes {
			t.Run(fmt.Sprintf("%s then %s", held, requested), func(t *testing.T) {
				h := newHarness(t)
				holder := h.j.Begin(1)
				requester := h.j.Begin(2)
				require.Equal(t, LockGranted, h.lock(holder, "k", held))

				if held.ConflictsWith(requested) {
					assert.Equal(t, LockWouldBlock, h.tryLock(requester, "k", requested))
					_, err := h.j.Lock(context.Background(), requester, lockReq(testRel, "k", requested, WaitError))
					assert.ErrorIs(t, err, ErrLockNotAvailable)

					done := h.lockBlocked(requester, "k", requested)
					assert.Equal(t, []TxnID{holder.ID()}, h.j.BlockedBy(requester))
					h.j.Commit(holder)
					assert.Equal(t, outcome{res: LockGranted}, h.await(done))
				} else {
					assert.Equal(t, LockGranted, h.tryLock(requester, "k", requested))
				}

				// A different row is never affected.
				assert.Equal(t, LockGranted, h.tryLock(requester, "other", LockUpdate))

				h.j.Abort(holder)
				h.j.Abort(requester)
				h.requireEmpty()
			})
		}
	}
}

func TestTransactionNeverConflictsWithItself(t *testing.T) {
	h := newHarness(t)
	txn := h.j.Begin(1)
	for _, first := range allModes {
		for _, second := range allModes {
			key := fmt.Sprintf("%s-%s", first, second)
			require.Equal(t, LockGranted, h.lock(txn, key, first))
			require.Equal(t, LockGranted, h.lock(txn, key, second))
		}
	}

	require.Equal(t, LockGranted, h.write(txn, "w", WriteInsert))
	require.Equal(t, LockGranted, h.write(txn, "w", WriteUpdate))
	require.Equal(t, LockGranted, h.write(txn, "w", WriteDelete))
	require.Equal(t, LockGranted, h.write(txn, "w", WriteInsert))
	require.Equal(t, LockGranted, h.lock(txn, "w", LockShare))

	h.j.Commit(txn)
	h.requireEmpty()
}

func TestOutcomeAfterWaitingForWriter(t *testing.T) {
	tests := []struct {
		name string
		// write is what the holder does to the row.
		write WriteRequest
		// commit is whether the holder commits or aborts.
		commit bool
		// mode is the lock the waiter requests.
		mode     LockMode
		expected LockResult
	}{
		{name: "holder updates and commits", write: writeReq(testRel, "k", WriteUpdate), commit: true, mode: LockUpdate, expected: LockRowUpdated},
		{name: "holder updates and commits, share", write: writeReq(testRel, "k", WriteUpdate), commit: true, mode: LockShare, expected: LockRowUpdated},
		{name: "holder updates and aborts", write: writeReq(testRel, "k", WriteUpdate), commit: false, mode: LockUpdate, expected: LockGranted},
		{name: "holder deletes and commits", write: writeReq(testRel, "k", WriteDelete), commit: true, mode: LockUpdate, expected: LockRowDeleted},
		{name: "holder deletes and commits, key share", write: writeReq(testRel, "k", WriteDelete), commit: true, mode: LockKeyShare, expected: LockRowDeleted},
		{name: "holder deletes and aborts", write: writeReq(testRel, "k", WriteDelete), commit: false, mode: LockNoKeyUpdate, expected: LockGranted},
		{
			name:     "holder changes key and commits, key share",
			write:    WriteRequest{Relation: testRel, Target: RowTarget([]byte("k")), Compare: bytes.Compare, Op: WriteUpdate, KeysChanged: true, Successor: []byte("k2")},
			commit:   true,
			mode:     LockKeyShare,
			expected: LockRowUpdated,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t)
			holder := h.j.Begin(1)
			waiter := h.j.Begin(2)

			res, err := h.j.Write(context.Background(), holder, test.write)
			require.NoError(t, err)
			require.Equal(t, LockGranted, res)

			done := h.lockBlocked(waiter, "k", test.mode)
			if test.commit {
				h.j.Commit(holder)
			} else {
				h.j.Abort(holder)
			}

			assert.Equal(t, outcome{res: test.expected}, h.await(done))
			if test.expected != LockGranted {
				assert.Empty(t, h.j.Locks(), "a refused request holds no lock")
			}

			// A transaction that starts after the holder ended sees its changes, and so has no conflict with them.
			later := h.j.Begin(3)
			if test.expected == LockGranted {
				assert.Equal(t, LockWouldBlock, h.tryLock(later, "k", LockUpdate))
			} else {
				assert.Equal(t, LockGranted, h.tryLock(later, "k", LockUpdate))
			}

			h.j.Abort(waiter)
			h.j.Abort(later)
			h.requireEmpty()
		})
	}
}

func TestKeyShareIgnoresNonKeyUpdate(t *testing.T) {
	h := newHarness(t)
	updater := h.j.Begin(1)
	locker := h.j.Begin(2)

	// An update that leaves the key alone does not conflict with a key share lock, before or after it commits.
	require.Equal(t, LockGranted, h.write(updater, "k", WriteUpdate))
	require.Equal(t, LockGranted, h.lock(locker, "k", LockKeyShare))
	h.j.Commit(updater)
	require.Equal(t, LockGranted, h.lock(locker, "k", LockKeyShare))

	// A stronger lock does see the update.
	assert.Equal(t, LockRowUpdated, h.lock(locker, "k", LockShare))

	// The key share lock stops a later transaction from deleting the row or changing its key.
	deleter := h.j.Begin(3)
	done := h.writeBlocked(deleter, writeReq(testRel, "k", WriteDelete))
	h.j.Commit(locker)
	assert.Equal(t, outcome{res: LockGranted}, h.await(done))

	h.j.Commit(deleter)
	h.requireEmpty()
}

func TestChangeCommittedAfterSnapshot(t *testing.T) {
	h := newHarness(t)
	reader := h.j.Begin(1)
	writer := h.j.Begin(2)

	require.Equal(t, LockGranted, h.write(writer, "updated", WriteUpdate))
	require.Equal(t, LockGranted, h.write(writer, "deleted", WriteDelete))
	require.Equal(t, LockGranted, h.write(writer, "inserted", WriteInsert))
	h.j.Commit(writer)

	// The reader's snapshot predates the writer's commit, so it must not build on those rows. It does not wait.
	assert.Equal(t, LockRowUpdated, h.lock(reader, "updated", LockUpdate))
	assert.Equal(t, LockRowDeleted, h.lock(reader, "deleted", LockUpdate))
	assert.Equal(t, LockRowUpdated, h.write(reader, "updated", WriteUpdate))
	assert.Equal(t, LockRowDeleted, h.write(reader, "deleted", WriteDelete))
	assert.Equal(t, LockKeyExists, h.write(reader, "inserted", WriteInsert))
	assert.Equal(t, LockGranted, h.write(reader, "deleted", WriteInsert), "a key freed by a concurrent delete can be inserted")
	assert.Equal(t, LockGranted, h.lock(reader, "untouched", LockUpdate))
	assert.Equal(t, 4, h.j.RetainedWrites())

	// The writer's changes are kept for as long as the reader's snapshot exists, and no longer.
	h.j.Abort(reader)
	assert.Equal(t, 0, h.j.RetainedWrites())
	h.requireEmpty()
}

func TestConcurrentInsertOfSameKey(t *testing.T) {
	for _, commit := range []bool{true, false} {
		t.Run(fmt.Sprintf("first inserter commits=%v", commit), func(t *testing.T) {
			h := newHarness(t)
			first := h.j.Begin(1)
			second := h.j.Begin(2)

			require.Equal(t, LockGranted, h.write(first, "k", WriteInsert))
			done := h.writeBlocked(second, writeReq(testRel, "k", WriteInsert))

			if commit {
				h.j.Commit(first)
				assert.Equal(t, outcome{res: LockKeyExists}, h.await(done))
			} else {
				h.j.Abort(first)
				assert.Equal(t, outcome{res: LockGranted}, h.await(done))
			}

			h.j.Commit(second)
			h.requireEmpty()
		})
	}
}

func TestWaitersAreServedInOrder(t *testing.T) {
	h := newHarness(t)
	holder := h.j.Begin(1)
	first := h.j.Begin(2)
	second := h.j.Begin(3)

	require.Equal(t, LockGranted, h.lock(holder, "k", LockUpdate))
	firstDone := h.lockBlocked(first, "k", LockUpdate)
	secondDone := h.lockBlocked(second, "k", LockShare)
	assert.Equal(t, []TxnID{holder.ID()}, h.j.BlockedBy(first))
	assert.Equal(t, []TxnID{holder.ID(), first.ID()}, h.j.BlockedBy(second))

	// The request that has waited longest gets the lock, and the other goes on waiting, now for it.
	h.j.Commit(holder)
	assert.Equal(t, outcome{res: LockGranted}, h.await(firstDone))
	h.requireStillBlocked(secondDone)

	h.j.Commit(first)
	assert.Equal(t, outcome{res: LockGranted}, h.await(secondDone))
	h.j.Commit(second)
	h.requireEmpty()
}

func TestCompatibleLockIsGrantedPastWaiters(t *testing.T) {
	h := newHarness(t)
	holder := h.j.Begin(1)
	exclusive := h.j.Begin(2)
	share := h.j.Begin(3)

	require.Equal(t, LockGranted, h.lock(holder, "k", LockShare))
	exclusiveDone := h.lockBlocked(exclusive, "k", LockUpdate)

	// A share request conflicts with no lock that is held, so it doesn't wait behind the exclusive request, which
	// now has it to wait for as well.
	assert.Equal(t, LockGranted, h.tryLock(share, "k", LockShare))
	assert.Equal(t, []TxnID{holder.ID(), share.ID()}, h.j.BlockedBy(exclusive))

	h.j.Commit(holder)
	h.requireStillBlocked(exclusiveDone)
	h.j.Commit(share)
	assert.Equal(t, outcome{res: LockGranted}, h.await(exclusiveDone))
	h.j.Commit(exclusive)
	h.requireEmpty()
}

func TestCompatibleWaitersAreGrantedTogether(t *testing.T) {
	h := newHarness(t)
	holder := h.j.Begin(1)
	require.Equal(t, LockGranted, h.lock(holder, "k", LockUpdate))

	var waiters []*Txn
	var dones []<-chan outcome
	for i := 0; i < 4; i++ {
		txn := h.j.Begin(uint32(i + 2))
		waiters = append(waiters, txn)
		dones = append(dones, h.lockBlocked(txn, "k", LockShare))
	}

	h.j.Commit(holder)
	for _, done := range dones {
		assert.Equal(t, outcome{res: LockGranted}, h.await(done))
	}

	for _, txn := range waiters {
		h.j.Commit(txn)
	}

	h.requireEmpty()
}

func TestUpgradeSkipsTheQueue(t *testing.T) {
	h := newHarness(t)
	upgrader := h.j.Begin(1)
	other := h.j.Begin(2)
	queued := h.j.Begin(3)

	require.Equal(t, LockGranted, h.lock(upgrader, "k", LockShare))
	require.Equal(t, LockGranted, h.lock(other, "k", LockShare))
	queuedDone := h.lockBlocked(queued, "k", LockUpdate)

	// The upgrader waits for the other share lock, but not for the queued request, which is itself waiting for the
	// upgrader. Queueing behind it would be a deadlock.
	upgradeDone := h.lockBlocked(upgrader, "k", LockUpdate)
	assert.Equal(t, []TxnID{other.ID()}, h.j.BlockedBy(upgrader))

	h.j.Commit(other)
	assert.Equal(t, outcome{res: LockGranted}, h.await(upgradeDone))
	h.requireStillBlocked(queuedDone)

	h.j.Commit(upgrader)
	assert.Equal(t, outcome{res: LockGranted}, h.await(queuedDone))
	h.j.Commit(queued)
	h.requireEmpty()
}

func TestSavepointRollbackRestoresWeakerLock(t *testing.T) {
	h := newHarness(t)
	txn := h.j.Begin(1)
	other := h.j.Begin(2)

	require.Equal(t, LockGranted, h.lock(txn, "k", LockShare))
	level, err := h.j.SavepointBegin(txn)
	require.NoError(t, err)
	require.Equal(t, uint32(1), level)

	require.Equal(t, LockGranted, h.lock(txn, "k", LockUpdate))
	require.Equal(t, LockGranted, h.lock(txn, "k2", LockUpdate))
	require.Equal(t, LockGranted, h.write(txn, "k3", WriteUpdate))
	assert.Equal(t, LockWouldBlock, h.tryLock(other, "k", LockKeyShare))

	shareDone := h.lockBlocked(other, "k", LockShare)
	require.NoError(t, h.j.SavepointRollback(txn, level))

	// The share lock taken before the savepoint survives. Everything taken since is gone, and so is the write.
	assert.Equal(t, outcome{res: LockGranted}, h.await(shareDone))
	assert.Equal(t, LockWouldBlock, h.tryLock(other, "k", LockNoKeyUpdate))
	assert.Equal(t, LockGranted, h.tryLock(other, "k2", LockUpdate))
	assert.Equal(t, LockGranted, h.tryLock(other, "k3", LockUpdate))
	assert.Equal(t, 0, h.j.RetainedWrites())

	// The savepoint is still open, so it can be rolled back again.
	require.Equal(t, LockGranted, h.lock(txn, "k4", LockUpdate))
	require.NoError(t, h.j.SavepointRollback(txn, level))
	assert.Equal(t, LockGranted, h.tryLock(other, "k4", LockUpdate))

	assert.Error(t, h.j.SavepointRollback(txn, level+1))
	h.j.Commit(txn)
	assert.ErrorIs(t, h.j.SavepointRollback(txn, level), ErrTxnNotInProgress)
	h.j.Commit(other)
	h.requireEmpty()
}

func TestSavepointReleaseKeepsLocks(t *testing.T) {
	h := newHarness(t)
	txn := h.j.Begin(1)
	other := h.j.Begin(2)

	outer, err := h.j.SavepointBegin(txn)
	require.NoError(t, err)
	require.Equal(t, LockGranted, h.lock(txn, "outer", LockUpdate))

	inner, err := h.j.SavepointBegin(txn)
	require.NoError(t, err)
	require.Equal(t, LockGranted, h.lock(txn, "inner", LockUpdate))
	require.Equal(t, LockGranted, h.write(txn, "inner", WriteUpdate))

	// Releasing the inner savepoint hands its locks to the outer one.
	require.NoError(t, h.j.SavepointRelease(txn, inner))
	assert.Equal(t, LockWouldBlock, h.tryLock(other, "inner", LockShare))
	assert.Equal(t, 1, h.j.RetainedWrites())

	// Rolling back the outer savepoint then releases both.
	require.NoError(t, h.j.SavepointRollback(txn, outer))
	assert.Equal(t, LockGranted, h.tryLock(other, "inner", LockShare))
	assert.Equal(t, LockGranted, h.tryLock(other, "outer", LockShare))
	assert.Equal(t, 0, h.j.RetainedWrites())

	// Releasing the outer savepoint returns the transaction to its base level, where locks last until it ends.
	require.Equal(t, LockGranted, h.lock(txn, "base", LockUpdate))
	require.NoError(t, h.j.SavepointRelease(txn, outer))
	assert.Error(t, h.j.SavepointRollback(txn, outer))
	assert.Equal(t, LockWouldBlock, h.tryLock(other, "base", LockShare))

	h.j.Commit(txn)
	h.j.Commit(other)
	h.requireEmpty()
}

func TestDeadlockBetweenTwoTransactions(t *testing.T) {
	h := newHarness(t)
	a := h.j.Begin(1)
	b := h.j.Begin(2)

	require.Equal(t, LockGranted, h.lock(a, "x", LockUpdate))
	require.Equal(t, LockGranted, h.lock(b, "y", LockUpdate))

	// The request that would complete the cycle is refused at once. The one already waiting keeps waiting, and
	// proceeds when the refused transaction gives up its locks.
	aDone := h.lockBlocked(a, "y", LockUpdate)
	_, err := h.j.Lock(context.Background(), b, lockReq(testRel, "x", LockUpdate, WaitBlock))
	require.ErrorIs(t, err, ErrDeadlock)
	h.requireStillBlocked(aDone)

	h.j.Abort(b)
	assert.Equal(t, outcome{res: LockGranted}, h.await(aDone))
	h.j.Abort(a)
	h.requireEmpty()
}

func TestDeadlockAmongThreeTransactions(t *testing.T) {
	h := newHarness(t)
	h.j.SetDeadlockTimeout(50 * time.Millisecond)
	txns := []*Txn{h.j.Begin(1), h.j.Begin(2), h.j.Begin(3)}
	keys := []string{"x", "y", "z"}
	for i, txn := range txns {
		require.Equal(t, LockGranted, h.lock(txn, keys[i], LockUpdate))
	}

	// Each transaction waits for the next one's row.
	results := make(chan int, len(txns))
	errs := make([]error, len(txns))
	for i, txn := range txns {
		go func() {
			_, errs[i] = h.j.Lock(context.Background(), txn, lockReq(testRel, keys[(i+1)%len(keys)], LockUpdate, WaitBlock))
			if errs[i] != nil {
				h.j.Abort(txn)
			}

			results <- i
		}()
	}

	victims := 0
	for range txns {
		select {
		case i := <-results:
			if errs[i] != nil {
				require.ErrorIs(t, errs[i], ErrDeadlock)
				victims++
				continue
			}

			// A transaction that got its lock finishes, which lets the one waiting for it proceed.
			h.j.Commit(txns[i])
		case <-time.After(5 * time.Second):
			t.Fatal("deadlock was not resolved")
		}
	}

	assert.GreaterOrEqual(t, victims, 1)
	assert.Less(t, victims, len(txns))
	h.requireEmpty()
}

func TestLockTimeoutAndCancellation(t *testing.T) {
	h := newHarness(t)
	h.j.SetDeadlockTimeout(time.Hour)
	holder := h.j.Begin(1)
	waiter := h.j.Begin(2)
	require.Equal(t, LockGranted, h.lock(holder, "k", LockUpdate))

	req := lockReq(testRel, "k", LockShare, WaitBlock)
	req.Timeout = 20 * time.Millisecond
	_, err := h.j.Lock(context.Background(), waiter, req)
	assert.ErrorIs(t, err, ErrLockTimeout)

	ctx, cancel := context.WithCancel(context.Background())
	done := h.blocked(waiter, func() (LockResult, error) {
		return h.j.Lock(ctx, waiter, lockReq(testRel, "k", LockShare, WaitBlock))
	})
	cancel()
	assert.ErrorIs(t, h.await(done).err, context.Canceled)

	// A request whose own transaction ends while it waits gives up.
	done = h.lockBlocked(waiter, "k", LockShare)
	h.j.Abort(waiter)
	assert.ErrorIs(t, h.await(done).err, ErrTxnNotInProgress)

	// None of the abandoned requests left anything behind.
	locks := h.j.Locks()
	require.Len(t, locks, 1)
	assert.Equal(t, LockInfo{
		Relation:  testRel,
		Target:    RowTarget([]byte("k")),
		Txn:       holder.ID(),
		SessionID: 1,
		Mode:      LockUpdate,
		Granted:   true,
	}, locks[0])
	assert.False(t, h.j.IsSessionBlocked(2))

	h.j.Commit(holder)
	h.requireEmpty()
}

func TestWritesArePrunedOnceEverySnapshotSeesThem(t *testing.T) {
	h := newHarness(t)
	old := h.j.Begin(1)

	for i := 0; i < 3; i++ {
		writer := h.j.Begin(2)
		require.Equal(t, LockGranted, h.write(writer, fmt.Sprintf("k%d", i), WriteUpdate))
		h.j.Commit(writer)
	}

	aborted := h.j.Begin(3)
	require.Equal(t, LockGranted, h.write(aborted, "aborted", WriteUpdate))
	h.j.Abort(aborted)

	// The old transaction's snapshot cannot see the three committed writes, so they are kept. The aborted write is
	// not.
	assert.Equal(t, 3, h.j.RetainedWrites())
	assert.Equal(t, LockRowUpdated, h.lock(old, "k1", LockShare))
	assert.Equal(t, LockGranted, h.lock(old, "aborted", LockShare))

	// A transaction that starts now sees them, but does not let them go while the old one is running.
	recent := h.j.Begin(4)
	assert.Equal(t, LockGranted, h.lock(recent, "k1", LockShare))
	h.j.Commit(recent)
	assert.Equal(t, 3, h.j.RetainedWrites())

	h.j.Commit(old)
	assert.Equal(t, 0, h.j.RetainedWrites())
	h.requireEmpty()
}

func TestTableLocks(t *testing.T) {
	h := newHarness(t)
	tableReq := func(mode LockMode, policy WaitPolicy) LockRequest {
		return LockRequest{Relation: testRel, Target: TableTarget(), Compare: bytes.Compare, Mode: mode, WaitPolicy: policy}
	}

	rowHolder := h.j.Begin(1)
	ddl := h.j.Begin(2)
	latecomer := h.j.Begin(3)
	require.Equal(t, LockGranted, h.lock(rowHolder, "a", LockNoKeyUpdate))

	// A lock on the table waits for the row locks other transactions hold in it.
	res, err := h.j.Lock(context.Background(), ddl, tableReq(LockUpdate, WaitSkip))
	require.NoError(t, err)
	assert.Equal(t, LockWouldBlock, res)
	ddlDone := h.blocked(ddl, func() (LockResult, error) {
		return h.j.Lock(context.Background(), ddl, tableReq(LockUpdate, WaitBlock))
	})
	assert.Equal(t, []TxnID{rowHolder.ID()}, h.j.BlockedBy(ddl))

	// A transaction that already holds locks in the table can take more. One that holds none waits behind the
	// request for the table.
	assert.Equal(t, LockGranted, h.tryLock(rowHolder, "b", LockUpdate))
	assert.Equal(t, LockWouldBlock, h.tryLock(latecomer, "c", LockShare))
	latecomerDone := h.lockBlocked(latecomer, "c", LockShare)

	h.j.Commit(rowHolder)
	assert.Equal(t, outcome{res: LockGranted}, h.await(ddlDone))
	h.requireStillBlocked(latecomerDone)

	// The table's holder can still lock and write its rows.
	assert.Equal(t, LockGranted, h.lock(ddl, "c", LockUpdate))
	res, err = h.j.Write(context.Background(), ddl, WriteRequest{Relation: testRel, Target: TableTarget(), Compare: bytes.Compare, Op: WriteUpdate})
	require.NoError(t, err)
	require.Equal(t, LockGranted, res)

	// Once the table's writer commits, every row is suspect to a snapshot that predates it.
	h.j.Commit(ddl)
	assert.Equal(t, outcome{res: LockRowUpdated}, h.await(latecomerDone))
	assert.Equal(t, LockRowUpdated, h.tryLock(latecomer, "never-touched", LockShare))

	later := h.j.Begin(4)
	assert.Equal(t, LockGranted, h.tryLock(later, "never-touched", LockShare))

	// A share lock on the table allows row share locks and refuses row writes.
	res, err = h.j.Lock(context.Background(), later, tableReq(LockShare, WaitError))
	require.NoError(t, err)
	require.Equal(t, LockGranted, res)
	h.j.Abort(latecomer)
	another := h.j.Begin(5)
	assert.Equal(t, LockGranted, h.tryLock(another, "d", LockShare))
	assert.Equal(t, LockWouldBlock, h.tryLock(another, "d", LockNoKeyUpdate))

	h.j.Commit(later)
	h.j.Commit(another)
	h.requireEmpty()
}

func TestRelationsAreIndependent(t *testing.T) {
	h := newHarness(t)
	a := h.j.Begin(1)
	b := h.j.Begin(2)
	require.Equal(t, LockGranted, h.lock(a, "k", LockUpdate))

	otherBranch := testRel
	otherBranch.Branch = "workingSets/heads/feature"
	otherTable := testRel
	otherTable.Table = doltdb.TableName{Name: "t", Schema: "s"}
	otherDatabase := testRel
	otherDatabase.Database = "db2"

	for _, rel := range []RelationKey{otherBranch, otherTable, otherDatabase} {
		res, err := h.j.Lock(context.Background(), b, lockReq(rel, "k", LockUpdate, WaitError))
		require.NoError(t, err)
		assert.Equal(t, LockGranted, res)
	}

	assert.Equal(t, LockWouldBlock, h.tryLock(b, "k", LockUpdate))
	assert.Len(t, h.j.Locks(), 4)

	// A write committed on one branch says nothing about the same row on another.
	require.Equal(t, LockGranted, h.write(a, "k", WriteUpdate))
	c := h.j.Begin(3)
	h.j.Commit(a)
	assert.Equal(t, LockRowUpdated, h.tryLock(c, "k", LockShare))
	h.j.Abort(b)
	res, err := h.j.Lock(context.Background(), c, lockReq(otherBranch, "k", LockShare, WaitError))
	require.NoError(t, err)
	assert.Equal(t, LockGranted, res)

	h.j.Commit(c)
	h.requireEmpty()
}

func TestKeysAreMatchedWithTheRelationsComparator(t *testing.T) {
	h := newHarness(t)
	caseInsensitive := func(left, right []byte) int {
		return bytes.Compare(bytes.ToLower(left), bytes.ToLower(right))
	}

	a := h.j.Begin(1)
	b := h.j.Begin(2)
	req := lockReq(testRel, "Key", LockUpdate, WaitSkip)
	req.Compare = caseInsensitive
	res, err := h.j.Lock(context.Background(), a, req)
	require.NoError(t, err)
	require.Equal(t, LockGranted, res)

	req.Target = RowTarget([]byte("kEY"))
	res, err = h.j.Lock(context.Background(), b, req)
	require.NoError(t, err)
	assert.Equal(t, LockWouldBlock, res)

	req.Target = RowTarget([]byte("key2"))
	res, err = h.j.Lock(context.Background(), b, req)
	require.NoError(t, err)
	assert.Equal(t, LockGranted, res)

	h.j.Abort(a)
	h.j.Abort(b)
	h.requireEmpty()
}

func TestKeyFormatChange(t *testing.T) {
	h := newHarness(t)
	oldFormat := hash.Of([]byte("old schema"))
	newFormat := hash.Of([]byte("new schema"))
	request := func(key string, mode LockMode, format hash.Hash) LockRequest {
		req := lockReq(testRel, key, mode, WaitSkip)
		req.KeyFormat = format
		if format == newFormat {
			// Keys of the new format are ordered differently, and comparing one with a key of the old format
			// would be a bug.
			req.Compare = func(left, right []byte) int {
				require.NotContains(t, string(left)+string(right), "old")
				return -bytes.Compare(left, right)
			}
		}

		return req
	}

	lock := func(txn *Txn, key string, mode LockMode, format hash.Hash) LockResult {
		res, err := h.j.Lock(context.Background(), txn, request(key, mode, format))
		require.NoError(t, err)
		return res
	}

	holder := h.j.Begin(1)
	writer := h.j.Begin(2)
	waiter := h.j.Begin(3)
	old := h.j.Begin(4)

	require.Equal(t, LockGranted, lock(holder, "old-a", LockShare, oldFormat))
	wreq := writeReq(testRel, "old-b", WriteUpdate)
	wreq.KeyFormat = oldFormat
	res, err := h.j.Write(context.Background(), writer, wreq)
	require.NoError(t, err)
	require.Equal(t, LockGranted, res)
	h.j.Commit(writer)

	blocked := h.blocked(waiter, func() (LockResult, error) {
		req := request("old-a", LockUpdate, oldFormat)
		req.WaitPolicy = WaitBlock
		return h.j.Lock(context.Background(), waiter, req)
	})

	// The first request with keys of a new format moves everything known about rows to the table as a whole.
	changer := h.j.Begin(5)
	assert.Equal(t, LockGranted, lock(changer, "new-a", LockShare, newFormat))
	assert.ErrorIs(t, h.await(blocked).err, ErrRelationChanged)

	// The share lock that was on a row now covers every row.
	assert.Equal(t, LockWouldBlock, lock(changer, "new-b", LockUpdate, newFormat))
	assert.Equal(t, LockGranted, lock(changer, "new-b", LockShare, newFormat))

	// The write that was to a row now counts against every row, for a snapshot that can't see it.
	assert.Equal(t, LockRowUpdated, lock(old, "new-c", LockShare, newFormat))
	assert.Equal(t, 1, h.j.RetainedWrites())

	h.j.Commit(holder)
	assert.Equal(t, LockGranted, lock(changer, "new-b", LockUpdate, newFormat))

	h.j.Abort(waiter)
	h.j.Abort(old)
	h.j.Commit(changer)
	h.requireEmpty()
}

func TestBranchDeleteAndRename(t *testing.T) {
	h := newHarness(t)
	feature := testRel
	feature.Branch = "heads/feature"
	renamed := testRel
	renamed.Branch = "heads/renamed"
	tryLock := func(txn *Txn, rel RelationKey, key string) LockResult {
		res, err := h.j.Lock(context.Background(), txn, lockReq(rel, key, LockUpdate, WaitSkip))
		require.NoError(t, err)
		return res
	}

	holder := h.j.Begin(1)
	other := h.j.Begin(2)
	require.Equal(t, LockGranted, tryLock(holder, feature, "k"))
	require.Equal(t, LockGranted, tryLock(holder, testRel, "k"))

	// Renaming a branch takes the locks on its tables along, and replaces whatever was held under the new name.
	require.Equal(t, LockGranted, tryLock(other, renamed, "k"))
	h.j.RecordRefUpdate(nil, RefUpdateRecord{
		Database:  feature.Database,
		Kind:      RefRename,
		Ref:       "refs/heads/feature",
		NewRef:    "refs/heads/renamed",
		Branch:    feature.Branch,
		NewBranch: renamed.Branch,
	})
	assert.Equal(t, LockWouldBlock, tryLock(other, renamed, "k"))
	assert.Equal(t, LockGranted, tryLock(other, feature, "k"))
	assert.Equal(t, LockWouldBlock, tryLock(other, testRel, "k"))

	// Deleting a branch drops the locks on its tables, and requests waiting for them give up.
	waiter := h.j.Begin(3)
	blocked := h.blocked(waiter, func() (LockResult, error) {
		return h.j.Lock(context.Background(), waiter, lockReq(renamed, "k", LockUpdate, WaitBlock))
	})
	h.j.RecordRefUpdate(nil, RefUpdateRecord{
		Database: renamed.Database,
		Kind:     RefDelete,
		Ref:      "refs/heads/renamed",
		Branch:   renamed.Branch,
	})
	assert.ErrorIs(t, h.await(blocked).err, ErrRelationChanged)
	assert.Equal(t, LockGranted, tryLock(waiter, renamed, "k"))

	// Other branches, and other kinds of ref update, are left alone.
	h.j.RecordRefUpdate(nil, RefUpdateRecord{Database: testRel.Database, Kind: RefAdvance, Ref: "refs/heads/main", Branch: testRel.Branch})
	h.j.RecordRefUpdate(nil, RefUpdateRecord{Database: testRel.Database, Kind: RefDelete, Ref: "refs/tags/v1"})
	assert.Equal(t, LockWouldBlock, tryLock(other, testRel, "k"))

	h.j.Commit(holder)
	h.j.Commit(other)
	h.j.Commit(waiter)
	h.requireEmpty()
}

// recordingSink collects the records appended to a journal.
type recordingSink struct {
	records []Record
}

func (s *recordingSink) Append(rec Record) {
	s.records = append(s.records, rec)
}

func TestEveryChangeIsAppendedAsARecord(t *testing.T) {
	h := newHarness(t)
	sink := &recordingSink{}
	h.j.SetRecordSink(sink)

	txn := h.j.Begin(7)
	require.Equal(t, LockGranted, h.lock(txn, "k", LockShare))
	level, err := h.j.SavepointBegin(txn)
	require.NoError(t, err)
	res, err := h.j.Write(context.Background(), txn, WriteRequest{
		Relation:    testRel,
		Target:      RowTarget([]byte("k")),
		Compare:     bytes.Compare,
		Op:          WriteUpdate,
		KeysChanged: true,
		Successor:   []byte("k2"),
		Statement:   3,
	})
	require.NoError(t, err)
	require.Equal(t, LockGranted, res)
	require.NoError(t, h.j.SavepointRelease(txn, level))
	_, err = h.j.SavepointBegin(txn)
	require.NoError(t, err)
	require.NoError(t, h.j.SavepointRollback(txn, level))

	h.j.RecordRefUpdate(txn, RefUpdateRecord{Database: "db", Kind: RefCreate, Ref: "refs/heads/feature"})
	h.j.RecordSchemaChange(txn, SchemaChangeRecord{Relation: testRel, Kind: SchemaAlterTable})
	h.j.RecordRefUpdate(nil, RefUpdateRecord{Database: "db", Kind: RefDelete, Ref: "refs/heads/feature"})

	observer := h.j.Begin(8)
	h.j.Commit(txn)
	h.j.Abort(observer)

	for i, rec := range sink.records {
		if begin, ok := rec.(TxnBeginRecord); ok {
			require.False(t, begin.StartTime.IsZero())
			begin.StartTime = time.Time{}
			sink.records[i] = begin
		}
	}

	id := txn.ID()
	key := RowTarget([]byte("k"))
	assert.Equal(t, []Record{
		TxnBeginRecord{Txn: id, SessionID: 7, Snapshot: Snapshot{OldestRunning: id, NextTxn: id + 1}},
		RowLockRecord{Txn: id, Relation: testRel, Target: key, Mode: LockShare},
		SavepointBeginRecord{Txn: id, Level: 1},
		RowLockRecord{Txn: id, Level: 1, Relation: testRel, Target: key, Mode: LockUpdate},
		RowWriteRecord{Txn: id, Level: 1, Relation: testRel, Target: key, Statement: 3, Op: WriteUpdate, KeysChanged: true, Successor: []byte("k2")},
		SavepointReleaseRecord{Txn: id, Level: 1},
		SavepointBeginRecord{Txn: id, Level: 1},
		SavepointRollbackRecord{Txn: id, Level: 1},
		RefUpdateRecord{Txn: id, Database: "db", Kind: RefCreate, Ref: "refs/heads/feature"},
		SchemaChangeRecord{Txn: id, Relation: testRel, Kind: SchemaAlterTable},
		RefUpdateRecord{Database: "db", Kind: RefDelete, Ref: "refs/heads/feature"},
		TxnBeginRecord{Txn: id + 1, SessionID: 8, Snapshot: Snapshot{OldestRunning: id, NextTxn: id + 2, Running: []TxnID{id}}},
		TxnCommitRecord{Txn: id},
		TxnAbortRecord{Txn: id + 1},
		PruneRecord{Horizon: id + 2},
	}, sink.records)
	h.requireEmpty()
}

// TestConcurrentTransactions runs many transactions against a few rows and checks that the journal never lets two of
// them hold conflicting locks, and that it is left empty afterwards.
func TestConcurrentTransactions(t *testing.T) {
	const workers = 16
	const rows = 6
	iterations := 300
	if testing.Short() {
		iterations = 50
	}

	h := newHarness(t)
	h.j.SetDeadlockTimeout(time.Millisecond)

	// For each row, the number of transactions that currently believe they hold it exclusively, and in share mode.
	var exclusive, shared [rows]atomic.Int32
	var granted, refused, deadlocks atomic.Int64

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(worker)))
			for i := 0; i < iterations; i++ {
				txn := h.j.Begin(uint32(worker))
				var heldExclusive, heldShared []int

				for n := rng.Intn(4); n >= 0; n-- {
					row := rng.Intn(rows)
					mode := LockShare
					if rng.Intn(2) == 0 {
						mode = LockUpdate
					}

					key := fmt.Sprintf("row%d", row)
					var res LockResult
					var err error
					if mode == LockUpdate && rng.Intn(2) == 0 {
						res, err = h.j.Write(context.Background(), txn, writeReq(testRel, key, WriteUpdate))
						mode = LockNoKeyUpdate
					} else {
						res, err = h.j.Lock(context.Background(), txn, lockReq(testRel, key, mode, WaitPolicy(rng.Intn(2))))
					}

					if err != nil {
						if !assert.ErrorIs(t, err, ErrDeadlock) {
							return
						}

						deadlocks.Add(1)
						break
					}

					if res != LockGranted {
						refused.Add(1)
						continue
					}

					granted.Add(1)
					if mode == LockShare {
						shared[row].Add(1)
						heldShared = append(heldShared, row)
						assert.Zero(t, exclusive[row].Load()-countOf(heldExclusive, row), "share lock granted while another transaction holds the row exclusively")
					} else {
						exclusive[row].Add(1)
						heldExclusive = append(heldExclusive, row)
						assert.Zero(t, exclusive[row].Load()-countOf(heldExclusive, row), "two transactions hold the row exclusively")
						assert.Zero(t, shared[row].Load()-countOf(heldShared, row), "exclusive lock granted while another transaction shares the row")
					}
				}

				for _, row := range heldExclusive {
					exclusive[row].Add(-1)
				}

				for _, row := range heldShared {
					shared[row].Add(-1)
				}

				if rng.Intn(3) == 0 {
					h.j.Abort(txn)
				} else {
					h.j.Commit(txn)
				}
			}
		}()
	}

	wg.Wait()
	t.Logf("granted %d, refused %d, deadlocks %d", granted.Load(), refused.Load(), deadlocks.Load())
	assert.NotZero(t, granted.Load())
	h.requireEmpty()
}

func countOf(rows []int, row int) int32 {
	n := int32(0)
	for _, r := range rows {
		if r == row {
			n++
		}
	}

	return n
}
