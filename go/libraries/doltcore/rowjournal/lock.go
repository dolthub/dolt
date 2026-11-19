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
	"context"
	"errors"
	"math"
	"time"
)

var (
	// ErrDeadlock is returned to a lock request that found itself waiting, directly or through other transactions,
	// for its own transaction.
	ErrDeadlock = errors.New("deadlock detected")
	// ErrLockTimeout is returned to a lock request that waited longer than its timeout.
	ErrLockTimeout = errors.New("lock wait timeout exceeded")
	// ErrLockNotAvailable is returned to a lock request with the WaitError policy that could not be granted
	// immediately.
	ErrLockNotAvailable = errors.New("could not obtain lock on row")
	// ErrTxnNotInProgress is returned when a transaction that has committed or aborted is used.
	ErrTxnNotInProgress = errors.New("transaction is not in progress")
)

// LockRequest asks for a lock on a row or a table.
type LockRequest struct {
	Relation RelationKey
	Target   Target
	// Compare orders the row keys of the relation. It is used the first time the journal sees the relation.
	Compare KeyCompare
	Mode    LockMode
	// WaitPolicy says what to do if the lock cannot be granted immediately.
	WaitPolicy WaitPolicy
	// Timeout bounds how long the request waits. Zero waits indefinitely.
	Timeout time.Duration
}

// WriteRequest records a change to a row, or with a table target, to an unspecified set of the rows in a table.
type WriteRequest struct {
	Relation RelationKey
	Target   Target
	// Compare orders the row keys of the relation. It is used the first time the journal sees the relation.
	Compare KeyCompare
	Op      WriteOp
	// KeysChanged is whether an update changes any column of the row's key.
	KeysChanged bool
	// Successor is the row's new key when an update changes it.
	Successor []byte
	// Statement numbers the statement making the write within its transaction.
	Statement uint32
	// Timeout bounds how long the request waits for the lock it needs. Zero waits indefinitely.
	Timeout time.Duration
}

// lockMode returns the lock a write needs.
func (r WriteRequest) lockMode() LockMode {
	if r.Op == WriteUpdate && !r.KeysChanged {
		return LockNoKeyUpdate
	}

	return LockUpdate
}

// Lock takes a lock for the transaction, waiting for conflicting locks to be released according to the request's
// wait policy. The lock is held until the transaction ends, or until the savepoint it was taken in is rolled back.
//
// A result of LockGranted means the lock is held and the target is unchanged since the transaction's snapshot. Any
// other result means no lock was taken. An error is returned when the wait was abandoned: on a deadlock, a timeout,
// a canceled context, or the WaitError policy.
func (j *Journal) Lock(ctx context.Context, txn *Txn, req LockRequest) (LockResult, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if txn.status != TxnInProgress {
		return 0, ErrTxnNotInProgress
	}

	e := j.relationFor(req.Relation, req.Compare, true).entryFor(req.Target, true)
	return j.acquire(ctx, txn, e, req.Mode, req.WaitPolicy, req.Timeout, false)
}

// Write takes the lock that a change to a row requires, waiting for conflicting locks to be released, and then
// records the change.
//
// A result of LockGranted means the write was recorded. LockRowUpdated and LockRowDeleted mean that a transaction
// this one cannot see committed a change to the row, and LockKeyExists that one created the row this transaction is
// trying to insert; in those cases nothing was recorded.
func (j *Journal) Write(ctx context.Context, txn *Txn, req WriteRequest) (LockResult, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if txn.status != TxnInProgress {
		return 0, ErrTxnNotInProgress
	}

	e := j.relationFor(req.Relation, req.Compare, true).entryFor(req.Target, true)
	res, err := j.acquire(ctx, txn, e, req.lockMode(), WaitBlock, req.Timeout, req.Op == WriteInsert)
	if err != nil || res != LockGranted {
		return res, err
	}

	var successor []byte
	if req.Successor != nil {
		successor = append(successor, req.Successor...)
	}

	j.append(RowWriteRecord{
		Txn:         txn.id,
		Level:       txn.level,
		Relation:    req.Relation,
		Target:      e.target(),
		Statement:   req.Statement,
		Op:          req.Op,
		KeysChanged: req.KeysChanged,
		Successor:   successor,
	}, hint{txn: txn, entry: e})
	return LockGranted, nil
}

// acquire takes a lock on |e| for |txn|. It is called with j.mu held and returns with it held, but releases it while
// waiting.
func (j *Journal) acquire(
	ctx context.Context,
	txn *Txn,
	e *entry,
	mode LockMode,
	policy WaitPolicy,
	timeout time.Duration,
	forInsert bool,
) (LockResult, error) {
	var w *waiter
	var timeoutC, deadlockC <-chan time.Time

	// done is called exactly once, when the request stops waiting for any reason.
	done := func() {
		if w != nil {
			j.removeWaiter(w)
		}

		j.removeIfEmpty(e)
	}

	for {
		if res := concurrentChange(txn, e, mode, forInsert); res != LockGranted {
			done()
			return res, nil
		}

		if e.heldMode(txn).Covers(mode) {
			done()
			return LockGranted, nil
		}

		seq := uint64(math.MaxUint64)
		if w != nil {
			seq = w.seq
		}

		if !j.isBlocked(txn, e, mode, seq) {
			j.append(RowLockRecord{
				Txn:      txn.id,
				Level:    txn.level,
				Relation: e.rel.key,
				Target:   e.target(),
				Mode:     mode,
			}, hint{txn: txn, entry: e})
			done()
			return LockGranted, nil
		}

		switch policy {
		case WaitSkip:
			done()
			return LockWouldBlock, nil
		case WaitError:
			done()
			return 0, ErrLockNotAvailable
		}

		if w == nil {
			w = j.addWaiter(txn, e, mode)

			if timeout > 0 {
				timer := time.NewTimer(timeout)
				defer timer.Stop()
				timeoutC = timer.C
			}

			timer := time.NewTimer(j.deadlockTimeout)
			defer timer.Stop()
			deadlockC = timer.C
		}

		// A cycle of waiting transactions is completed by whichever of them starts waiting last, so checking here
		// finds a deadlock the moment it forms, and refuses the request that would have formed it. The check is
		// repeated after the deadlock timeout in case a cycle was ever to form some other way.
		if j.isDeadlocked(txn) {
			done()
			return 0, ErrDeadlock
		}

		var err error
		checkDeadlock := false

		j.mu.Unlock()
		select {
		case <-w.wake:
		case <-deadlockC:
			checkDeadlock = true
		case <-timeoutC:
			err = ErrLockTimeout
		case <-ctx.Done():
			err = ctx.Err()
		}
		j.mu.Lock()

		if err == nil && txn.status != TxnInProgress {
			err = ErrTxnNotInProgress
		}

		if err == nil && checkDeadlock {
			deadlockC = nil
			if j.isDeadlocked(txn) {
				err = ErrDeadlock
			}
		}

		if err != nil {
			done()
			return 0, err
		}
	}
}

// concurrentChange reports whether a transaction that |txn| cannot see has committed a change to |e| that a lock of
// |mode| must not ignore. It returns LockGranted when there is none. Requires j.mu.
func concurrentChange(txn *Txn, e *entry, mode LockMode, forInsert bool) LockResult {
	if !e.isTable {
		// A change to the table as a whole may have touched any row.
		for _, w := range e.rel.table.writes {
			if txn.isConcurrentCommit(w.txn) {
				return LockRowUpdated
			}
		}
	}

	res := LockGranted
	for _, w := range e.writes {
		if !txn.isConcurrentCommit(w.txn) {
			continue
		}

		if w.op == WriteDelete {
			res = LockRowDeleted
			continue
		}

		if mode == LockKeyShare && w.op == WriteUpdate && !w.keysChanged {
			// A key share lock only protects the row's key, which this update left alone.
			continue
		}

		res = LockRowUpdated
	}

	if forInsert {
		switch res {
		case LockRowDeleted:
			// The key was free when the last concurrent transaction finished with it.
			return LockGranted
		case LockRowUpdated:
			return LockKeyExists
		}
	}

	return res
}

// isBlocked returns whether a request by |txn| for a lock of |mode| on |e| has to wait. |seq| is the request's
// position among waiters, or the maximum value if it is not waiting yet. Requires j.mu.
func (j *Journal) isBlocked(txn *Txn, e *entry, mode LockMode, seq uint64) bool {
	blocked := false
	j.eachBlocker(txn, e, mode, seq, func(*Txn) bool {
		blocked = true
		return false
	})
	return blocked
}

// eachBlocker calls |fn| with every transaction that a request by |txn| for a lock of |mode| on |e| has to wait for,
// until |fn| returns false. A transaction may be reported more than once. Requires j.mu.
func (j *Journal) eachBlocker(txn *Txn, e *entry, mode LockMode, seq uint64, fn func(*Txn) bool) {
	rel := e.rel

	// Transactions that hold a conflicting lock on the target itself.
	for _, l := range e.lockers {
		if l.txn != txn && l.mode.ConflictsWith(mode) && !fn(l.txn) {
			return
		}
	}

	if e.isTable {
		// A lock on the table conflicts with locks on its rows.
		for holder, counts := range rel.rowLocks {
			if holder == txn {
				continue
			}

			for held, n := range counts {
				if n > 0 && LockMode(held).ConflictsWith(mode) && !fn(holder) {
					return
				}
			}
		}
	} else {
		// A lock on a row conflicts with locks on its table.
		for _, l := range rel.table.lockers {
			if l.txn != txn && l.mode.ConflictsWith(mode) && !fn(l.txn) {
				return
			}
		}
	}

	// Requests that arrived earlier and conflict with this one go first, so that a stream of compatible lockers
	// cannot starve a request they all conflict with. A transaction that already holds a lock on the target, or on
	// the table the target is a row of, is exempt: making it queue behind a request that is waiting for it would
	// deadlock the two.
	holdsTarget := e.heldMode(txn) != LockNone || rel.table.heldMode(txn) != LockNone
	if !holdsTarget {
		for _, q := range e.waiters {
			if q.seq < seq && q.txn != txn && q.mode.ConflictsWith(mode) && !fn(q.txn) {
				return
			}
		}
	}

	// A row request also queues behind earlier requests for the table, unless its transaction already holds a lock
	// somewhere in the table, which that table request is itself waiting for.
	if e.isTable {
		return
	}

	if _, holdsRows := rel.rowLocks[txn]; holdsRows || rel.table.heldMode(txn) != LockNone {
		return
	}

	for _, q := range rel.table.waiters {
		if q.seq < seq && q.txn != txn && q.mode.ConflictsWith(mode) && !fn(q.txn) {
			return
		}
	}
}

// isDeadlocked returns whether |txn| is waiting, directly or through other waiting transactions, for itself.
// Requires j.mu.
func (j *Journal) isDeadlocked(txn *Txn) bool {
	visited := make(map[*Txn]struct{})

	var waitsForTxn func(t *Txn) bool
	waitsForTxn = func(t *Txn) bool {
		found := false
		for w := range t.waits {
			j.eachBlocker(t, w.entry, w.mode, w.seq, func(blocker *Txn) bool {
				if blocker == txn {
					found = true
					return false
				}

				if _, ok := visited[blocker]; ok {
					return true
				}

				visited[blocker] = struct{}{}
				found = waitsForTxn(blocker)
				return !found
			})

			if found {
				return true
			}
		}

		return false
	}

	return waitsForTxn(txn)
}

// addWaiter queues a request on an entry. Requires j.mu.
func (j *Journal) addWaiter(txn *Txn, e *entry, mode LockMode) *waiter {
	w := &waiter{
		txn:   txn,
		entry: e,
		mode:  mode,
		seq:   j.nextWaiterSeq,
		wake:  make(chan struct{}, 1),
	}
	j.nextWaiterSeq++

	e.waiters = append(e.waiters, w)
	e.rel.waiters[w] = struct{}{}
	txn.waits[w] = struct{}{}
	return w
}

// removeWaiter takes a request out of its entry's queue and wakes the requests that may have been queued behind it.
// Requires j.mu.
func (j *Journal) removeWaiter(w *waiter) {
	e := w.entry
	for i, q := range e.waiters {
		if q == w {
			e.waiters = append(e.waiters[:i], e.waiters[i+1:]...)
			break
		}
	}

	delete(e.rel.waiters, w)
	delete(w.txn.waits, w)

	if e.isTable {
		// Row requests queue behind requests for the table.
		for q := range e.rel.waiters {
			q.signal()
		}

		return
	}

	e.signalWaiters()
}
