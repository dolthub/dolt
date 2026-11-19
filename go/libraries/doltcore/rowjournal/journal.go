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

// Package rowjournal implements an in-memory journal of transactions and of the rows they lock and write. It is what
// gives a server row-level locking: it hands out transaction IDs, knows which transactions are running and which
// have committed, and for every row that a running or recently committed transaction has touched, records who holds
// which lock on it and who changed it.
//
// The journal does not store table data. Transactions still read from their own snapshot of the database and write
// to their own working root. The journal decides who may write a row and tells a transaction when a row it wants has
// been changed by a transaction it cannot see.
//
// The state of a Journal is the result of applying a sequence of Records, and every change to that state is made by
// appending a Record. Today the records are applied to in-memory indexes and then dropped. The same sequence can
// later be written to disk.
package rowjournal

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"
	"time"
)

// DefaultDeadlockTimeout is how long a lock request waits before checking a second time whether it is part of a
// deadlock. It checks a first time when it starts waiting.
const DefaultDeadlockTimeout = time.Second

// Journal is an in-memory journal of transactions, row locks and row writes, shared by every session of a server. It
// is safe for concurrent use.
type Journal struct {
	// mu guards every field below, and every relation, entry and Txn field the journal owns. It is held only for
	// in-memory work, never while a lock request waits.
	mu sync.Mutex
	// nextTxn is the next transaction ID to hand out.
	nextTxn TxnID
	// active is the transactions in progress.
	active map[TxnID]*Txn
	// retired is the committed transactions whose writes some snapshot may not see yet.
	retired txnHeap
	// relations is the state of every table that has an entry.
	relations map[RelationKey]*relation
	// nextWaiterSeq numbers waiting lock requests in arrival order.
	nextWaiterSeq uint64
	// deadlockTimeout is how long a lock request waits before checking for a deadlock a second time.
	deadlockTimeout time.Duration
	// sink, when set, receives every record appended.
	sink RecordSink
}

// NewJournal returns an empty Journal.
func NewJournal() *Journal {
	return &Journal{
		nextTxn:         1,
		active:          make(map[TxnID]*Txn),
		relations:       make(map[RelationKey]*relation),
		deadlockTimeout: DefaultDeadlockTimeout,
	}
}

// SetDeadlockTimeout sets how long a lock request waits before checking a second time whether it is part of a
// deadlock.
func (j *Journal) SetDeadlockTimeout(d time.Duration) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.deadlockTimeout = d
}

// SetRecordSink sets the sink that receives every record appended from now on. A nil sink receives nothing.
func (j *Journal) SetRecordSink(sink RecordSink) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.sink = sink
}

// Begin registers a new transaction for the session given, assigns it the next transaction ID, and takes its
// snapshot.
func (j *Journal) Begin(sessionID uint32) *Txn {
	j.mu.Lock()
	defer j.mu.Unlock()

	id := j.nextTxn
	snapshot := Snapshot{
		OldestRunning: id,
		NextTxn:       id + 1,
	}

	if len(j.active) > 0 {
		snapshot.Running = make([]TxnID, 0, len(j.active))
		for running := range j.active {
			snapshot.Running = append(snapshot.Running, running)
		}

		sort.Slice(snapshot.Running, func(a, b int) bool { return snapshot.Running[a] < snapshot.Running[b] })
		snapshot.OldestRunning = snapshot.Running[0]
	}

	j.append(TxnBeginRecord{
		Txn:       id,
		SessionID: sessionID,
		StartTime: time.Now(),
		Snapshot:  snapshot,
	}, hint{})
	return j.active[id]
}

// Commit marks the transaction as committed and releases its locks. Its writes stay in the journal until every
// snapshot can see them. Commit does nothing if the transaction already ended.
func (j *Journal) Commit(txn *Txn) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if txn.status != TxnInProgress {
		return
	}

	j.append(TxnCommitRecord{Txn: txn.id}, hint{txn: txn})
	j.pruneIfPossible()
}

// Abort marks the transaction as aborted, discards its writes and releases its locks. Abort does nothing if the
// transaction already ended.
func (j *Journal) Abort(txn *Txn) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if txn.status != TxnInProgress {
		return
	}

	j.append(TxnAbortRecord{Txn: txn.id}, hint{txn: txn})
	j.pruneIfPossible()
}

// SavepointBegin opens a savepoint in the transaction and returns its level. Locks and writes made from now on belong
// to that level until the savepoint is released or rolled back.
func (j *Journal) SavepointBegin(txn *Txn) (uint32, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if txn.status != TxnInProgress {
		return 0, ErrTxnNotInProgress
	}

	level := txn.level + 1
	j.append(SavepointBeginRecord{Txn: txn.id, Level: level}, hint{txn: txn})
	return level, nil
}

// SavepointRelease closes the savepoint at |level| and every savepoint opened after it. The locks and writes they
// made are kept, and now belong to the level below.
func (j *Journal) SavepointRelease(txn *Txn, level uint32) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if err := checkSavepoint(txn, level); err != nil {
		return err
	}

	j.append(SavepointReleaseRecord{Txn: txn.id, Level: level}, hint{txn: txn})
	return nil
}

// SavepointRollback discards the writes and releases the locks made since the savepoint at |level| was opened. A lock
// that was strengthened since then goes back to the mode it had. The savepoint itself stays open, and savepoints
// opened after it are closed.
func (j *Journal) SavepointRollback(txn *Txn, level uint32) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if err := checkSavepoint(txn, level); err != nil {
		return err
	}

	j.append(SavepointRollbackRecord{Txn: txn.id, Level: level}, hint{txn: txn})
	return nil
}

func checkSavepoint(txn *Txn, level uint32) error {
	if txn.status != TxnInProgress {
		return ErrTxnNotInProgress
	}

	if level == 0 || level > txn.level {
		return fmt.Errorf("rowjournal: transaction %d has no savepoint at level %d", txn.id, level)
	}

	return nil
}

// RecordRefUpdate appends a record of a version control operation's change to a ref. |txn| may be nil for a change
// made outside any transaction.
func (j *Journal) RecordRefUpdate(txn *Txn, rec RefUpdateRecord) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if txn != nil {
		rec.Txn = txn.id
	}

	j.append(rec, hint{txn: txn})
}

// RecordSchemaChange appends a record of a DDL statement's change to a table. |txn| may be nil for a change made
// outside any transaction.
func (j *Journal) RecordSchemaChange(txn *Txn, rec SchemaChangeRecord) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if txn != nil {
		rec.Txn = txn.id
	}

	j.append(rec, hint{txn: txn})
}

// hint carries what the caller of append has already looked up, to save apply from looking it up again by
// identifier. A zero hint is always valid.
type hint struct {
	txn   *Txn
	entry *entry
}

// append adds a record to the journal: it hands the record to the sink and applies it to the in-memory state. Every
// change to the journal's durable state goes through here. Requires j.mu.
func (j *Journal) append(rec Record, h hint) {
	if j.sink != nil {
		j.sink.Append(rec)
	}

	txn := h.txn
	if txn == nil && rec.TxnID() != 0 {
		txn = j.active[rec.TxnID()]
	}

	switch r := rec.(type) {
	case TxnBeginRecord:
		j.nextTxn = r.Txn + 1
		j.active[r.Txn] = &Txn{
			journal:   j,
			id:        r.Txn,
			sessionID: r.SessionID,
			startTime: r.StartTime,
			snapshot:  r.Snapshot,
			touched:   make(map[*entry]struct{}),
			waits:     make(map[*waiter]struct{}),
		}
	case TxnCommitRecord:
		j.endTxn(txn, TxnCommitted)
	case TxnAbortRecord:
		j.endTxn(txn, TxnAborted)
	case SavepointBeginRecord:
		txn.level = r.Level
	case SavepointReleaseRecord:
		j.releaseLevel(txn, r.Level)
	case SavepointRollbackRecord:
		j.undoLevel(txn, r.Level)
		txn.level = r.Level
	case RowLockRecord:
		e := h.entry
		if e == nil {
			e = j.relations[r.Relation].entryFor(r.Target, true)
		}

		j.addLocker(e, locker{txn: txn, level: r.Level, mode: r.Mode})
	case RowWriteRecord:
		e := h.entry
		if e == nil {
			e = j.relations[r.Relation].entryFor(r.Target, true)
		}

		e.writes = append(e.writes, write{
			txn:         txn,
			level:       r.Level,
			statement:   r.Statement,
			op:          r.Op,
			keysChanged: r.KeysChanged,
			successor:   r.Successor,
			value:       r.Value,
		})
		txn.touched[e] = struct{}{}
		txn.wrote = true
	case RefUpdateRecord, SchemaChangeRecord:
		// Recorded for the sink only. Nothing in the journal's state depends on these yet.
	case PruneRecord:
		j.prune(r.Horizon)
	default:
		panic(fmt.Sprintf("rowjournal: unknown record type %T", rec))
	}
}

// relationFor returns the relation for |key|, creating it when |create| is set and it does not exist. Requires j.mu.
func (j *Journal) relationFor(key RelationKey, compare KeyCompare, create bool) *relation {
	rel, ok := j.relations[key]
	if ok || !create {
		return rel
	}

	rel = newRelation(key, compare)
	j.relations[key] = rel
	return rel
}

// addLocker records a lock on an entry. Requires j.mu.
func (j *Journal) addLocker(e *entry, l locker) {
	e.lockers = append(e.lockers, l)
	l.txn.touched[e] = struct{}{}
	if e.isTable {
		return
	}

	counts := e.rel.rowLocks[l.txn]
	if counts == nil {
		counts = new([numLockModes]int)
		e.rel.rowLocks[l.txn] = counts
	}

	counts[l.mode]++
}

// removeLockers releases the locks that |txn| took on |e| at |minLevel| or deeper, and returns whether there were
// any. Requires j.mu.
func (j *Journal) removeLockers(e *entry, txn *Txn, minLevel uint32) bool {
	kept := e.lockers[:0]
	removed := false
	for _, l := range e.lockers {
		if l.txn != txn || l.level < minLevel {
			kept = append(kept, l)
			continue
		}

		removed = true
		if e.isTable {
			continue
		}

		counts := e.rel.rowLocks[txn]
		counts[l.mode]--
		if *counts == ([numLockModes]int{}) {
			delete(e.rel.rowLocks, txn)
		}
	}

	clear(e.lockers[len(kept):])
	e.lockers = kept
	return removed
}

// removeWrites discards the writes that |txn| made to |e| at |minLevel| or deeper. Requires j.mu.
func (j *Journal) removeWrites(e *entry, txn *Txn, minLevel uint32) {
	kept := e.writes[:0]
	for _, w := range e.writes {
		if w.txn != txn || w.level < minLevel {
			kept = append(kept, w)
		}
	}

	clear(e.writes[len(kept):])
	e.writes = kept
}

// removeIfEmpty drops an entry that nothing refers to any more, and its relation if that was its last entry.
// Requires j.mu.
func (j *Journal) removeIfEmpty(e *entry) {
	if e.removed || !e.isEmpty() {
		return
	}

	rel := e.rel
	if !e.isTable {
		rel.rows.Delete(e)
		e.removed = true
	}

	if rel.isEmpty() && j.relations[rel.key] == rel {
		delete(j.relations, rel.key)
	}
}

// release drops the locks, and when |dropWrites| is set the writes, that |txn| made at |minLevel| or deeper, and
// wakes every request that may have been waiting for them. Requires j.mu.
func (j *Journal) release(txn *Txn, minLevel uint32, dropWrites bool) {
	// For each relation touched, whether a lock on the table as a whole was released.
	relations := make(map[*relation]bool)
	for e := range txn.touched {
		if e.removed {
			continue
		}

		unlocked := j.removeLockers(e, txn, minLevel)
		if dropWrites {
			j.removeWrites(e, txn, minLevel)
		}

		e.signalWaiters()
		relations[e.rel] = relations[e.rel] || (e.isTable && unlocked)
	}

	for rel, tableUnlocked := range relations {
		if !tableUnlocked {
			// Requests for the table as a whole wait for every row lock in the relation.
			rel.table.signalWaiters()
			continue
		}

		for w := range rel.waiters {
			w.signal()
		}
	}

	for e := range txn.touched {
		j.removeIfEmpty(e)
		if e.removed || (e.isTable && e.isEmpty()) {
			delete(txn.touched, e)
		}
	}
}

// endTxn finishes a transaction. Requires j.mu.
func (j *Journal) endTxn(txn *Txn, status TxnStatus) {
	txn.status = status
	j.release(txn, 0, status == TxnAborted)
	delete(j.active, txn.id)

	// Wake the transaction's own waiting requests, so that they notice it has ended.
	for w := range txn.waits {
		w.signal()
	}

	if status == TxnCommitted && txn.wrote {
		heap.Push(&j.retired, txn)
	} else {
		txn.touched = nil
	}
}

// undoLevel discards what |txn| did at |level| or deeper. Requires j.mu.
func (j *Journal) undoLevel(txn *Txn, level uint32) {
	j.release(txn, level, true)
}

// releaseLevel moves what |txn| did at |level| or deeper to the level below. Requires j.mu.
func (j *Journal) releaseLevel(txn *Txn, level uint32) {
	for e := range txn.touched {
		for i := range e.lockers {
			if e.lockers[i].txn == txn && e.lockers[i].level >= level {
				e.lockers[i].level = level - 1
			}
		}

		for i := range e.writes {
			if e.writes[i].txn == txn && e.writes[i].level >= level {
				e.writes[i].level = level - 1
			}
		}
	}

	txn.level = level - 1
}

// horizon returns the transaction ID below which every committed transaction is visible to every snapshot that exists
// or can still be taken. Requires j.mu.
func (j *Journal) horizon() TxnID {
	horizon := j.nextTxn
	for _, txn := range j.active {
		if txn.snapshot.OldestRunning < horizon {
			horizon = txn.snapshot.OldestRunning
		}
	}

	return horizon
}

// pruneIfPossible appends a prune record if there are committed writes that every snapshot can now see. Requires
// j.mu.
func (j *Journal) pruneIfPossible() {
	if len(j.retired) == 0 {
		return
	}

	horizon := j.horizon()
	if j.retired[0].id < horizon {
		j.append(PruneRecord{Horizon: horizon}, hint{})
	}
}

// prune discards the writes of committed transactions with IDs below |horizon|. Requires j.mu.
func (j *Journal) prune(horizon TxnID) {
	for len(j.retired) > 0 && j.retired[0].id < horizon {
		txn := heap.Pop(&j.retired).(*Txn)
		for e := range txn.touched {
			if e.removed {
				continue
			}

			j.removeWrites(e, txn, 0)
			j.removeIfEmpty(e)
		}

		txn.touched = nil
	}
}
