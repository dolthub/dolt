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
	"github.com/google/btree"
)

// KeyCompare orders two row keys of the same table. Keys that compare equal are the same row, even when their bytes
// differ, as they can under a case-insensitive collation.
type KeyCompare func(left, right []byte) int

// rowIndexDegree is the branching factor of the ordered index of row entries in a relation.
const rowIndexDegree = 32

// relation holds the journal's state for one table on one branch. All of its fields are guarded by Journal.mu.
type relation struct {
	key     RelationKey
	compare KeyCompare
	// rows holds an entry for every row that is locked, waited on, or has a write that some snapshot may not see.
	rows *btree.BTreeG[*entry]
	// table is the entry for locks and writes that target the table as a whole.
	table *entry
	// rowLocks counts, for each transaction that holds row locks in this relation, how many it holds in each mode.
	// A request for a table lock conflicts with these.
	rowLocks map[*Txn]*[numLockModes]int
	// waiters is every lock request waiting on any entry in this relation.
	waiters map[*waiter]struct{}
	// probe is a scratch entry for lookups in rows.
	probe entry
}

func newRelation(key RelationKey, compare KeyCompare) *relation {
	rel := &relation{
		key:      key,
		compare:  compare,
		rowLocks: make(map[*Txn]*[numLockModes]int),
		waiters:  make(map[*waiter]struct{}),
	}
	rel.rows = btree.NewG(rowIndexDegree, func(a, b *entry) bool {
		return compare(a.key, b.key) < 0
	})
	rel.table = &entry{rel: rel, isTable: true}
	return rel
}

// entryFor returns the entry for |target|, creating it when |create| is set and it does not exist.
func (rel *relation) entryFor(target Target, create bool) *entry {
	if target.WholeTable {
		return rel.table
	}

	rel.probe.key = target.Key
	e, ok := rel.rows.Get(&rel.probe)
	rel.probe.key = nil
	if ok || !create {
		return e
	}

	e = &entry{rel: rel, key: append([]byte(nil), target.Key...)}
	rel.rows.ReplaceOrInsert(e)
	return e
}

// isEmpty returns whether the relation holds no state at all.
func (rel *relation) isEmpty() bool {
	return rel.rows.Len() == 0 && rel.table.isEmpty() && len(rel.waiters) == 0
}

// entry is the journal's state for one lock target: a row, or a table as a whole. An entry belongs to no transaction.
// It exists for as long as anything refers to the target, and a target with no entry is unlocked and unchanged as far
// as every snapshot is concerned.
type entry struct {
	rel     *relation
	key     []byte
	isTable bool
	// removed is set once a row entry has been taken out of its relation's index, to tell stale references apart from
	// a newer entry for the same key.
	removed bool
	// lockers is the locks held on the target. A transaction appears once for each savepoint level at which it took
	// a lock, so that rolling back a savepoint restores whatever weaker lock was held before it.
	lockers []locker
	// writes is the changes made to the target, oldest first, by transactions that are still in progress or that
	// committed too recently for every snapshot to see them.
	writes []write
	// waiters is the lock requests waiting on the target, in arrival order.
	waiters []*waiter
}

// locker is a lock held on an entry.
type locker struct {
	txn   *Txn
	level uint32
	mode  LockMode
}

// write is a change recorded on an entry.
type write struct {
	txn         *Txn
	level       uint32
	statement   uint32
	op          WriteOp
	keysChanged bool
	successor   []byte
	value       []byte
}

// waiter is a lock request that could not be granted immediately.
type waiter struct {
	txn   *Txn
	entry *entry
	mode  LockMode
	// seq orders waiters across all entries by arrival.
	seq uint64
	// wake is signaled whenever the conditions the waiter is blocked on may have changed.
	wake chan struct{}
}

func (w *waiter) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (e *entry) isEmpty() bool {
	return len(e.lockers) == 0 && len(e.writes) == 0 && len(e.waiters) == 0
}

// target returns the Target this entry stands for.
func (e *entry) target() Target {
	if e.isTable {
		return TableTarget()
	}

	return RowTarget(e.key)
}

// heldMode returns the strongest mode in which |txn| holds a lock on the entry, or LockNone.
func (e *entry) heldMode(txn *Txn) LockMode {
	held := LockNone
	for _, l := range e.lockers {
		if l.txn == txn && l.mode > held {
			held = l.mode
		}
	}

	return held
}

// signalWaiters wakes every request waiting on the entry.
func (e *entry) signalWaiters() {
	for _, w := range e.waiters {
		w.signal()
	}
}
