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
	"sort"
	"time"
)

// TxnInfo describes a transaction in progress.
type TxnInfo struct {
	ID        TxnID
	SessionID uint32
	StartTime time.Time
	Snapshot  Snapshot
	// Level is the transaction's current savepoint level.
	Level uint32
}

// LockInfo describes a lock that is held, or a request for one that is waiting.
type LockInfo struct {
	Relation  RelationKey
	Target    Target
	Txn       TxnID
	SessionID uint32
	Mode      LockMode
	// Granted is true for a held lock and false for a waiting request.
	Granted bool
	// Level is the savepoint level at which a held lock was taken.
	Level uint32
	// BlockedBy is the transactions a waiting request is waiting for, in ascending order.
	BlockedBy []TxnID
}

// ActiveTxns returns the transactions in progress, in ascending order of ID.
func (j *Journal) ActiveTxns() []TxnInfo {
	j.mu.Lock()
	defer j.mu.Unlock()

	infos := make([]TxnInfo, 0, len(j.active))
	for _, txn := range j.active {
		infos = append(infos, TxnInfo{
			ID:        txn.id,
			SessionID: txn.sessionID,
			StartTime: txn.startTime,
			Snapshot:  txn.snapshot,
			Level:     txn.level,
		})
	}

	sort.Slice(infos, func(a, b int) bool { return infos[a].ID < infos[b].ID })
	return infos
}

// Locks returns every lock held and every lock request waiting, in no particular order.
func (j *Journal) Locks() []LockInfo {
	j.mu.Lock()
	defer j.mu.Unlock()

	var infos []LockInfo
	for _, rel := range j.relations {
		infos = j.appendLockInfos(infos, rel.table)
		rel.rows.Ascend(func(e *entry) bool {
			infos = j.appendLockInfos(infos, e)
			return true
		})
	}

	return infos
}

// BlockedBy returns the transactions that the transaction given is waiting for, in ascending order. It returns nil
// for a transaction that is not waiting.
func (j *Journal) BlockedBy(txn *Txn) []TxnID {
	j.mu.Lock()
	defer j.mu.Unlock()

	var blockers []TxnID
	for w := range txn.waits {
		blockers = append(blockers, j.blockersOf(w)...)
	}

	return sortedUnique(blockers)
}

// IsSessionBlocked returns whether any transaction of the session given is waiting for a lock.
func (j *Journal) IsSessionBlocked(sessionID uint32) bool {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, txn := range j.active {
		if txn.sessionID == sessionID && len(txn.waits) > 0 {
			return true
		}
	}

	return false
}

// RetainedWrites returns the number of row and table writes the journal is holding on to.
func (j *Journal) RetainedWrites() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	n := 0
	for _, rel := range j.relations {
		n += len(rel.table.writes)
		rel.rows.Ascend(func(e *entry) bool {
			n += len(e.writes)
			return true
		})
	}

	return n
}

// appendLockInfos describes the lockers and waiters of an entry. Requires j.mu.
func (j *Journal) appendLockInfos(infos []LockInfo, e *entry) []LockInfo {
	target := e.target()
	if !e.isTable {
		target.Key = append([]byte(nil), e.key...)
	}

	for _, l := range e.lockers {
		infos = append(infos, LockInfo{
			Relation:  e.rel.key,
			Target:    target,
			Txn:       l.txn.id,
			SessionID: l.txn.sessionID,
			Mode:      l.mode,
			Granted:   true,
			Level:     l.level,
		})
	}

	for _, w := range e.waiters {
		infos = append(infos, LockInfo{
			Relation:  e.rel.key,
			Target:    target,
			Txn:       w.txn.id,
			SessionID: w.txn.sessionID,
			Mode:      w.mode,
			BlockedBy: sortedUnique(j.blockersOf(w)),
		})
	}

	return infos
}

// blockersOf returns the transactions a waiting request is waiting for, possibly with repeats. Requires j.mu.
func (j *Journal) blockersOf(w *waiter) []TxnID {
	var blockers []TxnID
	j.eachBlocker(w.txn, w.entry, w.mode, w.seq, func(blocker *Txn) bool {
		blockers = append(blockers, blocker.id)
		return true
	})
	return blockers
}

func sortedUnique(ids []TxnID) []TxnID {
	if len(ids) == 0 {
		return nil
	}

	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	unique := ids[:1]
	for _, id := range ids[1:] {
		if id != unique[len(unique)-1] {
			unique = append(unique, id)
		}
	}

	return unique
}
