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

// TxnID identifies a transaction. IDs are handed out in ascending order for the life of the server process and are
// never reused. The zero value is not a valid transaction.
type TxnID uint64

// TxnStatus is the state of a transaction.
type TxnStatus uint8

const (
	// TxnInProgress means the transaction has neither committed nor aborted.
	TxnInProgress TxnStatus = iota
	// TxnCommitted means the transaction's effects are durable.
	TxnCommitted
	// TxnAborted means the transaction's effects were discarded.
	TxnAborted
)

// String implements fmt.Stringer.
func (s TxnStatus) String() string {
	switch s {
	case TxnInProgress:
		return "InProgress"
	case TxnCommitted:
		return "Committed"
	case TxnAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

// Snapshot describes which transactions' effects a transaction can see: those that had committed when the snapshot
// was taken.
type Snapshot struct {
	// OldestRunning is the lowest ID among the transactions that were in progress when the snapshot was taken. Every
	// transaction with a lower ID had finished.
	OldestRunning TxnID
	// NextTxn is the first ID that had not been handed out when the snapshot was taken. No transaction with this ID
	// or a higher one is visible.
	NextTxn TxnID
	// Running is the IDs of the transactions that were in progress when the snapshot was taken, in ascending order.
	Running []TxnID
}

// Includes returns whether the transaction |id|, assuming it committed, did so before this snapshot was taken.
func (s Snapshot) Includes(id TxnID) bool {
	if id >= s.NextTxn {
		return false
	}

	if id < s.OldestRunning {
		return true
	}

	i := sort.Search(len(s.Running), func(i int) bool { return s.Running[i] >= id })
	return i == len(s.Running) || s.Running[i] != id
}

// Txn is a transaction registered with a Journal. Its exported methods are safe for concurrent use.
type Txn struct {
	journal   *Journal
	id        TxnID
	sessionID uint32
	startTime time.Time
	snapshot  Snapshot

	// The fields below are guarded by journal.mu.

	status TxnStatus
	// level is the current savepoint level. Level 0 is the transaction itself, and each open savepoint adds one.
	level uint32
	// touched is the set of entries that hold, or once held, a locker or a write of this transaction.
	touched map[*entry]struct{}
	// waits is the set of lock requests of this transaction that are currently waiting.
	waits map[*waiter]struct{}
	// wrote is whether this transaction recorded any write.
	wrote bool
}

// ID returns the transaction's ID.
func (t *Txn) ID() TxnID {
	return t.id
}

// SessionID returns the ID of the session that started the transaction.
func (t *Txn) SessionID() uint32 {
	return t.sessionID
}

// StartTime returns when the transaction began.
func (t *Txn) StartTime() time.Time {
	return t.startTime
}

// Snapshot returns the snapshot taken when the transaction began.
func (t *Txn) Snapshot() Snapshot {
	return t.snapshot
}

// Status returns the transaction's current status.
func (t *Txn) Status() TxnStatus {
	t.journal.mu.Lock()
	defer t.journal.mu.Unlock()
	return t.status
}

// sees returns whether the effects of |other| are visible to this transaction.
func (t *Txn) sees(other *Txn) bool {
	if other == t {
		return true
	}

	return other.status == TxnCommitted && t.snapshot.Includes(other.id)
}

// isConcurrentCommit returns whether |other| is a different transaction that has committed, but whose effects this
// transaction cannot see.
func (t *Txn) isConcurrentCommit(other *Txn) bool {
	return other != t && other.status == TxnCommitted && !t.snapshot.Includes(other.id)
}

// txnHeap is a min-heap of transactions ordered by ID.
type txnHeap []*Txn

func (h txnHeap) Len() int           { return len(h) }
func (h txnHeap) Less(i, j int) bool { return h[i].id < h[j].id }
func (h txnHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *txnHeap) Push(x any)        { *h = append(*h, x.(*Txn)) }

func (h *txnHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return t
}
