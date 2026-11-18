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

// LockMode is the strength of a lock on a row or a table. Modes are ordered from weakest to strongest, and a stronger
// mode conflicts with everything a weaker mode conflicts with.
type LockMode uint8

const (
	// LockNone is the absence of a lock.
	LockNone LockMode = iota
	// LockKeyShare prevents the row from being deleted or having its key changed. Taken by foreign key checks on the
	// referenced row, and by SELECT ... FOR KEY SHARE.
	LockKeyShare
	// LockShare prevents the row from being modified at all. Taken by SELECT ... FOR SHARE.
	LockShare
	// LockNoKeyUpdate is taken by an UPDATE that does not change key columns, and by SELECT ... FOR NO KEY UPDATE. It
	// permits concurrent LockKeyShare lockers.
	LockNoKeyUpdate
	// LockUpdate is taken by a DELETE, by an UPDATE that changes key columns, and by SELECT ... FOR UPDATE. It
	// conflicts with every other mode.
	LockUpdate

	numLockModes = iota
)

// lockConflicts is indexed by [held][requested].
var lockConflicts = [numLockModes][numLockModes]bool{
	LockKeyShare:    {LockUpdate: true},
	LockShare:       {LockNoKeyUpdate: true, LockUpdate: true},
	LockNoKeyUpdate: {LockShare: true, LockNoKeyUpdate: true, LockUpdate: true},
	LockUpdate:      {LockKeyShare: true, LockShare: true, LockNoKeyUpdate: true, LockUpdate: true},
}

// ConflictsWith returns whether a lock of this mode held by one transaction prevents another transaction from taking
// a lock of mode |other| on the same target.
func (m LockMode) ConflictsWith(other LockMode) bool {
	return lockConflicts[m][other]
}

// Covers returns whether holding a lock of this mode makes a request for |other| by the same transaction redundant.
func (m LockMode) Covers(other LockMode) bool {
	return m >= other
}

// String implements fmt.Stringer.
func (m LockMode) String() string {
	switch m {
	case LockNone:
		return "None"
	case LockKeyShare:
		return "KeyShare"
	case LockShare:
		return "Share"
	case LockNoKeyUpdate:
		return "NoKeyUpdate"
	case LockUpdate:
		return "Update"
	default:
		return "Unknown"
	}
}

// WaitPolicy says what a lock request does when it cannot be granted immediately.
type WaitPolicy uint8

const (
	// WaitBlock waits until the lock can be granted, the context is canceled, the lock timeout expires, or a deadlock
	// is detected.
	WaitBlock WaitPolicy = iota
	// WaitSkip returns LockWouldBlock instead of waiting.
	WaitSkip
	// WaitError returns ErrLockNotAvailable instead of waiting.
	WaitError
)

// LockResult is the outcome of a request to lock or write a row.
type LockResult uint8

const (
	// LockGranted means the transaction now holds the requested lock, and that no transaction invisible to its
	// snapshot has committed a conflicting change to the target.
	LockGranted LockResult = iota
	// LockRowUpdated means a transaction that is not visible to the requester's snapshot committed an update (or an
	// insert) of the target. No lock was taken.
	LockRowUpdated
	// LockRowDeleted means a transaction that is not visible to the requester's snapshot committed a delete of the
	// target. No lock was taken.
	LockRowDeleted
	// LockWouldBlock means the request used WaitSkip and the lock could not be granted without waiting. No lock was
	// taken.
	LockWouldBlock
	// LockKeyExists means a transaction that is not visible to the requester's snapshot committed a row with the key
	// that the requester is trying to insert. Nothing was recorded.
	LockKeyExists
)

// String implements fmt.Stringer.
func (r LockResult) String() string {
	switch r {
	case LockGranted:
		return "Granted"
	case LockRowUpdated:
		return "RowUpdated"
	case LockRowDeleted:
		return "RowDeleted"
	case LockWouldBlock:
		return "WouldBlock"
	case LockKeyExists:
		return "KeyExists"
	default:
		return "Unknown"
	}
}
