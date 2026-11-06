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
	"github.com/dolthub/go-mysql-server/sql"
)

// A DatabaseLease keeps a database from being dropped while a session's command is using it. A session acquires a
// lease on a database the first time a command resolves session state for it, and releases its leases when the
// command ends. DROP DATABASE waits for other sessions' leases on the database to be released before it invalidates
// session state for the database and closes it.
type DatabaseLease interface {
	// Release releases the lease. Releasing a lease more than once has no effect.
	Release()
}

// DatabaseLeaseStatus is the result of an attempt to acquire a DatabaseLease.
type DatabaseLeaseStatus int

const (
	// DatabaseLeased means the lease was acquired.
	DatabaseLeased DatabaseLeaseStatus = iota
	// DatabaseNotLeasable means the provider does not manage the lifecycle of the database, so no lease is needed.
	DatabaseNotLeasable
	// DatabaseDropped means the database is being dropped, or has been dropped, and must be treated as not found.
	DatabaseDropped
)

// DatabaseLeaseProvider is implemented by providers that support leasing databases to sessions.
type DatabaseLeaseProvider interface {
	// AcquireDatabaseLease acquires a lease on the database named |baseName| for |holder|, which identifies the
	// session acquiring it (see DoltSession.DatabaseLeaseHolder). The returned lease is non-nil only when the status
	// is DatabaseLeased.
	AcquireDatabaseLease(ctx *sql.Context, baseName string, holder any) (DatabaseLease, DatabaseLeaseStatus)
}

// sessionLeases tracks the DatabaseLease instances held by a DoltSession's current command. It is guarded by the
// session's mutex.
type sessionLeases struct {
	// commandDepth is the number of commands the session has begun but not ended. Leases are only acquired while it
	// is positive, since they are released when the outermost command ends.
	commandDepth int
	// held maps a database's lower-case base name to the lease the current command holds on it
	held map[string]DatabaseLease
}

func newSessionLeases() *sessionLeases {
	return &sessionLeases{
		held: make(map[string]DatabaseLease),
	}
}

// needsLease returns whether the current command must acquire a lease on |baseName| before using it.
func (l *sessionLeases) needsLease(baseName string) bool {
	if l == nil || l.commandDepth == 0 {
		return false
	}

	_, ok := l.held[baseName]
	return !ok
}

// beginCommand records that the session began a command.
func (l *sessionLeases) beginCommand() {
	if l != nil {
		l.commandDepth++
	}
}

// endCommand records that the session ended a command, and returns the leases to release if it was the outermost
// command.
func (l *sessionLeases) endCommand() []DatabaseLease {
	if l == nil {
		return nil
	}

	l.commandDepth--
	if l.commandDepth > 0 {
		return nil
	}

	l.commandDepth = 0
	return l.takeAll()
}

// add records |lease| as the current command's lease on |baseName|. Returns false, without recording |lease|, if the
// command already holds a lease on |baseName| or if no command is running.
func (l *sessionLeases) add(baseName string, lease DatabaseLease) bool {
	if !l.needsLease(baseName) {
		return false
	}

	l.held[baseName] = lease
	return true
}

// takeAll removes and returns every lease held by the current command.
func (l *sessionLeases) takeAll() []DatabaseLease {
	if l == nil || len(l.held) == 0 {
		return nil
	}

	leases := make([]DatabaseLease, 0, len(l.held))
	for _, lease := range l.held {
		leases = append(leases, lease)
	}
	clear(l.held)
	return leases
}

// releaseAll releases every lease in |leases|.
func releaseAll(leases []DatabaseLease) {
	for _, lease := range leases {
		lease.Release()
	}
}
