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

package sqle

import (
	"context"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// databaseLeases tracks the dsess.DatabaseLease instances held on each database, keyed by the database's map key.
// It is shared by every copy of a DoltDatabaseProvider.
type databaseLeases struct {
	mu   sync.Mutex
	sets map[string]*databaseLeaseSet
}

// databaseLeaseSet is the set of leases held on a single database.
type databaseLeaseSet struct {
	// holders counts the leases each session holds on the database, keyed by dsess.DoltSession.DatabaseLeaseHolder
	holders map[any]int
	// released is closed and replaced each time a lease in this set is released
	released chan struct{}
}

// databaseLease is a lease held by one session on one database.
type databaseLease struct {
	leases *databaseLeases
	set    *databaseLeaseSet
	holder any
	once   sync.Once
}

var _ dsess.DatabaseLease = (*databaseLease)(nil)

func newDatabaseLeases() *databaseLeases {
	return &databaseLeases{
		sets: make(map[string]*databaseLeaseSet),
	}
}

// acquire returns a new lease on the database with map key |key| for |holder|.
func (l *databaseLeases) acquire(key string, holder any) *databaseLease {
	l.mu.Lock()
	defer l.mu.Unlock()

	set, ok := l.sets[key]
	if !ok {
		set = &databaseLeaseSet{
			holders:  make(map[any]int),
			released: make(chan struct{}),
		}
		l.sets[key] = set
	}

	set.holders[holder]++
	return &databaseLease{
		leases: l,
		set:    set,
		holder: holder,
	}
}

// Release implements dsess.DatabaseLease.
func (lease *databaseLease) Release() {
	lease.once.Do(func() {
		l := lease.leases
		l.mu.Lock()
		defer l.mu.Unlock()

		set := lease.set
		set.holders[lease.holder]--
		if set.holders[lease.holder] <= 0 {
			delete(set.holders, lease.holder)
		}

		close(set.released)
		set.released = make(chan struct{})
	})
}

// awaitRelease blocks until no session other than |holder| holds a lease on the database with map key |key|, or until
// |ctx| is done. Afterward, leases acquired on |key| belong to a new set, so that leases still held on the old
// database do not delay a drop of a database later created with the same name.
func (l *databaseLeases) awaitRelease(ctx context.Context, key string, holder any) error {
	for {
		l.mu.Lock()
		set, ok := l.sets[key]
		if !ok || !set.heldByOtherThan(holder) {
			if ok {
				delete(l.sets, key)
			}
			l.mu.Unlock()
			return nil
		}

		released := set.released
		l.mu.Unlock()

		select {
		case <-released:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// heldByOtherThan returns whether any session other than |holder| holds a lease in this set. Must be called with the
// databaseLeases mutex held.
func (s *databaseLeaseSet) heldByOtherThan(holder any) bool {
	for h := range s.holders {
		if h != holder {
			return true
		}
	}
	return false
}

// AcquireDatabaseLease implements dsess.DatabaseLeaseProvider.
func (p *DoltDatabaseProvider) AcquireDatabaseLease(ctx *sql.Context, baseName string, holder any) (dsess.DatabaseLease, dsess.DatabaseLeaseStatus) {
	key := formatDbMapKeyName(baseName)

	// Holding p.mu while acquiring the lease makes acquisition atomic with DropDatabase removing the database: either
	// the lease is acquired first, and the drop waits for it to be released, or the database is already gone.
	p.mu.RLock()
	defer p.mu.RUnlock()

	if _, ok := p.databases[key]; ok {
		return p.leases.acquire(key, holder), dsess.DatabaseLeased
	}

	_, draining := p.drainingDatabases[key]
	_, deleting := p.deletingDatabases[key]
	if draining || deleting {
		return nil, dsess.DatabaseDropped
	}

	return nil, dsess.DatabaseNotLeasable
}
