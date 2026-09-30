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
	"encoding/binary"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// XID is an 8-byte MySQL transaction identifier ([my_xid]) used by
// the transaction coordinator to identify prepared and committed
// transactions across the storage engine and binary log.
//
// [my_xid]: https://dev.mysql.com/doc/c-api/8.0/en/c-api-data-structures.html
type XID = uint64

// XIDFromUUIDv7 derives an XID from the time-ordered high 64 bits
// of |u|.
//
// As specified in [RFC 9562 §6.2], the high 64 bits encode a 48-bit
// millisecond timestamp, version bits, and a sequence counter,
// guaranteeing monotonic ordering and uniqueness.
//
// [RFC 9562 §6.2]: https://www.rfc-editor.org/rfc/rfc9562.html#section-6.2
func XIDFromUUIDv7(u uuid.UUID) XID {
	return binary.BigEndian.Uint64(u[0:8])
}

// NextXID generates a new UUIDv7 and returns its derived
// coordinator transaction identifier and full UUIDv7.
//
// It serves as the centralized transaction identifier generator for
// all operations (DML commits, table DDLs, and database
// creation/clone/drop), ensuring all subsystems share a single
// monotonic coordinator clock.
func NextXID() (XID, uuid.UUID, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return 0, uuid.Nil, err
	}
	return XIDFromUUIDv7(u), u, nil
}

var (
	xidCheckerMu sync.RWMutex
	xidChecker   func(filesys.Filesys, XID) bool
)

// RegisterXIDChecker registers a callback to query whether an XID
// is present in the coordinator log (e.g. binary log).
func RegisterXIDChecker(checker func(filesys.Filesys, XID) bool) {
	xidCheckerMu.Lock()
	defer xidCheckerMu.Unlock()
	xidChecker = checker
}

// HasXID reports whether coordinator transaction |xid| is present
// in the coordinator log under |fs|.
func HasXID(fs filesys.Filesys, xid XID) bool {
	xidCheckerMu.RLock()
	checker := xidChecker
	xidCheckerMu.RUnlock()
	if checker == nil {
		return false
	}
	return checker(fs, xid)
}

// ResetXIDCheckerForTesting clears the registered XID checker.
// It is intended solely for test teardown.
func ResetXIDCheckerForTesting() {
	xidCheckerMu.Lock()
	defer xidCheckerMu.Unlock()
	xidChecker = nil
}

// NotifyDatabaseCreated notifies registered listeners that database
// |name| was created with coordinator transaction |xid| under |ctx|.
func NotifyDatabaseCreated(ctx *sql.Context, name string, xid XID) error {
	for _, l := range doltdb.DatabaseUpdateListeners {
		if err := l.DatabaseCreated(ctx, name, xid); err != nil {
			return err
		}
	}
	return nil
}

// ResetDatabaseUpdateListenersForTesting clears registered listeners.
// It is intended solely for test teardown.
func ResetDatabaseUpdateListenersForTesting() {
	doltdb.DatabaseUpdateListeners = make([]doltdb.DatabaseUpdateListener, 0)
}
