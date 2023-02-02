// Copyright 2022 Dolthub, Inc.
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

package branch_control

import (
	"fmt"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
)

//TODO: add stored procedure functions for modifying the binlog

// Binlog is a running log file that tracks changes to tables within branch control. This is used for history purposes,
// as well as transactional purposes through the use of the BinlogOverlay.
type Binlog struct {
	rows    []BinlogRow
	RWMutex *sync.RWMutex
}

// BinlogRow is a row within the Binlog.
type BinlogRow struct {
	IsInsert    bool
	Database    string
	Branch      string
	User        string
	Host        string
	Permissions uint64
}

// BinlogOverlay enables transactional use cases over Binlog. Unlike a Binlog, a BinlogOverlay requires external
// synchronization.
type BinlogOverlay struct {
	parentLength int
	rows         []BinlogRow
}

// NewAccessBinlog returns a new Binlog that represents the construction of the given Access values. May be used to
// truncate the Binlog's history.
func NewAccessBinlog(vals []AccessRow) *Binlog {
	rows := make([]BinlogRow, len(vals))
	for i, val := range vals {
		rows[i] = BinlogRow{
			IsInsert:    true,
			Database:    val.Database,
			Branch:      val.Branch,
			User:        val.User,
			Host:        val.Host,
			Permissions: uint64(val.Permissions),
		}
	}
	return &Binlog{
		rows:    rows,
		RWMutex: &sync.RWMutex{},
	}
}

// NewNamespaceBinlog returns a new Binlog that represents the construction of the given Namespace values. May be used
// to truncate the Binlog's history.
func NewNamespaceBinlog(vals []NamespaceValue) *Binlog {
	rows := make([]BinlogRow, len(vals))
	for i, val := range vals {
		rows[i] = BinlogRow{
			IsInsert:    true,
			Database:    val.Database,
			Branch:      val.Branch,
			User:        val.User,
			Host:        val.Host,
			Permissions: 0,
		}
	}
	return &Binlog{
		rows:    rows,
		RWMutex: &sync.RWMutex{},
	}
}

// Serialize returns the offset for the Binlog written to the given builder.
func (binlog *Binlog) Serialize(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	binlog.RWMutex.RLock()
	defer binlog.RWMutex.RUnlock()

	// Initialize row offset slice
	rowOffsets := make([]flatbuffers.UOffsetT, len(binlog.rows))
	// Get each row's offset
	for i, row := range binlog.rows {
		rowOffsets[i] = row.Serialize(b)
	}
	// Get the row vector
	serial.BranchControlBinlogStartRowsVector(b, len(binlog.rows))
	for i := len(rowOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(rowOffsets[i])
	}
	rows := b.EndVector(len(binlog.rows))
	// Write the binlog
	serial.BranchControlBinlogStart(b)
	serial.BranchControlBinlogAddRows(b, rows)
	return serial.BranchControlBinlogEnd(b)
}

// Deserialize populates the binlog with the data from the flatbuffers representation.
func (binlog *Binlog) Deserialize(fb *serial.BranchControlBinlog) error {
	binlog.RWMutex.Lock()
	defer binlog.RWMutex.Unlock()

	// Verify that the binlog is empty
	if len(binlog.rows) != 0 {
		return fmt.Errorf("cannot deserialize to a non-empty binlog")
	}
	// Initialize the rows
	binlog.rows = make([]BinlogRow, fb.RowsLength())
	// Read the rows
	for i := 0; i < fb.RowsLength(); i++ {
		serialBinlogRow := &serial.BranchControlBinlogRow{}
		fb.Rows(serialBinlogRow, i)
		binlog.rows[i] = BinlogRow{
			IsInsert:    serialBinlogRow.IsInsert(),
			Database:    string(serialBinlogRow.Database()),
			Branch:      string(serialBinlogRow.Branch()),
			User:        string(serialBinlogRow.User()),
			Host:        string(serialBinlogRow.Host()),
			Permissions: serialBinlogRow.Permissions(),
		}
	}
	return nil
}

// NewOverlay returns a new BinlogOverlay for the calling Binlog.
func (binlog *Binlog) NewOverlay() *BinlogOverlay {
	binlog.RWMutex.RLock()
	defer binlog.RWMutex.RUnlock()

	return &BinlogOverlay{
		parentLength: len(binlog.rows),
		rows:         nil,
	}
}

// MergeOverlay merges the given BinlogOverlay with the calling Binlog. Fails if the Binlog has been written to since
// the overlay was created.
func (binlog *Binlog) MergeOverlay(overlay *BinlogOverlay) error {
	binlog.RWMutex.Lock()
	defer binlog.RWMutex.Unlock()

	// Except for recovery situations, the binlog is an append-only structure, therefore if there are a different number
	// of entries than when the overlay was created, then it has probably been written to. The likelihood of there being
	// an outstanding overlay while the binlog is being modified is exceedingly low.
	if len(binlog.rows) != overlay.parentLength {
		return fmt.Errorf("cannot merge overlay as binlog has been modified")
	}
	binlog.rows = append(binlog.rows, overlay.rows...)
	return nil
}

// Insert adds an insert entry to the Binlog.
func (binlog *Binlog) Insert(database string, branch string, user string, host string, permissions uint64) {
	binlog.RWMutex.Lock()
	defer binlog.RWMutex.Unlock()

	binlog.rows = append(binlog.rows, BinlogRow{
		IsInsert:    true,
		Database:    database,
		Branch:      branch,
		User:        user,
		Host:        host,
		Permissions: permissions,
	})
}

// Delete adds a delete entry to the Binlog.
func (binlog *Binlog) Delete(database string, branch string, user string, host string, permissions uint64) {
	binlog.RWMutex.Lock()
	defer binlog.RWMutex.Unlock()

	binlog.rows = append(binlog.rows, BinlogRow{
		IsInsert:    false,
		Database:    database,
		Branch:      branch,
		User:        user,
		Host:        host,
		Permissions: permissions,
	})
}

// Rows returns the underlying rows.
func (binlog *Binlog) Rows() []BinlogRow {
	return binlog.rows
}

// Serialize returns the offset for the BinlogRow written to the given builder.
func (row *BinlogRow) Serialize(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	database := b.CreateSharedString(row.Database)
	branch := b.CreateSharedString(row.Branch)
	user := b.CreateSharedString(row.User)
	host := b.CreateSharedString(row.Host)

	serial.BranchControlBinlogRowStart(b)
	serial.BranchControlBinlogRowAddIsInsert(b, row.IsInsert)
	serial.BranchControlBinlogRowAddDatabase(b, database)
	serial.BranchControlBinlogRowAddBranch(b, branch)
	serial.BranchControlBinlogRowAddUser(b, user)
	serial.BranchControlBinlogRowAddHost(b, host)
	serial.BranchControlBinlogRowAddPermissions(b, row.Permissions)
	return serial.BranchControlBinlogRowEnd(b)
}

// Insert adds an insert entry to the BinlogOverlay.
func (overlay *BinlogOverlay) Insert(database string, branch string, user string, host string, permissions uint64) {
	overlay.rows = append(overlay.rows, BinlogRow{
		IsInsert:    true,
		Database:    database,
		Branch:      branch,
		User:        user,
		Host:        host,
		Permissions: permissions,
	})
}

// Delete adds a delete entry to the BinlogOverlay.
func (overlay *BinlogOverlay) Delete(database string, branch string, user string, host string, permissions uint64) {
	overlay.rows = append(overlay.rows, BinlogRow{
		IsInsert:    false,
		Database:    database,
		Branch:      branch,
		User:        user,
		Host:        host,
		Permissions: permissions,
	})
}
