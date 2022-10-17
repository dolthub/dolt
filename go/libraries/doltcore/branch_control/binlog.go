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
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

const (
	currentBinlogVersion = uint16(1)
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
func NewAccessBinlog(vals []AccessValue) *Binlog {
	rows := make([]BinlogRow, len(vals))
	for i, val := range vals {
		rows[i] = BinlogRow{
			IsInsert:    true,
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

// Serialize returns the Binlog as a byte slice. Writes to the given buffer if one is provided, else allocates a
// temporary buffer. All encoded integers are big-endian.
func (binlog *Binlog) Serialize(buffer *bytes.Buffer) []byte {
	binlog.RWMutex.RLock()
	defer binlog.RWMutex.RUnlock()

	if buffer == nil {
		buffer = &bytes.Buffer{}
	}
	// Write the version bytes
	writeUint16(buffer, currentBinlogVersion)
	// Write the number of entries
	binlogSize := uint64(len(binlog.rows))
	writeUint64(buffer, binlogSize)

	// Write the rows
	for _, binlogRow := range binlog.rows {
		binlogRow.Serialize(buffer)
	}
	return buffer.Bytes()
}

// Deserialize populates the binlog with the given data. Returns an error if the data cannot be deserialized, or if the
// Binlog has already been written to. Deserialize must be called on an empty Binlog.
func (binlog *Binlog) Deserialize(data []byte, position *uint64) error {
	binlog.RWMutex.Lock()
	defer binlog.RWMutex.Unlock()

	if len(binlog.rows) != 0 {
		return fmt.Errorf("cannot deserialize to a non-empty binlog")
	}
	// Read the version
	version := binary.BigEndian.Uint16(data[*position:])
	*position += 2
	if version != currentBinlogVersion {
		// If we ever increment the binlog version, this will instead handle the conversion from previous versions
		return fmt.Errorf(`cannot deserialize a binlog with version "%d"`, version)
	}
	// Read the number of entries
	binlogSize := binary.BigEndian.Uint64(data[*position:])
	*position += 8
	// Read the rows
	binlog.rows = make([]BinlogRow, binlogSize)
	for i := uint64(0); i < binlogSize; i++ {
		binlog.rows[i] = deserializeBinlogRow(data, position)
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
func (binlog *Binlog) Insert(branch string, user string, host string, permissions uint64) {
	binlog.RWMutex.Lock()
	defer binlog.RWMutex.Unlock()

	binlog.rows = append(binlog.rows, BinlogRow{
		IsInsert:    true,
		Branch:      branch,
		User:        user,
		Host:        host,
		Permissions: permissions,
	})
}

// Delete adds a delete entry to the Binlog.
func (binlog *Binlog) Delete(branch string, user string, host string, permissions uint64) {
	binlog.RWMutex.Lock()
	defer binlog.RWMutex.Unlock()

	binlog.rows = append(binlog.rows, BinlogRow{
		IsInsert:    false,
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

// Serialize writes the row to the given buffer. All encoded integers are big-endian.
func (row *BinlogRow) Serialize(buffer *bytes.Buffer) {
	// Write whether this was an insertion or deletion
	if row.IsInsert {
		buffer.WriteByte(1)
	} else {
		buffer.WriteByte(0)
	}
	// Write the branch
	branchLen := uint16(len(row.Branch))
	writeUint16(buffer, branchLen)
	buffer.WriteString(row.Branch)
	// Write the user
	userLen := uint16(len(row.User))
	writeUint16(buffer, userLen)
	buffer.WriteString(row.User)
	// Write the host
	hostLen := uint16(len(row.Host))
	writeUint16(buffer, hostLen)
	buffer.WriteString(row.Host)
	// Write the permissions
	writeUint64(buffer, row.Permissions)
}

// deserializeBinlogRow returns a BinlogRow from the data at the given position. Assumes that the given data's encoded
// integers are big-endian.
func deserializeBinlogRow(data []byte, position *uint64) BinlogRow {
	binlogRow := BinlogRow{}
	// Read whether this was an insert or write
	if data[*position] == 1 {
		binlogRow.IsInsert = true
	} else {
		binlogRow.IsInsert = false
	}
	*position += 1
	// Read the branch
	branchLen := uint64(binary.BigEndian.Uint16(data[*position:]))
	*position += 2
	binlogRow.Branch = string(data[*position : *position+branchLen])
	*position += branchLen
	// Read the user
	userLen := uint64(binary.BigEndian.Uint16(data[*position:]))
	*position += 2
	binlogRow.User = string(data[*position : *position+userLen])
	*position += userLen
	// Read the host
	hostLen := uint64(binary.BigEndian.Uint16(data[*position:]))
	*position += 2
	binlogRow.Host = string(data[*position : *position+hostLen])
	*position += hostLen
	// Read the permissions
	binlogRow.Permissions = binary.BigEndian.Uint64(data[*position:])
	*position += 8
	return binlogRow
}

// Insert adds an insert entry to the BinlogOverlay.
func (overlay *BinlogOverlay) Insert(branch string, user string, host string, permissions uint64) {
	overlay.rows = append(overlay.rows, BinlogRow{
		IsInsert:    true,
		Branch:      branch,
		User:        user,
		Host:        host,
		Permissions: permissions,
	})
}

// Delete adds a delete entry to the BinlogOverlay.
func (overlay *BinlogOverlay) Delete(branch string, user string, host string, permissions uint64) {
	overlay.rows = append(overlay.rows, BinlogRow{
		IsInsert:    false,
		Branch:      branch,
		User:        user,
		Host:        host,
		Permissions: permissions,
	})
}

// writeUint64 writes an uint64 into the buffer.
func writeUint64(buffer *bytes.Buffer, val uint64) {
	buffer.WriteByte(byte(val >> 56))
	buffer.WriteByte(byte(val >> 48))
	buffer.WriteByte(byte(val >> 40))
	buffer.WriteByte(byte(val >> 32))
	buffer.WriteByte(byte(val >> 24))
	buffer.WriteByte(byte(val >> 16))
	buffer.WriteByte(byte(val >> 8))
	buffer.WriteByte(byte(val))
}

// writeUint32 writes an uint32 into the buffer.
func writeUint32(buffer *bytes.Buffer, val uint32) {
	buffer.WriteByte(byte(val >> 24))
	buffer.WriteByte(byte(val >> 16))
	buffer.WriteByte(byte(val >> 8))
	buffer.WriteByte(byte(val))
}

// writeUint16 writes an uint16 into the buffer.
func writeUint16(buffer *bytes.Buffer, val uint16) {
	buffer.WriteByte(byte(val >> 8))
	buffer.WriteByte(byte(val))
}
