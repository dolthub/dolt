// Copyright 2025 Dolthub, Inc.
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

package dtables

import (
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
)

var _ sql.Table = (*BranchActivityTable)(nil)

// BranchActivityTable is a read-only system table that tracks branch activity
type BranchActivityTable struct {
	db        dsess.SqlDatabase
	tableName string
}

func NewBranchActivityTable(_ *sql.Context, db dsess.SqlDatabase) sql.Table {
	return &BranchActivityTable{db: db, tableName: doltdb.BranchActivityTableName}
}

func (bat *BranchActivityTable) Name() string {
	return bat.tableName
}

func (bat *BranchActivityTable) String() string {
	return bat.tableName
}

func (bat *BranchActivityTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "branch", Type: types.Text, Source: bat.tableName, PrimaryKey: true, Nullable: false, DatabaseSource: bat.db.Name()},
		{Name: "last_read", Type: types.Datetime, Source: bat.tableName, PrimaryKey: false, Nullable: true, DatabaseSource: bat.db.Name()},
		{Name: "last_write", Type: types.Datetime, Source: bat.tableName, PrimaryKey: false, Nullable: true, DatabaseSource: bat.db.Name()},
		{Name: "active_sessions", Type: types.Int32, Source: bat.tableName, PrimaryKey: false, Nullable: false, DatabaseSource: bat.db.Name()},
		{Name: "system_start_time", Type: types.Datetime, Source: bat.tableName, PrimaryKey: false, Nullable: false, DatabaseSource: bat.db.Name()},
	}
}

func (bat *BranchActivityTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (bat *BranchActivityTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (bat *BranchActivityTable) PartitionRows(sqlCtx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return NewBranchActivityItr(sqlCtx, bat)
}

type BranchActivityItr struct {
	table *BranchActivityTable
	idx   int
	rows  []sql.Row
}

func NewBranchActivityItr(ctx *sql.Context, table *BranchActivityTable) (*BranchActivityItr, error) {
	// Check if branch activity tracking is enabled
	if provider, ok := ctx.Session.(doltdb.BranchActivityProvider); ok {
		tracker := provider.GetBranchActivityTracker()
		if tracker == nil || !tracker.IsTrackingEnabled() {
			return nil, fmt.Errorf("branch activity tracking is not enabled; enable it in the server config with 'behavior.branch_activity_tracking: true'")
		}
	} else {
		return nil, fmt.Errorf("branch activity tracking is not enabled; enable it in the server config with 'behavior.branch_activity_tracking: true'")
	}

	sessionCounts, err := countActiveSessions(ctx)
	if err != nil {
		return nil, err
	}

	activityData, err := doltdb.GetBranchActivity(ctx, table.db.DbData().Ddb)
	if err != nil {
		return nil, err
	}

	rows := make([]sql.Row, 0, len(activityData))
	for _, act := range activityData {
		var lastRead, lastWrite interface{}

		if act.LastRead != nil {
			lastRead = *act.LastRead
		} else {
			lastRead = nil
		}

		if act.LastWrite != nil {
			lastWrite = *act.LastWrite
		} else {
			lastWrite = nil
		}

		row := sql.NewRow(act.Branch, lastRead, lastWrite, sessionCounts[act.Branch], act.SystemStartTime)
		rows = append(rows, row)
	}

	return &BranchActivityItr{
		table: table,
		idx:   0,
		rows:  rows,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
func (itr *BranchActivityItr) Next(ctx *sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.rows) {
		return nil, io.EOF
	}

	row := itr.rows[itr.idx]
	itr.idx++
	return row, nil
}

// Close closes the iterator.
func (itr *BranchActivityItr) Close(*sql.Context) error {
	return nil
}

// countActiveSessions counts active sessions per branch using SessionManager. Would be preferrable to stick this
// in doltdb/branch_activity.go but that would create a circular dependency.
func countActiveSessions(ctx *sql.Context) (map[string]int, error) {
	sessionCounts := make(map[string]int)

	if !sqlserver.RunningInServerMode() {
		return sessionCounts, nil
	}
	runningServer := sqlserver.GetRunningServer()
	if runningServer == nil {
		return sessionCounts, nil
	}

	// DB of the requester is used to filter out other databases.
	currentDbName := ctx.GetCurrentDatabase()
	baseDbName, _ := doltdb.SplitRevisionDbName(currentDbName)
	if baseDbName == "" {
		return sessionCounts, nil
	}

	sessionManager := runningServer.SessionManager()

	err := sessionManager.Iter(func(session sql.Session) (bool, error) {
		sess, ok := session.(*dsess.DoltSession)
		if !ok {
			// Don't think this should ever happen
			return false, fmt.Errorf("expected DoltSession, got %T", session)
		}

		sessionDbName := sess.Session.GetCurrentDatabase()
		baseName, revision := doltdb.SplitRevisionDbName(sessionDbName)
		if baseName != baseDbName {
			// Different database, skip
			return false, nil
		}
		if revision == "" {
			activeBranchRef, err := sess.CWBHeadRef(ctx, sessionDbName)
			if err != nil {
				return false, err
			}
			revision = activeBranchRef.GetPath()
		}

		sessionCounts[revision]++

		return false, nil
	})

	return sessionCounts, err
}
