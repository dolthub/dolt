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

package dtables

import (
	"context"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

// MergeStatusTable is a sql.Table implementation that implements a system table
// which shows information about an active merge.
type MergeStatusTable struct {
	dbName string
}

func (s MergeStatusTable) Name() string {
	return doltdb.MergeStatusTableName
}

func (s MergeStatusTable) String() string {
	return doltdb.MergeStatusTableName
}

func (s MergeStatusTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "is_merging", Type: types.Boolean, Source: doltdb.MergeStatusTableName, PrimaryKey: false, Nullable: false, DatabaseSource: s.dbName},
		{Name: "source", Type: types.Text, Source: doltdb.MergeStatusTableName, PrimaryKey: false, Nullable: true, DatabaseSource: s.dbName},
		{Name: "source_commit", Type: types.Text, Source: doltdb.MergeStatusTableName, PrimaryKey: false, Nullable: true, DatabaseSource: s.dbName},
		{Name: "target", Type: types.Text, Source: doltdb.MergeStatusTableName, PrimaryKey: false, Nullable: true, DatabaseSource: s.dbName},
		{Name: "unmerged_tables", Type: types.Text, Source: doltdb.MergeStatusTableName, PrimaryKey: false, Nullable: true, DatabaseSource: s.dbName},
	}
}

func (s MergeStatusTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (s MergeStatusTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (s MergeStatusTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	sesh := dsess.DSessFromSess(ctx.Session)
	ws, err := sesh.WorkingSet(ctx, s.dbName)
	if err != nil {
		return nil, err
	}

	return newMergeStatusItr(ctx, ws)
}

// NewMergeStatusTable creates a StatusTable
func NewMergeStatusTable(dbName string) sql.Table {
	return &MergeStatusTable{dbName}
}

// MergeStatusIter is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type MergeStatusIter struct {
	idx            int
	isMerging      bool
	sourceCommit   *string
	source         *string
	target         *string
	unmergedTables *string
}

func newMergeStatusItr(ctx context.Context, ws *doltdb.WorkingSet) (*MergeStatusIter, error) {
	wr := ws.WorkingRoot()

	inConflict, err := doltdb.TablesWithDataConflicts(ctx, wr)
	if err != nil {
		return nil, err
	}

	tblsWithViolations, err := doltdb.TablesWithConstraintViolations(ctx, wr)
	if err != nil {
		return nil, err
	}

	var schConflicts []string
	if ws.MergeActive() {
		schConflicts = ws.MergeState().TablesWithSchemaConflicts()
	}

	unmergedTblNames := set.NewStrSet(inConflict)
	unmergedTblNames.Add(doltdb.FlattenTableNames(tblsWithViolations)...)
	unmergedTblNames.Add(schConflicts...)

	var sourceCommitSpecStr *string
	var sourceCommitHash *string
	var target *string
	var unmergedTables *string
	if ws.MergeActive() {
		state := ws.MergeState()

		s := state.CommitSpecStr()
		sourceCommitSpecStr = &s

		cmHash, err := state.Commit().HashOf()
		if err != nil {
			return nil, err
		}
		s2 := cmHash.String()
		sourceCommitHash = &s2

		curr, err := ws.Ref().ToHeadRef()
		if err != nil {
			return nil, err
		}
		s3 := curr.String()
		target = &s3

		s4 := strings.Join(unmergedTblNames.AsSlice(), ", ")
		unmergedTables = &s4
	}

	return &MergeStatusIter{
		idx:            0,
		isMerging:      ws.MergeActive(),
		source:         sourceCommitSpecStr,
		sourceCommit:   sourceCommitHash,
		target:         target,
		unmergedTables: unmergedTables,
	}, nil
}

// Next retrieves the next row.
func (itr *MergeStatusIter) Next(*sql.Context) (sql.Row, error) {
	if itr.idx >= 1 {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	return sql.NewRow(itr.isMerging, unwrapString(itr.source), unwrapString(itr.sourceCommit), unwrapString(itr.target), unwrapString(itr.unmergedTables)), nil
}

func unwrapString(s *string) interface{} {
	if s == nil {
		return nil
	}
	return *s
}

// Close closes the iterator.
func (itr *MergeStatusIter) Close(*sql.Context) error {
	return nil
}
