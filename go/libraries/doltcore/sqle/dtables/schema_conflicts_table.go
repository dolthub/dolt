// Copyright 2020 Dolthub, Inc.
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
	"errors"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	noms "github.com/dolthub/dolt/go/store/types"
)

var _ sql.Table = (*SchemaConflictsTable)(nil)

// SchemaConflictsTable is a sql.Table implementation that implements a system table which shows the current conflicts
type SchemaConflictsTable struct {
	dbName string
	ddb    *doltdb.DoltDB
}

// NewSchemaConflictsTable creates a SchemaConflictsTable
func NewSchemaConflictsTable(_ *sql.Context, dbName string, ddb *doltdb.DoltDB) sql.Table {
	return &SchemaConflictsTable{dbName: dbName, ddb: ddb}
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// SchemaConflictsTableName
func (dt *SchemaConflictsTable) Name() string {
	return doltdb.SchemaConflictsTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// SchemaConflictsTableName
func (dt *SchemaConflictsTable) String() string {
	return doltdb.SchemaConflictsTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (dt *SchemaConflictsTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "table_name", Type: types.Text, Source: doltdb.SchemaConflictsTableName, PrimaryKey: true, DatabaseSource: dt.dbName},
		{Name: "base_schema", Type: types.Text, Source: doltdb.SchemaConflictsTableName, PrimaryKey: false, DatabaseSource: dt.dbName},
		{Name: "our_schema", Type: types.Text, Source: doltdb.SchemaConflictsTableName, PrimaryKey: false, DatabaseSource: dt.dbName},
		{Name: "their_schema", Type: types.Text, Source: doltdb.SchemaConflictsTableName, PrimaryKey: false, DatabaseSource: dt.dbName},
		{Name: "description", Type: types.Text, Source: doltdb.SchemaConflictsTableName, PrimaryKey: false, DatabaseSource: dt.dbName},
	}
}

// Collation implements the sql.Table interface.
func (dt *SchemaConflictsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Conflict data for all tables exists in a single partition.
func (dt *SchemaConflictsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	ws, err := sess.WorkingSet(ctx, dt.dbName)
	if err != nil {
		return nil, err
	}
	dbd, _ := sess.GetDbData(ctx, dt.dbName)

	if ws.MergeState() == nil || !ws.MergeState().HasSchemaConflicts() {
		return sql.PartitionsToPartitionIter(), nil
	}

	head, err := sess.GetHeadCommit(ctx, dt.dbName)
	if err != nil {
		return nil, err
	}

	return sql.PartitionsToPartitionIter(schemaConflictsPartition{
		state: ws.MergeState(),
		head:  head,
		ddb:   dbd.Ddb,
	}), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (dt *SchemaConflictsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p, ok := part.(schemaConflictsPartition)
	if !ok {
		return nil, errors.New("unexpected partition for schema conflicts table")
	}

	optCmt, err := doltdb.GetCommitAncestor(ctx, p.head, p.state.Commit())
	if err != nil {
		return nil, err
	}
	base, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	baseRoot, err := base.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	var conflicts []schemaConflict
	err = p.state.IterSchemaConflicts(ctx, p.ddb, func(table string, cnf doltdb.SchemaConflict) error {
		// TODO: schema name
		c, err := newSchemaConflict(ctx, doltdb.TableName{Name: table}, baseRoot, cnf)
		if err != nil {
			return err
		}
		conflicts = append(conflicts, c)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &schemaConflictsIter{
		conflicts: conflicts,
	}, nil
}

type schemaConflictsPartition struct {
	state *doltdb.MergeState
	head  *doltdb.Commit
	ddb   *doltdb.DoltDB
}

func (p schemaConflictsPartition) Key() []byte {
	return []byte(doltdb.SchemaConflictsTableName)
}

type schemaConflict struct {
	table       doltdb.TableName
	baseSch     string
	ourSch      string
	theirSch    string
	description string
}

func newSchemaConflict(ctx context.Context, table doltdb.TableName, baseRoot doltdb.RootValue, c doltdb.SchemaConflict) (schemaConflict, error) {
	bs, err := doltdb.GetAllSchemas(ctx, baseRoot)
	if err != nil {
		return schemaConflict{}, err
	}
	baseSch := bs[table]

	fkc, err := baseRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return schemaConflict{}, err
	}

	baseFKs, _ := fkc.KeysForTable(table)

	var base string
	if baseSch != nil {
		var err error
		base, err = getCreateTableStatement(table.Name, baseSch, baseFKs, bs)
		if err != nil {
			return schemaConflict{}, err
		}
	} else {
		base = "<deleted>"
	}

	var ours string
	if c.ToSch != nil {
		var err error
		ours, err = getCreateTableStatement(table.Name, c.ToSch, c.ToFks, c.ToParentSchemas)
		if err != nil {
			return schemaConflict{}, err
		}
	} else {
		ours = "<deleted>"
	}

	var theirs string
	if c.FromSch != nil {
		var err error
		theirs, err = getCreateTableStatement(table.Name, c.FromSch, c.FromFks, c.FromParentSchemas)
		if err != nil {
			return schemaConflict{}, err
		}
	} else {
		theirs = "<deleted>"
	}

	if c.ToSch == nil || c.FromSch == nil {
		return schemaConflict{
			table:       table,
			baseSch:     base,
			ourSch:      ours,
			theirSch:    theirs,
			description: "cannot merge a table deletion with schema modification",
		}, nil
	}

	desc, err := getSchemaConflictDescription(ctx, table, baseSch, c.ToSch, c.FromSch)
	if err != nil {
		return schemaConflict{}, err
	}

	return schemaConflict{
		table:       table,
		baseSch:     base,
		ourSch:      ours,
		theirSch:    theirs,
		description: desc,
	}, nil
}

func getCreateTableStatement(table string, sch schema.Schema, fks []doltdb.ForeignKey, parents map[doltdb.TableName]schema.Schema) (string, error) {
	return sqlfmt.GenerateCreateTableStatement(table, sch, fks, parents)
}

func getSchemaConflictDescription(ctx context.Context, table doltdb.TableName, base, ours, theirs schema.Schema) (string, error) {
	_, conflict, _, _, err := merge.SchemaMerge(ctx, noms.Format_Default, ours, theirs, base, table)
	if err != nil {
		return "", err
	}
	return conflict.String(), nil
}

type schemaConflictsIter struct {
	conflicts   []schemaConflict
	baseSchemas map[string]schema.Schema
	baseCommit  *doltdb.Commit
}

func (it *schemaConflictsIter) Next(ctx *sql.Context) (sql.Row, error) {
	if len(it.conflicts) == 0 {
		return nil, io.EOF
	}
	c := it.conflicts[0] // pop next conflict
	it.conflicts = it.conflicts[1:]
	return sql.NewRow(c.table, c.baseSch, c.ourSch, c.theirSch, c.description), nil
}

func (it *schemaConflictsIter) Close(ctx *sql.Context) error {
	it.conflicts = nil
	return nil
}
