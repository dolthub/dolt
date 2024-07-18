// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// ProceduresTableName is the name of the dolt stored procedures table.
	ProceduresTableName = "dolt_procedures"
	// ProceduresTableNameCol is the name of the stored procedure. Using CREATE PROCEDURE, will always be lowercase.
	ProceduresTableNameCol = "name"
	// ProceduresTableCreateStmtCol is the CREATE PROCEDURE statement for this stored procedure.
	ProceduresTableCreateStmtCol = "create_stmt"
	// ProceduresTableCreatedAtCol is the time that the stored procedure was created at, in UTC.
	ProceduresTableCreatedAtCol = "created_at"
	// ProceduresTableModifiedAtCol is the time that the stored procedure was last modified, in UTC.
	ProceduresTableModifiedAtCol = "modified_at"
)

type ProceduresTable struct {
	backingTable *WritableDoltTable
}

func (pt *ProceduresTable) Name() string {
	return ProceduresTableName
}

func (pt *ProceduresTable) String() string {
	return ProceduresTableName
}

func (pt *ProceduresTable) Schema() sql.Schema {
	return ProceduresTableSqlSchema().Schema
}

func (pt *ProceduresTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (pt *ProceduresTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	if pt.backingTable == nil {
		// no backing table; return an empty iter.
		return index.SinglePartitionIterFromNomsMap(nil), nil
	}
	return pt.backingTable.Partitions(ctx)
}

func (pt *ProceduresTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if pt.backingTable == nil {
		// no backing table; return an empty iter.
		return sql.RowsToRowIter(), nil
	}

	return pt.backingTable.PartitionRows(ctx, partition)
}

func (pt *ProceduresTable) LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error) {
	if pt.backingTable == nil {
		return pt, nil

	}
	return pt.backingTable.LockedToRoot(ctx, root)
}

func (pt *ProceduresTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	// Never reached. Interface required for LockedToRoot to be implemented.
	panic("Unreachable")
}

func (pt *ProceduresTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

func (pt *ProceduresTable) PreciseMatch() bool {
	return true
}

func (pt *ProceduresTable) UnWrap() *WritableDoltTable {
	return pt.backingTable
}

func NewProceduresTable(backing *WritableDoltTable) sql.Table {
	return &ProceduresTable{backingTable: backing}
}

func NewEmptyProceduresTable() sql.Table {
	return &ProceduresTable{}
}

var _ sql.Table = (*ProceduresTable)(nil)
var _ dtables.VersionableTable = (*ProceduresTable)(nil)
var _ sql.IndexAddressableTable = (*ProceduresTable)(nil)
var _ WritableDoltTableWrapper = (*ProceduresTable)(nil)

// The fixed SQL schema for the `dolt_procedures` table.
func ProceduresTableSqlSchema() sql.PrimaryKeySchema {
	sqlSchema, err := sqlutil.FromDoltSchema("", doltdb.ProceduresTableName, ProceduresTableSchema())
	if err != nil {
		panic(err) // should never happen
	}
	return sqlSchema
}

// The fixed dolt schema for the `dolt_procedures` table.
func ProceduresTableSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn(doltdb.ProceduresTableNameCol, schema.DoltProceduresNameTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn(doltdb.ProceduresTableCreateStmtCol, schema.DoltProceduresCreateStmtTag, types.StringKind, false),
		schema.NewColumn(doltdb.ProceduresTableCreatedAtCol, schema.DoltProceduresCreatedAtTag, types.TimestampKind, false),
		schema.NewColumn(doltdb.ProceduresTableModifiedAtCol, schema.DoltProceduresModifiedAtTag, types.TimestampKind, false),
		schema.NewColumn(doltdb.ProceduresTableSqlModeCol, schema.DoltProceduresSqlModeTag, types.StringKind, false),
	)
	return schema.MustSchemaFromCols(colColl)
}

// DoltProceduresGetOrCreateTable returns the `dolt_procedures` table from the given db, creating it in the db's
// current root if it doesn't exist.
func DoltProceduresGetOrCreateTable(ctx *sql.Context, db Database) (*WritableDoltTable, error) {
	tbl, found, err := db.GetTableInsensitive(ctx, doltdb.ProceduresTableName)
	if err != nil {
		return nil, err
	}
	if !found {
		// Should never happen.
		panic("runtime error. dolt_procedures table not found")
	}

	wrapper, ok := tbl.(*ProceduresTable)
	if !ok {
		return nil, fmt.Errorf("expected a ProceduresTable, but got %T", tbl)
	}

	if wrapper.backingTable == nil {
		// We haven't materialized the table yet. Go ahead and do so.
		root, err := db.GetRoot(ctx)
		if err != nil {
			return nil, err
		}
		err = db.createDoltTable(ctx, doltdb.ProceduresTableName, doltdb.DefaultSchemaName, root, ProceduresTableSchema())
		if err != nil {
			return nil, err
		}
		tbl, _, err = db.GetTableInsensitive(ctx, doltdb.ProceduresTableName)
		if err != nil {
			return nil, err
		}
		wrapper, ok = tbl.(*ProceduresTable)
		if !ok {
			return nil, fmt.Errorf("expected a ProceduresTable, but got %T", tbl)
		}
		if wrapper.backingTable == nil {
			return nil, sql.ErrTableNotFound.New(ProceduresTableName)
		}
		return wrapper.backingTable, nil
	} else {
		return migrateDoltProceduresSchema(ctx, db, wrapper.backingTable)
	}
}

// migrateDoltProceduresSchema migrates the dolt_procedures system table from a previous schema version to the current
// schema version by adding any columns that do not exist.
func migrateDoltProceduresSchema(ctx *sql.Context, db Database, oldTable *WritableDoltTable) (newTable *WritableDoltTable, rerr error) {
	// Check whether the table needs to be migrated
	targetSchema := ProceduresTableSqlSchema().Schema
	if len(oldTable.Schema()) == len(targetSchema) {
		return oldTable, nil
	}

	// Copy all the old data
	iter, err := SqlTableToRowIter(ctx, oldTable.DoltTable, nil)
	if err != nil {
		return nil, err
	}

	nameIdx := oldTable.sqlSchema().IndexOfColName(doltdb.ProceduresTableNameCol)
	createStatementIdx := oldTable.sqlSchema().IndexOfColName(doltdb.ProceduresTableCreateStmtCol)
	createdAtIdx := oldTable.sqlSchema().IndexOfColName(doltdb.ProceduresTableCreatedAtCol)
	modifiedAtIdx := oldTable.sqlSchema().IndexOfColName(doltdb.ProceduresTableModifiedAtCol)
	sqlModeIdx := oldTable.sqlSchema().IndexOfColName(doltdb.ProceduresTableSqlModeCol)

	defer func(iter sql.RowIter, ctx *sql.Context) {
		err := iter.Close(ctx)
		if err != nil && rerr == nil {
			rerr = err
		}
	}(iter, ctx)

	var newRows []sql.Row
	for {
		sqlRow, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		newRow := make(sql.Row, ProceduresTableSchema().GetAllCols().Size())
		newRow[0] = sqlRow[nameIdx]
		newRow[1] = sqlRow[createStatementIdx]
		newRow[2] = sqlRow[createdAtIdx]
		newRow[3] = sqlRow[modifiedAtIdx]
		if sqlModeIdx >= 0 {
			newRow[4] = sqlRow[sqlModeIdx]
		}
		newRows = append(newRows, newRow)
	}

	err = db.dropTable(ctx, doltdb.ProceduresTableName)
	if err != nil {
		return nil, err
	}

	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	err = db.createDoltTable(ctx, doltdb.ProceduresTableName, doltdb.DefaultSchemaName, root, ProceduresTableSchema())
	if err != nil {
		return nil, err
	}

	tbl, _, err := db.GetTableInsensitive(ctx, doltdb.ProceduresTableName)
	if err != nil {
		return nil, err
	}

	wrapper, ok := tbl.(*ProceduresTable)
	if !ok {
		return nil, fmt.Errorf("expected a ProceduresTable, but got %T", tbl)
	}
	if wrapper.backingTable == nil {
		return nil, sql.ErrTableNotFound.New(doltdb.ProceduresTableName)
	}

	inserter := wrapper.backingTable.Inserter(ctx)
	for _, row := range newRows {
		err = inserter.Insert(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	err = inserter.Close(ctx)
	if err != nil {
		return nil, err
	}

	return wrapper.backingTable, nil
}

// DoltProceduresGetTable returns the `dolt_procedures` table from the given db, or nil if the table doesn't exist
func DoltProceduresGetTable(ctx *sql.Context, db Database) (*WritableDoltTable, error) {
	tbl, _, err := db.GetTableInsensitive(ctx, doltdb.ProceduresTableName)
	if err != nil {
		return nil, err
	}

	wrapper, ok := tbl.(*ProceduresTable)
	if !ok {
		return nil, fmt.Errorf("expected a ProceduresTable, but got %T", tbl)
	}
	if wrapper.backingTable != nil {
		return migrateDoltProceduresSchema(ctx, db, wrapper.backingTable)
	}
	return nil, nil
}

// DoltProceduresGetAll returns all stored procedures for the database if the procedureName is blank (and empty string),
// or it returns only the procedure with the matching name if one is given. The name is not case-sensitive.
func DoltProceduresGetAll(ctx *sql.Context, db Database, procedureName string) ([]sql.StoredProcedureDetails, error) {
	tbl, err := DoltProceduresGetTable(ctx, db)
	if err != nil {
		return nil, err
	} else if tbl == nil {
		return nil, nil
	}

	indexes, err := tbl.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	if len(indexes) == 0 {
		return nil, fmt.Errorf("missing index for stored procedures")
	}
	idx := indexes[0]

	if len(idx.Expressions()) == 0 {
		return nil, fmt.Errorf("missing index expression for stored procedures")
	}
	nameExpr := idx.Expressions()[0]

	var lookup sql.IndexLookup
	if procedureName == "" {
		lookup, err = sql.NewIndexBuilder(idx).IsNotNull(ctx, nameExpr).Build(ctx)
	} else {
		lookup, err = sql.NewIndexBuilder(idx).Equals(ctx, nameExpr, procedureName).Build(ctx)
	}
	if err != nil {
		return nil, err
	}

	iter, err := index.RowIterForIndexLookup(ctx, tbl.DoltTable, lookup, tbl.sqlSch, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := iter.Close(ctx); cerr != nil {
			err = cerr
		}
	}()

	var sqlRow sql.Row
	var details []sql.StoredProcedureDetails
	missingValue := errors.NewKind("missing `%s` value for procedure row: (%s)")

	for {
		sqlRow, err = iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		var d sql.StoredProcedureDetails
		var ok bool

		if d.Name, ok = sqlRow[0].(string); !ok {
			return nil, missingValue.New(doltdb.ProceduresTableNameCol, sqlRow)
		}
		if d.CreateStatement, ok = sqlRow[1].(string); !ok {
			return nil, missingValue.New(doltdb.ProceduresTableCreateStmtCol, sqlRow)
		}
		if d.CreatedAt, ok = sqlRow[2].(time.Time); !ok {
			return nil, missingValue.New(doltdb.ProceduresTableCreatedAtCol, sqlRow)
		}
		if d.ModifiedAt, ok = sqlRow[3].(time.Time); !ok {
			return nil, missingValue.New(doltdb.ProceduresTableModifiedAtCol, sqlRow)
		}
		if s, ok := sqlRow[4].(string); ok {
			d.SqlMode = s
		} else {
			defaultSqlMode, err := loadDefaultSqlMode()
			if err != nil {
				return nil, err
			}
			d.SqlMode = defaultSqlMode
		}
		details = append(details, d)
	}
	return details, nil
}

// DoltProceduresAddProcedure adds the stored procedure to the `dolt_procedures` table in the given db, creating it if
// it does not exist.
func DoltProceduresAddProcedure(ctx *sql.Context, db Database, spd sql.StoredProcedureDetails) (retErr error) {
	tbl, err := DoltProceduresGetOrCreateTable(ctx, db)
	if err != nil {
		return err
	}
	_, ok, err := DoltProceduresGetDetails(ctx, tbl, spd.Name)
	if err != nil {
		return err
	}
	if ok {
		return sql.ErrStoredProcedureAlreadyExists.New(spd.Name)
	}
	inserter := tbl.Inserter(ctx)
	defer func() {
		err := inserter.Close(ctx)
		if retErr == nil {
			retErr = err
		}
	}()
	return inserter.Insert(ctx, sql.Row{
		strings.ToLower(spd.Name),
		spd.CreateStatement,
		spd.CreatedAt.UTC(),
		spd.ModifiedAt.UTC(),
		spd.SqlMode,
	})
}

// DoltProceduresDropProcedure removes the stored procedure from the `dolt_procedures` table. The procedure named must
// exist.
func DoltProceduresDropProcedure(ctx *sql.Context, db Database, name string) (retErr error) {
	name = strings.ToLower(name)
	tbl, err := DoltProceduresGetTable(ctx, db)
	if err != nil {
		return err
	} else if tbl == nil {
		return sql.ErrStoredProcedureDoesNotExist.New(name)
	}

	_, ok, err := DoltProceduresGetDetails(ctx, tbl, name)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrStoredProcedureDoesNotExist.New(name)
	}
	deleter := tbl.Deleter(ctx)
	defer func() {
		err := deleter.Close(ctx)
		if retErr == nil {
			retErr = err
		}
	}()
	return deleter.Delete(ctx, sql.Row{name})
}

// DoltProceduresGetDetails returns the stored procedure with the given name from `dolt_procedures` if it exists.
func DoltProceduresGetDetails(ctx *sql.Context, tbl *WritableDoltTable, name string) (sql.StoredProcedureDetails, bool, error) {
	name = strings.ToLower(name)
	indexes, err := tbl.GetIndexes(ctx)
	if err != nil {
		return sql.StoredProcedureDetails{}, false, err
	}
	var fragNameIndex sql.Index
	for _, idx := range indexes {
		if idx.ID() == "PRIMARY" {
			fragNameIndex = idx
			break
		}
	}
	if fragNameIndex == nil {
		return sql.StoredProcedureDetails{}, false, fmt.Errorf("could not find primary key index on system table `%s`", doltdb.ProceduresTableName)
	}

	indexLookup, err := sql.NewIndexBuilder(fragNameIndex).Equals(ctx, fragNameIndex.Expressions()[0], name).Build(ctx)
	if err != nil {
		return sql.StoredProcedureDetails{}, false, err
	}

	rowIter, err := index.RowIterForIndexLookup(ctx, tbl.DoltTable, indexLookup, tbl.sqlSch, nil)
	if err != nil {
		return sql.StoredProcedureDetails{}, false, err
	}
	defer func() {
		if cerr := rowIter.Close(ctx); cerr != nil {
			err = cerr
		}
	}()

	sqlRow, err := rowIter.Next(ctx)
	if err == nil {
		if len(sqlRow) != 5 {
			return sql.StoredProcedureDetails{}, false, fmt.Errorf("unexpected row in dolt_procedures:\n%v", sqlRow)
		}
		return sql.StoredProcedureDetails{
			Name:            sqlRow[0].(string),
			CreateStatement: sqlRow[1].(string),
			CreatedAt:       sqlRow[2].(time.Time),
			ModifiedAt:      sqlRow[3].(time.Time),
		}, true, nil
	} else if err == io.EOF {
		return sql.StoredProcedureDetails{}, false, nil
	} else {
		return sql.StoredProcedureDetails{}, false, err
	}
}
