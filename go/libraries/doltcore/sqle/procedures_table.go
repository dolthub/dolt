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

// The fixed SQL schema for the `dolt_procedures` table.
func ProceduresTableSqlSchema() sql.PrimaryKeySchema {
	sqlSchema, err := sqlutil.FromDoltSchema(doltdb.ProceduresTableName, ProceduresTableSchema())
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
	)
	return schema.MustSchemaFromCols(colColl)
}

// DoltProceduresGetOrCreateTable returns the `dolt_procedures` table from the given db, creating it in the db's
// current root if it doesn't exist
func DoltProceduresGetOrCreateTable(ctx *sql.Context, db Database) (*WritableDoltTable, error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	tbl, found, err := db.GetTableInsensitiveWithRoot(ctx, root, doltdb.ProceduresTableName)
	if err != nil {
		return nil, err
	}
	if found {
		return tbl.(*WritableDoltTable), nil
	}

	err = db.createDoltTable(ctx, doltdb.ProceduresTableName, root, ProceduresTableSchema())
	if err != nil {
		return nil, err
	}
	root, err = db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	tbl, found, err = db.GetTableInsensitiveWithRoot(ctx, root, doltdb.ProceduresTableName)
	if err != nil {
		return nil, err
	}
	// Verify it was created successfully
	if !found {
		return nil, sql.ErrTableNotFound.New(ProceduresTableName)
	}
	return tbl.(*WritableDoltTable), nil
}

// DoltProceduresGetTable returns the `dolt_procedures` table from the given db, or nil if the table doesn't exist
func DoltProceduresGetTable(ctx *sql.Context, db Database) (*WritableDoltTable, error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	tbl, found, err := db.GetTableInsensitiveWithRoot(ctx, root, doltdb.ProceduresTableName)
	if err != nil {
		return nil, err
	}
	if found {
		return tbl.(*WritableDoltTable), nil
	} else {
		return nil, nil
	}
}

func DoltProceduresGetAll(ctx *sql.Context, db Database) ([]sql.StoredProcedureDetails, error) {
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

	lookup, err := sql.NewIndexBuilder(ctx, idx).IsNotNull(ctx, nameExpr).Build(ctx)
	if err != nil {
		return nil, err
	}

	dt, err := tbl.doltTable(ctx)
	if err != nil {
		return nil, err
	}

	iter, err := index.RowIterForIndexLookup(ctx, dt, lookup, tbl.sqlSch, nil)
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
	})
}

// DoltProceduresDropProcedure removes the stored procedure from the `dolt_procedures` table. The procedure named must
// exist.
func DoltProceduresDropProcedure(ctx *sql.Context, db Database, name string) (retErr error) {
	strings.ToLower(name)
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
		return sql.StoredProcedureDetails{}, false, fmt.Errorf("could not find primary key index on system table `%s`",
			doltdb.SchemasTableName)
	}

	indexLookup, err := sql.NewIndexBuilder(ctx, fragNameIndex).Equals(ctx, fragNameIndex.Expressions()[0], name).Build(ctx)
	if err != nil {
		return sql.StoredProcedureDetails{}, false, err
	}

	dt, err := tbl.doltTable(ctx)
	if err != nil {
		return sql.StoredProcedureDetails{}, false, err
	}

	rowIter, err := index.RowIterForIndexLookup(ctx, dt, indexLookup, tbl.sqlSch, nil)
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
		if len(sqlRow) != 4 {
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
