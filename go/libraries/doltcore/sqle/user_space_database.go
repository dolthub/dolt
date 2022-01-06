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

package sqle

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

// UserSpaceDatabase in an implementation of sql.Database for root values. Does not expose any of the internal dolt tables.
type UserSpaceDatabase struct {
	*doltdb.RootValue

	editOpts editor.Options
}

var _ SqlDatabase = (*UserSpaceDatabase)(nil)

func NewUserSpaceDatabase(root *doltdb.RootValue, editOpts editor.Options) *UserSpaceDatabase {
	return &UserSpaceDatabase{RootValue: root, editOpts: editOpts}
}

func (db *UserSpaceDatabase) Name() string {
	return "dolt"
}

func (db *UserSpaceDatabase) GetTableInsensitive(ctx *sql.Context, tableName string) (sql.Table, bool, error) {
	if doltdb.IsReadOnlySystemTable(tableName) {
		return nil, false, nil
	}
	table, tableName, ok, err := db.RootValue.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, false, err
	}
	dt, err := NewDoltTable(tableName, sch, table, db, false, db.editOpts)
	if err != nil {
		return nil, false, err
	}
	return dt, true, nil
}

func (db *UserSpaceDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	tableNames, err := db.RootValue.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	resultingTblNames := []string{}
	for _, tbl := range tableNames {
		if !doltdb.IsReadOnlySystemTable(tbl) {
			resultingTblNames = append(resultingTblNames, tbl)
		}
	}
	return resultingTblNames, nil
}

func (db *UserSpaceDatabase) GetRoot(*sql.Context) (*doltdb.RootValue, error) {
	return db.RootValue, nil
}

func (db *UserSpaceDatabase) GetTemporaryTablesRoot(*sql.Context) (*doltdb.RootValue, bool) {
	panic("UserSpaceDatabase should not contain any temporary tables")
}

func (db *UserSpaceDatabase) DbData() env.DbData {
	panic("UserSpaceDatabase does not have dbdata")
}

func (db *UserSpaceDatabase) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	panic("UserSpaceDatabase does not support transactions")
}

func (db *UserSpaceDatabase) Flush(ctx *sql.Context) error {
	panic("UserSpaceDatabase cannot flush")
}

func (db *UserSpaceDatabase) EditOptions() editor.Options {
	panic("UserSpaceDatabase does not have edit options")
}
