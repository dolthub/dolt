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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
)

// UserSpaceDatabase in an implementation of sql.Database for root values. Does not expose any of the internal dolt tables.
type UserSpaceDatabase struct {
	doltdb.RootValue

	editOpts editor.Options
}

var _ dsess.SqlDatabase = (*UserSpaceDatabase)(nil)

func NewUserSpaceDatabase(root doltdb.RootValue, editOpts editor.Options) *UserSpaceDatabase {
	return &UserSpaceDatabase{RootValue: root, editOpts: editOpts}
}

func (db *UserSpaceDatabase) Name() string {
	return "dolt"
}

func (db *UserSpaceDatabase) Schema() string {
	return ""
}

func (db *UserSpaceDatabase) GetTableInsensitive(ctx *sql.Context, tableName string) (sql.Table, bool, error) {
	if doltdb.IsReadOnlySystemTable(tableName) {
		return nil, false, nil
	}
	table, tableName, ok, err := doltdb.GetTableInsensitive(ctx, db.RootValue, doltdb.TableName{Name: tableName})
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
	dt, err := NewDoltTable(tableName, sch, table, db, db.editOpts)
	if err != nil {
		return nil, false, err
	}
	return dt, true, nil
}

func (db *UserSpaceDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	tableNames, err := db.RootValue.GetTableNames(ctx, doltdb.DefaultSchemaName)
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

func (db *UserSpaceDatabase) InitialDBState(ctx *sql.Context) (dsess.InitialDbState, error) {
	return dsess.InitialDbState{
		Db:       db,
		ReadOnly: true,
		HeadRoot: db.RootValue,
		DbData: env.DbData{
			Rsw: noopRepoStateWriter{},
		},
		Remotes: concurrentmap.New[string, env.Remote](),
	}, nil
}

func (db *UserSpaceDatabase) WithBranchRevision(requestedName string, branchSpec dsess.SessionDatabaseBranchSpec) (dsess.SqlDatabase, error) {
	// Nothing to do here, we don't support changing branch revisions
	return db, nil
}

func (db *UserSpaceDatabase) DoltDatabases() []*doltdb.DoltDB {
	return nil
}

func (db *UserSpaceDatabase) GetRoot(*sql.Context) (doltdb.RootValue, error) {
	return db.RootValue, nil
}

func (db *UserSpaceDatabase) GetTemporaryTablesRoot(*sql.Context) (doltdb.RootValue, bool) {
	panic("UserSpaceDatabase should not contain any temporary tables")
}

func (db *UserSpaceDatabase) DbData() env.DbData {
	return env.DbData{}
}

func (db *UserSpaceDatabase) EditOptions() editor.Options {
	return db.editOpts
}

func (db *UserSpaceDatabase) Revision() string {
	return ""
}

func (db *UserSpaceDatabase) Versioned() bool {
	return false
}

func (db *UserSpaceDatabase) RevisionType() dsess.RevisionType {
	return dsess.RevisionTypeNone
}

func (db *UserSpaceDatabase) RevisionQualifiedName() string {
	return db.Name()
}

func (db *UserSpaceDatabase) RequestedName() string {
	return db.Name()
}

func (db *UserSpaceDatabase) GetSchema(ctx *sql.Context, schemaName string) (sql.DatabaseSchema, bool, error) {
	panic(fmt.Sprintf("GetSchema is not implemented for database %T", db))
}

func (db *UserSpaceDatabase) CreateSchema(ctx *sql.Context, schemaName string) error {
	panic(fmt.Sprintf("CreateSchema is not implemented for database %T", db))
}

func (db *UserSpaceDatabase) AllSchemas(ctx *sql.Context) ([]sql.DatabaseSchema, error) {
	panic(fmt.Sprintf("AllSchemas is not implemented for database %T", db))
}

func (db *UserSpaceDatabase) SchemaName() string {
	return ""
}