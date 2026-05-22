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

package sqle

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
)

const DoltClusterDbName = "dolt_cluster"

type clusterDatabase struct {
	statusProvider ClusterStatusProvider
}

var _ sql.Database = clusterDatabase{}
var _ dsess.VersionedDatabase = clusterDatabase{}
var _ SqlDatabase = clusterDatabase{}

const StatusTableName = "dolt_cluster_status"

func (clusterDatabase) Name() string {
	return DoltClusterDbName
}

func (db clusterDatabase) Schema() string {
	return ""
}

func (db clusterDatabase) Close() {
}

func (db clusterDatabase) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	tblName = strings.ToLower(tblName)
	if tblName == StatusTableName {
		return NewClusterStatusTable(db.statusProvider), true, nil
	}
	return nil, false, nil
}

func (clusterDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	return []string{StatusTableName}, nil
}

func NewClusterDatabase(p ClusterStatusProvider) sql.Database {
	return clusterDatabase{p}
}

// Implement StoredProcedureDatabase so that external stored procedures are available.
var _ sql.StoredProcedureDatabase = clusterDatabase{}

func (clusterDatabase) GetStoredProcedure(ctx *sql.Context, name string) (sql.StoredProcedureDetails, bool, error) {
	return sql.StoredProcedureDetails{}, false, nil
}

func (clusterDatabase) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	return nil, nil
}

func (clusterDatabase) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	return errors.New("unimplemented")
}

func (clusterDatabase) DropStoredProcedure(ctx *sql.Context, name string) error {
	return errors.New("unimplemented")
}

var _ sql.ViewDatabase = clusterDatabase{}

func (db clusterDatabase) CreateView(ctx *sql.Context, name string, selectStatement, createViewStmt string) error {
	return errors.New("unimplemented")
}

func (db clusterDatabase) DropView(ctx *sql.Context, name string) error {
	return errors.New("unimplemented")
}

func (db clusterDatabase) GetViewDefinition(ctx *sql.Context, viewName string) (sql.ViewDefinition, bool, error) {
	return sql.ViewDefinition{}, false, nil
}

func (db clusterDatabase) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	return nil, nil
}

var _ sql.ReadOnlyDatabase = clusterDatabase{}

func (clusterDatabase) IsReadOnly() bool {
	return true
}

func (db clusterDatabase) InitialDBState(ctx *sql.Context) (dsess.InitialDbState, error) {
	// TODO: almost none of this state is actually used, but is necessary because the current session setup requires a
	//  repo state writer
	return dsess.InitialDbState{
		Db: db,
		DbData: env.DbData[*sql.Context]{
			Rsw: noopRepoStateWriter{},
		},
		ReadOnly: true,
		Remotes:  concurrentmap.New[string, env.Remote](),
	}, nil
}

func (db clusterDatabase) WithBranchRevision(requestedName string, branchSpec dsess.SessionDatabaseBranchSpec) (SqlDatabase, error) {
	// Nothing to do here, we don't support changing branch revisions
	return db, nil
}

func (db clusterDatabase) DoltDatabases() []*doltdb.DoltDB {
	return nil
}

func (db clusterDatabase) ShouldCollectStats() bool {
	return false
}

func (db clusterDatabase) GetRoot(context *sql.Context) (doltdb.RootValue, error) {
	return nil, errors.New("unimplemented")
}

func (db clusterDatabase) DbData() env.DbData[*sql.Context] {
	return env.DbData[*sql.Context]{}
}

func (db clusterDatabase) EditOptions() editor.Options {
	return editor.Options{}
}

func (db clusterDatabase) GetGlobalState() globalstate.GlobalState {
	return globalstate.NoOp{}
}

func (db clusterDatabase) Revision() string {
	return ""
}

func (db clusterDatabase) Versioned() bool {
	return false
}

func (db clusterDatabase) RevisionType() dsess.RevisionType {
	return dsess.RevisionTypeNone
}

func (db clusterDatabase) RevisionQualifiedName() string {
	return db.Name()
}

func (db clusterDatabase) RequestedName() string {
	return db.Name()
}

func (db clusterDatabase) AliasedName() string {
	return db.Name()
}

// noopRepoStateWriter is defined in database.go (same package).

func (db clusterDatabase) GetSchema(ctx *sql.Context, schemaName string) (sql.DatabaseSchema, bool, error) {
	panic(fmt.Sprintf("GetSchema is not implemented for clusterDatabase %T", db))
}

func (db clusterDatabase) SupportsDatabaseSchemas() bool {
	return false
}

func (db clusterDatabase) CreateSchema(ctx *sql.Context, schemaName string) error {
	panic(fmt.Sprintf("CreateSchema is not implemented for clusterDatabase %T", db))
}

func (db clusterDatabase) DropSchema(ctx *sql.Context, schemaName string) error {
	panic(fmt.Sprintf("DropSchema is not implemented for clusterDatabase %T", db))
}

func (db clusterDatabase) AllSchemas(ctx *sql.Context) ([]sql.DatabaseSchema, error) {
	panic(fmt.Sprintf("AllSchemas is not implemented for clusterDatabase %T", db))
}

func (db clusterDatabase) SchemaName() string {
	return ""
}

func (db clusterDatabase) GetTableResolver() doltdb.TableResolver {
	return doltdb.SimpleTableResolver{}
}
