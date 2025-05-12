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

package clusterdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
)

const DoltClusterDbName = "dolt_cluster"

type database struct {
	statusProvider ClusterStatusProvider
}

var _ sql.Database = database{}
var _ dsess.SqlDatabase = database{}

const StatusTableName = "dolt_cluster_status"

func (database) Name() string {
	return DoltClusterDbName
}

func (db database) Schema() string {
	return ""
}

func (db database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	tblName = strings.ToLower(tblName)
	if tblName == StatusTableName {
		return NewClusterStatusTable(db.statusProvider), true, nil
	}
	return nil, false, nil
}

func (database) GetTableNames(ctx *sql.Context) ([]string, error) {
	return []string{StatusTableName}, nil
}

func NewClusterDatabase(p ClusterStatusProvider) sql.Database {
	return database{p}
}

// Implement StoredProcedureDatabase so that external stored procedures are available.
var _ sql.StoredProcedureDatabase = database{}

func (database) GetStoredProcedure(ctx *sql.Context, name string) (sql.StoredProcedureDetails, bool, error) {
	return sql.StoredProcedureDetails{}, false, nil
}

func (database) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	return nil, nil
}

func (database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	return errors.New("unimplemented")
}

func (database) DropStoredProcedure(ctx *sql.Context, name string) error {
	return errors.New("unimplemented")
}

var _ sql.ViewDatabase = database{}

func (db database) CreateView(ctx *sql.Context, name string, selectStatement, createViewStmt string) error {
	return errors.New("unimplemented")
}

func (db database) DropView(ctx *sql.Context, name string) error {
	return errors.New("unimplemented")
}

func (db database) GetViewDefinitionAsOf(ctx *sql.Context, viewName string, asOf interface{}) (sql.ViewDefinition, bool, error) {
	return sql.ViewDefinition{}, false, nil
}

func (db database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	return nil, nil
}

var _ sql.ReadOnlyDatabase = database{}

func (database) IsReadOnly() bool {
	return true
}

func (db database) InitialDBState(ctx *sql.Context) (dsess.InitialDbState, error) {
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

func (db database) WithBranchRevision(requestedName string, branchSpec dsess.SessionDatabaseBranchSpec) (dsess.SqlDatabase, error) {
	// Nothing to do here, we don't support changing branch revisions
	return db, nil
}

func (db database) DoltDatabases() []*doltdb.DoltDB {
	return nil
}

func (db database) GetRoot(context *sql.Context) (doltdb.RootValue, error) {
	return nil, errors.New("unimplemented")
}

func (db database) DbData() env.DbData[*sql.Context] {
	return env.DbData[*sql.Context]{}
}

func (db database) EditOptions() editor.Options {
	return editor.Options{}
}

func (db database) Revision() string {
	return ""
}

func (db database) Versioned() bool {
	return false
}

func (db database) RevisionType() dsess.RevisionType {
	return dsess.RevisionTypeNone
}

func (db database) RevisionQualifiedName() string {
	return db.Name()
}

func (db database) RequestedName() string {
	return db.Name()
}

func (db database) AliasedName() string {
	return db.Name()
}

type noopRepoStateWriter struct{}

var _ env.RepoStateWriter = noopRepoStateWriter{}

func (n noopRepoStateWriter) SetCWBHeadRef(ctx context.Context, marshalableRef ref.MarshalableRef) error {
	return nil
}

func (n noopRepoStateWriter) AddRemote(r env.Remote) error {
	return nil
}

func (n noopRepoStateWriter) AddBackup(r env.Remote) error {
	return nil
}

func (n noopRepoStateWriter) RemoveRemote(ctx context.Context, name string) error {
	return nil
}

func (n noopRepoStateWriter) RemoveBackup(ctx context.Context, name string) error {
	return nil
}

func (n noopRepoStateWriter) TempTableFilesDir() (string, error) {
	return "", nil
}

func (n noopRepoStateWriter) UpdateBranch(name string, new env.BranchConfig) error {
	return nil
}

func (db database) GetSchema(ctx *sql.Context, schemaName string) (sql.DatabaseSchema, bool, error) {
	panic(fmt.Sprintf("GetSchema is not implemented for database %T", db))
}

func (db database) CreateSchema(ctx *sql.Context, schemaName string) error {
	panic(fmt.Sprintf("CreateSchema is not implemented for database %T", db))
}

func (db database) AllSchemas(ctx *sql.Context) ([]sql.DatabaseSchema, error) {
	panic(fmt.Sprintf("AllSchemas is not implemented for database %T", db))
}

func (db database) SchemaName() string {
	return ""
}
