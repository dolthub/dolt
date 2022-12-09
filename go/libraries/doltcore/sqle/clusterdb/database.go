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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

type database struct {
	statusProvider ClusterStatusProvider
}

var _ sql.Database = database{}
var _ sqle.SqlDatabase = database{}

const StatusTableName = "dolt_cluster_status"

func (database) Name() string {
	return "dolt_cluster"
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

func (database) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	return nil, nil
}

func (database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	return errors.New("unimplemented")
}

func (database) DropStoredProcedure(ctx *sql.Context, name string) error {
	return errors.New("unimplemented")
}

var _ sql.ReadOnlyDatabase = database{}

func (database) IsReadOnly() bool {
	return true
}

func (db database) InitialDBState(ctx context.Context) (dsess.InitialDbState, error) {
	return dsess.InitialDbState{}, errors.New("unimplemented")
}

func (db database) GetRoot(context *sql.Context) (*doltdb.RootValue, error) {
	return nil, errors.New("unimplemented")
}

func (db database) DbData() env.DbData {
	panic("unimplemented")
}

func (db database) Flush(context *sql.Context) error {
	return errors.New("unimplemented")
}

func (db database) EditOptions() editor.Options {
	return editor.Options{}
}