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
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

type BackupsTable struct {
	db        dsess.SqlDatabase
	tableName string
}

var _ sql.Table = (*BackupsTable)(nil)

func NewBackupsTable(db dsess.SqlDatabase, tableName string) *BackupsTable {
	return &BackupsTable{db: db, tableName: tableName}
}

func (bt BackupsTable) Name() string {
	return bt.tableName
}

func (bt BackupsTable) String() string {
	return bt.tableName
}

func (bt BackupsTable) Schema() sql.Schema {
	columns := []*sql.Column{
		{Name: "name", Type: types.Text, Source: bt.tableName, PrimaryKey: true, Nullable: false, DatabaseSource: bt.db.Name()},
		{Name: "url", Type: types.Text, Source: bt.tableName, PrimaryKey: false, Nullable: false, DatabaseSource: bt.db.Name()},
	}
	return columns
}

func (bt BackupsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (bt BackupsTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (bt BackupsTable) PartitionRows(context *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return newBackupsIter(context)
}

type backupsItr struct {
	names []string
	urls  map[string]string
	idx   int
}

var _ sql.RowIter = (*backupsItr)(nil)

func (bi *backupsItr) Next(ctx *sql.Context) (sql.Row, error) {
	if bi.idx < len(bi.names) {
		bi.idx++
		name := bi.names[bi.idx-1]
		return sql.NewRow(name, bi.urls[name]), nil
	}
	return nil, io.EOF
}

func (bi *backupsItr) Close(_ *sql.Context) error { return nil }

func newBackupsIter(ctx *sql.Context) (*backupsItr, error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return nil, fmt.Errorf("Empty database name.")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	backups, err := dbData.Rsr.GetBackups()
	if err != nil {
		return nil, err
	}

	names := make([]string, 0)
	urls := map[string]string{}

	backups.Iter(func(key string, val env.Remote) bool {
		names = append(names, key)
		urls[key] = val.Url
		return true
	})

	sort.Strings(names)

	return &backupsItr{names: names, urls: urls, idx: 0}, nil
}
