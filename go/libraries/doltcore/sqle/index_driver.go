// Copyright 2020 Liquidata, Inc.
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

	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

type DoltIndexDriver struct {
	dbs map[string]Database
}

var _ sql.IndexDriver = (*DoltIndexDriver)(nil)

func NewDoltIndexDriver(dbs ...Database) *DoltIndexDriver {
	nameToDB := make(map[string]Database)
	for _, db := range dbs {
		nameToDB[db.Name()] = db
	}

	return &DoltIndexDriver{nameToDB}
}

func (*DoltIndexDriver) Create(string, string, string, []sql.Expression, map[string]string) (sql.Index, error) {
	panic("index driver create path not supported")
}

func (i *DoltIndexDriver) Delete(sql.Index, sql.PartitionIter) error {
	panic("index driver delete path not supported")
}

func (*DoltIndexDriver) ID() string {
	return "doltDbIndexDriver"
}

func (driver *DoltIndexDriver) LoadAll(ctx *sql.Context, db, table string) ([]sql.Index, error) {
	database, ok := driver.dbs[db]
	if !ok {
		panic("Unexpected db: " + db)
	}

	root, err := database.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	tbl, ok, err := root.GetTable(ctx, table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	rowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	cols := sch.GetPKCols().GetColumns()
	sqlIndexes := []sql.Index{
		&doltIndex{
			cols:         cols,
			ctx:          ctx,
			db:           database,
			driver:       driver,
			id:           fmt.Sprintf("%s:primaryKey%v", table, len(cols)),
			indexRowData: rowData,
			indexSch:     sch,
			table:        tbl,
			tableData:    rowData,
			tableName:    table,
			tableSch:     sch,
		},
	}
	for _, index := range sch.Indexes().AllIndexes() {
		indexRowData, err := tbl.GetIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
		cols := make([]schema.Column, index.Count())
		for i, tag := range index.IndexedColumnTags() {
			cols[i], _ = index.GetColumn(tag)
		}
		sqlIndexes = append(sqlIndexes, &doltIndex{
			cols:         cols,
			ctx:          ctx,
			db:           database,
			driver:       driver,
			id:           table + ":" + index.Name(),
			indexRowData: indexRowData,
			indexSch:     index.Schema(),
			table:        tbl,
			tableData:    rowData,
			tableName:    table,
			tableSch:     sch,
		})
	}

	return sqlIndexes, nil
}

func (i *DoltIndexDriver) Save(*sql.Context, sql.Index, sql.PartitionIndexKeyValueIter) error {
	panic("index driver save path not supported")
}
