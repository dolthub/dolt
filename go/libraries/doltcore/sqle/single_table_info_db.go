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
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	sqleSchema "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// SingleTableInfoDatabase is intended to allow a sole schema to make use of any display functionality in `go-mysql-server`.
// For example, you may have constructed a schema that you want a CREATE TABLE statement for, but the schema is not
// persisted or is temporary. This allows `go-mysql-server` to interact with that sole schema as though it were a database.
// No write operations will work with this database.
type SingleTableInfoDatabase struct {
	tableName   string
	sch         schema.Schema
	foreignKeys []doltdb.ForeignKey
	parentSchs  map[string]schema.Schema
}

var _ sql.Database = (*SingleTableInfoDatabase)(nil)
var _ sql.Table = (*SingleTableInfoDatabase)(nil)
var _ sql.IndexedTable = (*SingleTableInfoDatabase)(nil)
var _ sql.ForeignKeyTable = (*SingleTableInfoDatabase)(nil)

func NewSingleTableDatabase(tableName string, sch schema.Schema, foreignKeys []doltdb.ForeignKey, parentSchs map[string]schema.Schema) *SingleTableInfoDatabase {
	return &SingleTableInfoDatabase{
		tableName:   tableName,
		sch:         sch,
		foreignKeys: foreignKeys,
		parentSchs:  parentSchs,
	}
}

// Name implements sql.Table and sql.Database.
func (db *SingleTableInfoDatabase) Name() string {
	return db.tableName
}

// GetTableInsensitive implements sql.Database.
func (db *SingleTableInfoDatabase) GetTableInsensitive(ctx *sql.Context, tableName string) (sql.Table, bool, error) {
	if strings.ToLower(tableName) == strings.ToLower(db.tableName) {
		return db, true, nil
	}
	return nil, false, nil
}

// GetTableNames implements sql.Database.
func (db *SingleTableInfoDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	return []string{db.tableName}, nil
}

// String implements sql.Table.
func (db *SingleTableInfoDatabase) String() string {
	return db.tableName
}

// Schema implements sql.Table.
func (db *SingleTableInfoDatabase) Schema() sql.Schema {
	sqlSch, err := sqleSchema.FromDoltSchema(db.tableName, db.sch)
	if err != nil {
		panic(err)
	}
	return sqlSch
}

// Partitions implements sql.Table.
func (db *SingleTableInfoDatabase) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return nil, fmt.Errorf("cannot get paritions of a single table information database")
}

// PartitionRows implements sql.Table.
func (db *SingleTableInfoDatabase) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return nil, fmt.Errorf("cannot get parition rows of a single table information database")
}

// GetForeignKeys implements sql.ForeignKeyTable.
func (db *SingleTableInfoDatabase) GetForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	fks := make([]sql.ForeignKeyConstraint, len(db.foreignKeys))
	for i, fk := range db.foreignKeys {
		if parentSch, ok := db.parentSchs[fk.ReferencedTableName]; ok {
			var err error
			fks[i], err = toForeignKeyConstraint(fk, db.sch, parentSch)
			if err != nil {
				return nil, err
			}
		} else {
			// We can skip here since the given schema may be purposefully incomplete (such as with diffs).
			continue
		}
	}
	return fks, nil
}

// WithIndexLookup implements sql.IndexedTable.
func (db *SingleTableInfoDatabase) WithIndexLookup(sql.IndexLookup) sql.Table {
	return db
}

// GetIndexes implements sql.IndexedTable.
func (db *SingleTableInfoDatabase) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	var sqlIndexes []sql.Index
	for _, index := range db.sch.Indexes().AllIndexes() {
		cols := make([]schema.Column, index.Count())
		for i, tag := range index.IndexedColumnTags() {
			cols[i], _ = index.GetColumn(tag)
		}
		sqlIndexes = append(sqlIndexes, &doltIndex{
			cols:         cols,
			db:           db,
			id:           index.Name(),
			indexRowData: types.EmptyMap,
			indexSch:     index.Schema(),
			table:        nil,
			tableData:    types.EmptyMap,
			tableName:    db.tableName,
			tableSch:     db.sch,
			unique:       index.IsUnique(),
			comment:      index.Comment(),
		})
	}
	return sqlIndexes, nil
}
