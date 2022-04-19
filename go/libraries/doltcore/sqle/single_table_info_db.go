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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
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

var _ doltReadOnlyTableInterface = (*SingleTableInfoDatabase)(nil)

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
	sqlSch, err := sqlutil.FromDoltSchema(db.tableName, db.sch)
	if err != nil {
	}
	return sqlSch.Schema
}

// Partitions implements sql.Table.
func (db *SingleTableInfoDatabase) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return nil, fmt.Errorf("cannot get paritions of a single table information database")
}

// PartitionRows implements sql.Table.
func (db *SingleTableInfoDatabase) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return nil, fmt.Errorf("cannot get partition rows of a single table information database")
}

func (db *SingleTableInfoDatabase) PartitionRows2(ctx *sql.Context, part sql.Partition) (sql.RowIter2, error) {
	return nil, fmt.Errorf("cannot get partition rows of a single table information database")
}

// CreateIndexForForeignKey implements sql.ForeignKeyTable.
func (db *SingleTableInfoDatabase) CreateIndexForForeignKey(ctx *sql.Context, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn) error {
	return fmt.Errorf("cannot create foreign keys on a single table information database")
}

// GetDeclaredForeignKeys implements sql.ForeignKeyTable.
func (db *SingleTableInfoDatabase) GetDeclaredForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	fks := make([]sql.ForeignKeyConstraint, len(db.foreignKeys))
	for i, fk := range db.foreignKeys {
		if !fk.IsResolved() {
			fks[i] = sql.ForeignKeyConstraint{
				Name:           fk.Name,
				Database:       ctx.GetCurrentDatabase(),
				Table:          fk.TableName,
				Columns:        fk.UnresolvedFKDetails.TableColumns,
				ParentDatabase: ctx.GetCurrentDatabase(),
				ParentTable:    fk.ReferencedTableName,
				ParentColumns:  fk.UnresolvedFKDetails.ReferencedTableColumns,
				OnUpdate:       toReferentialAction(fk.OnUpdate),
				OnDelete:       toReferentialAction(fk.OnDelete),
			}
			continue
		}
		if parentSch, ok := db.parentSchs[fk.ReferencedTableName]; ok {
			var err error
			fks[i], err = toForeignKeyConstraint(fk, ctx.GetCurrentDatabase(), db.sch, parentSch)
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

// GetReferencedForeignKeys implements sql.ForeignKeyTable.
func (db *SingleTableInfoDatabase) GetReferencedForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	return nil, nil
}

// AddForeignKey implements sql.ForeignKeyTable.
func (db *SingleTableInfoDatabase) AddForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	return fmt.Errorf("cannot create foreign keys on a single table information database")
}

// DropForeignKey implements sql.ForeignKeyTable.
func (db *SingleTableInfoDatabase) DropForeignKey(ctx *sql.Context, fkName string) error {
	return fmt.Errorf("cannot create foreign keys on a single table information database")
}

// UpdateForeignKey implements sql.ForeignKeyTable.
func (db *SingleTableInfoDatabase) UpdateForeignKey(ctx *sql.Context, fkName string, fk sql.ForeignKeyConstraint) error {
	return fmt.Errorf("cannot create foreign keys on a single table information database")
}

// GetForeignKeyUpdater implements sql.ForeignKeyTable.
func (db *SingleTableInfoDatabase) GetForeignKeyUpdater(ctx *sql.Context) sql.ForeignKeyUpdater {
	return nil
}

// WithIndexLookup implements sql.IndexedTable.
func (db *SingleTableInfoDatabase) WithIndexLookup(sql.IndexLookup) sql.Table {
	return db
}

// GetIndexes implements sql.IndexedTable.
func (db *SingleTableInfoDatabase) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	var sqlIndexes []sql.Index
	for _, idx := range db.sch.Indexes().AllIndexes() {
		cols := make([]schema.Column, idx.Count())
		for i, tag := range idx.IndexedColumnTags() {
			cols[i], _ = idx.GetColumn(tag)
		}
		sqlIndexes = append(sqlIndexes, &fmtIndex{
			id:        idx.Name(),
			db:        db.Name(),
			tbl:       db.tableName,
			cols:      cols,
			unique:    idx.IsUnique(),
			generated: false,
			comment:   idx.Comment(),
		})
	}
	return sqlIndexes, nil
}

func (db *SingleTableInfoDatabase) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
	return checksInSchema(db.sch), nil
}

func (db *SingleTableInfoDatabase) IsTemporary() bool {
	return false
}

func (db *SingleTableInfoDatabase) NumRows(context *sql.Context) (uint64, error) {
	// TODO: to answer this accurately, we need the table as well as the schema
	return 0, nil
}

func (db *SingleTableInfoDatabase) DataLength(ctx *sql.Context) (uint64, error) {
	// TODO: to answer this accurately, we need the table as well as the schema
	return 0, nil
}

func (db *SingleTableInfoDatabase) PrimaryKeySchema() sql.PrimaryKeySchema {
	sqlSch, err := sqlutil.FromDoltSchema(db.tableName, db.sch)
	if err != nil {
	}
	return sqlSch
}

// fmtIndex is used for CREATE TABLE statements only.
type fmtIndex struct {
	id  string
	db  string
	tbl string

	cols      []schema.Column
	unique    bool
	generated bool
	comment   string
}

// ID implements sql.Index
func (idx fmtIndex) ID() string {
	return idx.id
}

// Database implements sql.Index
func (idx fmtIndex) Database() string {
	return idx.db
}

// Table implements sql.Index
func (idx fmtIndex) Table() string {
	return idx.tbl
}

// Expressions implements sql.Index
func (idx fmtIndex) Expressions() []string {
	strs := make([]string, len(idx.cols))
	for i, col := range idx.cols {
		strs[i] = idx.tbl + "." + col.Name
	}
	return strs
}

// IsUnique implements sql.Index
func (idx fmtIndex) IsUnique() bool {
	return idx.unique
}

// Comment implements sql.Index
func (idx fmtIndex) Comment() string {
	return idx.comment
}

// IndexType implements sql.Index
func (idx fmtIndex) IndexType() string {
	return "BTREE"
}

// IsGenerated implements sql.Index
func (idx fmtIndex) IsGenerated() bool {
	return idx.generated
}

// NewLookup implements sql.Index
func (idx fmtIndex) NewLookup(ctx *sql.Context, ranges ...sql.Range) (sql.IndexLookup, error) {
	panic("unimplemented")
}

// ColumnExpressionTypes implements sql.Index
func (idx fmtIndex) ColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	panic("unimplemented")
}
