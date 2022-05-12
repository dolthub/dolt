// Copyright 2019 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// TableNameRegexStr is the regular expression that valid tables must match.
	TableNameRegexStr = `^[a-zA-Z]{1}$|^[a-zA-Z_]+[-_0-9a-zA-Z]*[0-9a-zA-Z]+$`
	// ForeignKeyNameRegexStr is the regular expression that valid foreign keys must match.
	// From the unquoted identifiers: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	// We also allow the '-' character from quoted identifiers.
	ForeignKeyNameRegexStr = `^[-$_0-9a-zA-Z]+$`
	// IndexNameRegexStr is the regular expression that valid indexes must match.
	// From the unquoted identifiers: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	// We also allow the '-' character from quoted identifiers.
	IndexNameRegexStr = `^[-$_0-9a-zA-Z]+$`
)

var (
	tableNameRegex      = regexp.MustCompile(TableNameRegexStr)
	foreignKeyNameRegex = regexp.MustCompile(ForeignKeyNameRegexStr)
	indexNameRegex      = regexp.MustCompile(IndexNameRegexStr)

	ErrNoConflictsResolved = errors.New("no conflicts resolved")
)

// IsValidTableName returns true if the name matches the regular expression TableNameRegexStr.
// Table names must be composed of 1 or more letters and non-initial numerals, as well as the characters _ and -
func IsValidTableName(name string) bool {
	// Ignore all leading digits
	name = strings.TrimLeftFunc(name, unicode.IsDigit)
	return tableNameRegex.MatchString(name)
}

// IsValidForeignKeyName returns true if the name matches the regular expression ForeignKeyNameRegexStr.
func IsValidForeignKeyName(name string) bool {
	return foreignKeyNameRegex.MatchString(name)
}

// IsValidIndexName returns true if the name matches the regular expression IndexNameRegexStr.
func IsValidIndexName(name string) bool {
	return indexNameRegex.MatchString(name)
}

// Table is a struct which holds row data, as well as a reference to its schema.
type Table struct {
	table durable.Table
}

// NewNomsTable creates a noms Struct which stores row data, index data, and schema.
func NewNomsTable(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rows types.Map, indexes durable.IndexSet, autoIncVal types.Value) (*Table, error) {
	dt, err := durable.NewNomsTable(ctx, vrw, sch, rows, indexes, autoIncVal)
	if err != nil {
		return nil, err
	}

	return &Table{table: dt}, nil
}

// NewTable creates a durable object which stores row data, index data, and schema.
func NewTable(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rows durable.Index, indexes durable.IndexSet, autoIncVal types.Value) (*Table, error) {
	dt, err := durable.NewTable(ctx, vrw, sch, rows, indexes, autoIncVal)
	if err != nil {
		return nil, err
	}

	return &Table{table: dt}, nil
}

// Format returns the NomsBinFormat for this table.
func (t *Table) Format() *types.NomsBinFormat {
	return t.ValueReadWriter().Format()
}

// ValueReadWriter returns the ValueReadWriter for this table.
func (t *Table) ValueReadWriter() types.ValueReadWriter {
	return durable.VrwFromTable(t.table)
}

// SetConflicts sets the merge conflicts for this table.
func (t *Table) SetConflicts(ctx context.Context, schemas conflict.ConflictSchema, conflictData durable.ConflictIndex) (*Table, error) {
	table, err := t.table.SetConflicts(ctx, schemas, conflictData)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// GetConflicts returns a map built from ValueReadWriter when there are no conflicts in table.
func (t *Table) GetConflicts(ctx context.Context) (conflict.ConflictSchema, durable.ConflictIndex, error) {
	return t.table.GetConflicts(ctx)
}

// HasConflicts returns true if this table contains merge conflicts.
func (t *Table) HasConflicts(ctx context.Context) (bool, error) {
	return t.table.HasConflicts(ctx)
}

// NumRowsInConflict returns the number of rows with merge conflicts for this table.
func (t *Table) NumRowsInConflict(ctx context.Context) (uint64, error) {
	ok, err := t.table.HasConflicts(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}

	_, cons, err := t.table.GetConflicts(ctx)
	if err != nil {
		return 0, err
	}

	return cons.Count(), nil
}

// ClearConflicts deletes all merge conflicts for this table.
func (t *Table) ClearConflicts(ctx context.Context) (*Table, error) {
	table, err := t.table.ClearConflicts(ctx)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// GetConflictSchemas returns the merge conflict schemas for this table.
func (t *Table) GetConflictSchemas(ctx context.Context) (base, sch, mergeSch schema.Schema, err error) {
	cs, _, err := t.table.GetConflicts(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return cs.Base, cs.Schema, cs.MergeSchema, nil
}

// GetConstraintViolationsSchema returns this table's dolt_constraint_violations system table schema.
func (t *Table) GetConstraintViolationsSchema(ctx context.Context) (schema.Schema, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	typeType, err := typeinfo.FromSqlType(
		sql.MustCreateEnumType([]string{"foreign key", "unique index", "check constraint"}, sql.Collation_Default))
	if err != nil {
		return nil, err
	}
	typeCol, err := schema.NewColumnWithTypeInfo("violation_type", schema.DoltConstraintViolationsTypeTag, typeType, true, "", false, "")
	if err != nil {
		return nil, err
	}
	infoCol, err := schema.NewColumnWithTypeInfo("violation_info", schema.DoltConstraintViolationsInfoTag, typeinfo.JSONType, false, "", false, "")
	if err != nil {
		return nil, err
	}

	colColl := schema.NewColCollection()
	colColl = colColl.Append(typeCol)
	colColl = colColl.Append(sch.GetAllCols().GetColumns()...)
	colColl = colColl.Append(infoCol)
	return schema.SchemaFromCols(colColl)
}

// GetConstraintViolations returns a map of all constraint violations for this table, along with a bool indicating
// whether the table has any violations.
func (t *Table) GetConstraintViolations(ctx context.Context) (types.Map, error) {
	return t.table.GetConstraintViolations(ctx)
}

// SetConstraintViolations sets this table's violations to the given map. If the map is empty, then the constraint
// violations entry on the embedded struct is removed.
func (t *Table) SetConstraintViolations(ctx context.Context, violationsMap types.Map) (*Table, error) {
	table, err := t.table.SetConstraintViolations(ctx, violationsMap)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// GetSchema will retrieve the schema being referenced from the table in noms and unmarshal it.
func (t *Table) GetSchema(ctx context.Context) (schema.Schema, error) {
	return t.table.GetSchema(ctx)
}

// GetSchemaHash returns the hash of this table's schema.
func (t *Table) GetSchemaHash(ctx context.Context) (hash.Hash, error) {
	return t.table.GetSchemaHash(ctx)
}

// UpdateSchema updates the table with the schema given and returns the updated table. The original table is unchanged.
// This method only updates the schema of a table; the row data is unchanged. Schema alterations that require rebuilding
// the table (e.g. adding a column in the middle, adding a new non-null column, adding a column in the middle of a
// schema) must account for these changes separately.
func (t *Table) UpdateSchema(ctx context.Context, sch schema.Schema) (*Table, error) {
	table, err := t.table.SetSchema(ctx, sch)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// HashOf returns the hash of the underlying table struct.
func (t *Table) HashOf() (hash.Hash, error) {
	return t.table.HashOf()
}

// UpdateNomsRows replaces the current row data and returns and updated Table.
// Calls to UpdateNomsRows will not be written to the database.  The root must
// be updated with the updated table, and the root must be committed or written.
func (t *Table) UpdateNomsRows(ctx context.Context, updatedRows types.Map) (*Table, error) {
	table, err := t.table.SetTableRows(ctx, durable.IndexFromNomsMap(updatedRows, t.ValueReadWriter()))
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// UpdateRows replaces the current row data and returns and updated Table.
// Calls to UpdateRows will not be written to the database. The root must
// be updated with the updated table, and the root must be committed or written.
func (t *Table) UpdateRows(ctx context.Context, updatedRows durable.Index) (*Table, error) {
	table, err := t.table.SetTableRows(ctx, updatedRows)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// GetNomsRowData retrieves the underlying map which is a map from a primary key to a list of field values.
func (t *Table) GetNomsRowData(ctx context.Context) (types.Map, error) {
	idx, err := t.table.GetTableRows(ctx)
	if err != nil {
		return types.Map{}, err
	}

	return durable.NomsMapFromIndex(idx), nil
}

// GetRowData retrieves the underlying map which is a map from a primary key to a list of field values.
func (t *Table) GetRowData(ctx context.Context) (durable.Index, error) {
	return t.table.GetTableRows(ctx)
}

// ResolveConflicts resolves conflicts for this table.
func (t *Table) ResolveConflicts(ctx context.Context, pkTuples []types.Value) (invalid, notFound []types.Value, tbl *Table, err error) {
	removed := 0
	conflictSchema, confIdx, err := t.GetConflicts(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	if confIdx.Format() == types.Format_DOLT_1 {
		panic("resolve conflicts not implemented for new storage format")
	}

	confData := durable.NomsMapFromConflictIndex(confIdx)

	confEdit := confData.Edit()
	for _, pkTupleVal := range pkTuples {
		if has, err := confData.Has(ctx, pkTupleVal); err != nil {
			return nil, nil, nil, err
		} else if has {
			removed++
			confEdit.Remove(pkTupleVal)
		} else {
			notFound = append(notFound, pkTupleVal)
		}
	}

	if removed == 0 {
		return invalid, notFound, tbl, ErrNoConflictsResolved
	}

	conflicts, err := confEdit.Map(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	if conflicts.Len() == 0 {
		table, err := t.table.ClearConflicts(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		return invalid, notFound, &Table{table: table}, nil
	}

	table, err := t.table.SetConflicts(ctx, conflictSchema, durable.ConflictIndexFromNomsMap(conflicts, t.ValueReadWriter()))
	if err != nil {
		return nil, nil, nil, err
	}

	return invalid, notFound, &Table{table: table}, nil
}

// GetIndexSet returns the internal index map which goes from index name to a ref of the row data map.
func (t *Table) GetIndexSet(ctx context.Context) (durable.IndexSet, error) {
	return t.table.GetIndexes(ctx)
}

// SetIndexSet replaces the current internal index map, and returns an updated Table.
func (t *Table) SetIndexSet(ctx context.Context, indexes durable.IndexSet) (*Table, error) {
	table, err := t.table.SetIndexes(ctx, indexes)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// GetNomsIndexRowData retrieves the underlying map of an index, in which the primary key consists of all indexed columns.
func (t *Table) GetNomsIndexRowData(ctx context.Context, indexName string) (types.Map, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	indexes, err := t.GetIndexSet(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	idx, err := indexes.GetIndex(ctx, sch, indexName)
	if err != nil {
		return types.EmptyMap, err
	}

	return durable.NomsMapFromIndex(idx), nil
}

// GetIndexRowData retrieves the underlying map of an index, in which the primary key consists of all indexed columns.
func (t *Table) GetIndexRowData(ctx context.Context, indexName string) (durable.Index, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	indexes, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	return indexes.GetIndex(ctx, sch, indexName)
}

// SetIndexRows replaces the current row data for the given index and returns an updated Table.
func (t *Table) SetIndexRows(ctx context.Context, indexName string, idx durable.Index) (*Table, error) {
	indexes, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	indexes, err = indexes.PutIndex(ctx, indexName, idx)
	if err != nil {
		return nil, err
	}

	return t.SetIndexSet(ctx, indexes)
}

// SetNomsIndexRows replaces the current row data for the given index and returns an updated Table.
func (t *Table) SetNomsIndexRows(ctx context.Context, indexName string, idx types.Map) (*Table, error) {
	indexes, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	indexes, err = indexes.PutNomsIndex(ctx, indexName, idx)
	if err != nil {
		return nil, err
	}

	return t.SetIndexSet(ctx, indexes)
}

// DeleteIndexRowData removes the underlying map of an index, along with its key entry. This should only be used
// when removing an index altogether. If the intent is to clear an index's data, then use SetNomsIndexRows with
// an empty map.
func (t *Table) DeleteIndexRowData(ctx context.Context, indexName string) (*Table, error) {
	indexes, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	indexes, err = indexes.DropIndex(ctx, indexName)
	if err != nil {
		return nil, err
	}

	return t.SetIndexSet(ctx, indexes)
}

// RenameIndexRowData changes the name for the index data. Does not verify that the new name is unoccupied. If the old
// name does not exist, then this returns the called table without error.
func (t *Table) RenameIndexRowData(ctx context.Context, oldIndexName, newIndexName string) (*Table, error) {
	indexes, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	indexes, err = indexes.RenameIndex(ctx, oldIndexName, newIndexName)
	if err != nil {
		return nil, err
	}

	return t.SetIndexSet(ctx, indexes)
}

// VerifyIndexRowData verifies that the index with the given name's data matches what the index expects.
func (t *Table) VerifyIndexRowData(ctx context.Context, indexName string) error {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return err
	}

	index := sch.Indexes().GetByName(indexName)
	if index == nil {
		return fmt.Errorf("index `%s` does not exist", indexName)
	}

	indexes, err := t.GetIndexSet(ctx)
	if err != nil {
		return err
	}

	idx, err := indexes.GetIndex(ctx, sch, indexName)
	if err != nil {
		return err
	}

	im := durable.NomsMapFromIndex(idx)
	iter, err := im.Iterator(ctx)
	if err != nil {
		return err
	}

	return index.VerifyMap(ctx, iter, im.Format())
}

// GetAutoIncrementValue returns the current AUTO_INCREMENT value for this table.
func (t *Table) GetAutoIncrementValue(ctx context.Context) (uint64, error) {
	return t.table.GetAutoIncrement(ctx)
}

// SetAutoIncrementValue sets the current AUTO_INCREMENT value for this table.
func (t *Table) SetAutoIncrementValue(ctx context.Context, val uint64) (*Table, error) {
	table, err := t.table.SetAutoIncrement(ctx, val)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// AddColumnToRows adds the column named to row data as necessary and returns the resulting table.
func (t *Table) AddColumnToRows(ctx context.Context, newCol string, newSchema schema.Schema) (*Table, error) {
	idx, err := t.table.GetTableRows(ctx)
	if err != nil {
		return nil, err
	}

	newIdx, err := idx.AddColumnToRows(ctx, newCol, newSchema)
	if err != nil {
		return nil, err
	}

	newTable, err := t.table.SetTableRows(ctx, newIdx)
	if err != nil {
		return nil, err
	}

	return &Table{table: newTable}, nil
}

func (t *Table) DebugString(ctx context.Context) string {
	return t.table.DebugString(ctx)
}
