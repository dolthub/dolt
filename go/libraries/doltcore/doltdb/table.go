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
	"io"
	"math"
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate/sequences"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrNoConflictsResolved = errors.New("no conflicts resolved")

// IsValidIdentifier is used to validate identifiers. Defaults to MySQL rules.
// Doltgres overrides this to use Postgres rules (which allow supplementary Unicode).
var IsValidIdentifier = IsValidMySqlIdentifier

// IsValidTableName checks if name is a valid identifier, and doesn't end with space characters
func IsValidTableName(name string) bool {
	if len(name) == 0 || unicode.IsSpace(rune(name[len(name)-1])) {
		return false
	}
	return IsValidIdentifier(name)
}

// IsValidMySqlIdentifier returns true according to MySQL's quoted identifier rules.
// Docs here: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
func IsValidMySqlIdentifier(name string) bool {
	// Ignore all leading digits
	if len(name) == 0 {
		return false
	}
	for _, c := range name {
		if c == 0x0000 || c > 0xFFFF {
			return false
		}
	}
	return true
}

// Table is a struct which holds row data, as well as a reference to its schema.
type Table struct {
	table            durable.Table
	overriddenSchema schema.Schema
}

var _ sequences.SequencedRelation[*Table, uint64, AutoIncrementState] = &Table{}

// NewTable creates a durable object which stores row data, index data, and schema.
func NewTable(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema, rows durable.Index, indexes durable.IndexSet, autoIncVal types.Value) (*Table, error) {
	dt, err := durable.NewTable(ctx, vrw, ns, sch, rows, indexes, autoIncVal)
	if err != nil {
		return nil, err
	}
	return &Table{table: dt}, nil
}

func NewEmptyTable(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, sch schema.Schema) (*Table, error) {
	rows, err := durable.NewEmptyPrimaryIndex(ctx, vrw, ns, sch)
	if err != nil {
		return nil, err
	}
	indexes, err := durable.NewIndexSetWithEmptyIndexes(ctx, vrw, ns, sch)
	if err != nil {
		return nil, err
	}

	dt, err := durable.NewTable(ctx, vrw, ns, sch, rows, indexes, nil)
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

func (t *Table) NodeStore() tree.NodeStore {
	return durable.NodeStoreFromTable(t.table)
}

// OverrideSchema sets |sch| as the schema for this table, causing rows from this table to be transformed
// into that schema when they are read from this table.
func (t *Table) OverrideSchema(sch schema.Schema) {
	t.overriddenSchema = sch
}

// GetOverriddenSchema returns the overridden schema if one has been set, otherwise it returns nil.
func (t *Table) GetOverriddenSchema() schema.Schema {
	return t.overriddenSchema
}

// HasConflicts returns true if this table contains merge conflicts.
func (t *Table) HasConflicts(ctx context.Context) (bool, error) {
	art, err := t.GetArtifacts(ctx)
	if err != nil {
		return false, err
	}

	return art.HasConflicts(ctx)
}

// GetArtifacts returns the merge artifacts for this table.
func (t *Table) GetArtifacts(ctx context.Context) (durable.ArtifactIndex, error) {
	return t.table.GetArtifacts(ctx)
}

// SetArtifacts sets the merge artifacts for this table.
func (t *Table) SetArtifacts(ctx context.Context, artifacts durable.ArtifactIndex) (*Table, error) {
	table, err := t.table.SetArtifacts(ctx, artifacts)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// NumRowsInConflict returns the number of rows with merge conflicts for this table.
func (t *Table) NumRowsInConflict(ctx context.Context) (uint64, error) {
	artIdx, err := t.table.GetArtifacts(ctx)
	if err != nil {
		return 0, err
	}
	return artIdx.ConflictCount(ctx)
}

// NumConstraintViolations returns the number of constraint violations for this table.
func (t *Table) NumConstraintViolations(ctx context.Context) (uint64, error) {
	artIdx, err := t.table.GetArtifacts(ctx)
	if err != nil {
		return 0, err
	}
	return artIdx.ConstraintViolationCount(ctx)
}

// ClearConflicts deletes all merge conflicts for this table.
func (t *Table) ClearConflicts(ctx context.Context) (*Table, error) {
	return t.clearArtifactConflicts(ctx)
}

func (t *Table) clearArtifactConflicts(ctx context.Context) (*Table, error) {
	artIdx, err := t.table.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	artIdx, err = artIdx.ClearConflicts(ctx)
	if err != nil {
		return nil, err
	}
	table, err := t.table.SetArtifacts(ctx, artIdx)
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// GetConflictSchemas returns the merge conflict schemas for this table.
func (t *Table) GetConflictSchemas(ctx context.Context, tblName TableName) (base, sch, mergeSch schema.Schema, err error) {
	return t.getProllyConflictSchemas(ctx, tblName)
}

// The conflict schema is implicitly determined based on the first conflict in the artifacts table.
// For now, we will enforce that all conflicts in the artifacts table must have the same schema set (base, ours, theirs).
// In the future, we may be able to display conflicts in a way that allows different conflict schemas to coexist.
func (t *Table) getProllyConflictSchemas(ctx context.Context, tblName TableName) (base, sch, mergeSch schema.Schema, err error) {
	arts, err := t.GetArtifacts(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	ourSch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	if has, err := arts.HasConflicts(ctx); err != nil {
		return nil, nil, nil, err
	} else if !has {
		return ourSch, ourSch, ourSch, nil
	}

	m := durable.ProllyMapFromArtifactIndex(arts)

	itr, err := m.IterAllConflicts(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	art, err := itr.Next(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	baseTbl, baseOk, err := tableFromRootIsh(ctx, t.ValueReadWriter(), t.NodeStore(), art.Metadata.BaseRootIsh, tblName)
	if err != nil {
		return nil, nil, nil, err
	}
	theirTbl, theirOK, err := tableFromRootIsh(ctx, t.ValueReadWriter(), t.NodeStore(), art.TheirRootIsh, tblName)
	if err != nil {
		return nil, nil, nil, err
	}
	if !theirOK {
		return nil, nil, nil, fmt.Errorf("could not find tbl %s in right root value", tblName)
	}

	theirSch, err := theirTbl.GetSchema(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	// If the table does not exist in the ancestor, pretend it existed and that
	// it was completely empty.
	if !baseOk {
		if schema.SchemasAreEqual(ourSch, theirSch) {
			return ourSch, ourSch, theirSch, nil
		} else {
			return nil, nil, nil, fmt.Errorf("expected our schema to equal their schema since the table did not exist in the ancestor")
		}
	}

	baseSch, err := baseTbl.GetSchema(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return baseSch, ourSch, theirSch, nil
}

func tableFromRootIsh(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, h hash.Hash, tblName TableName) (*Table, bool, error) {
	rv, err := LoadRootValueFromRootIshAddr(ctx, vrw, ns, h)
	if err != nil {
		return nil, false, err
	}
	tbl, ok, err := rv.GetTable(ctx, tblName)
	if err != nil {
		return nil, false, err
	}
	return tbl, ok, nil
}

// GetSchema returns the schema.Schema for this Table.
func (t *Table) GetSchema(ctx context.Context) (schema.Schema, error) {
	return t.table.GetSchema(ctx)
}

// GetSchemaHash returns the hash of this table's schema.
func (t *Table) GetSchemaHash(ctx context.Context) (hash.Hash, error) {
	return t.table.GetSchemaHash(ctx)
}

func SchemaHashesEqual(ctx context.Context, t1, t2 *Table) (bool, error) {
	t1Hash, err := t1.GetSchemaHash(ctx)
	if err != nil {
		return false, err
	}
	t2Hash, err := t2.GetSchemaHash(ctx)
	if err != nil {
		return false, err
	}
	return t1Hash == t2Hash, nil
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

// GetRowData retrieves the underlying map which is a map from a primary key to a list of field values.
func (t *Table) GetRowData(ctx context.Context) (durable.Index, error) {
	return t.table.GetTableRows(ctx)
}

func (t *Table) GetRowDataWithDescriptors(ctx context.Context, kd, vd *val.TupleDesc) (durable.Index, error) {
	return t.table.GetTableRowsWithDescriptors(ctx, kd, vd)
}

// GetRowDataHash returns the hash.Hash of the row data index.
func (t *Table) GetRowDataHash(ctx context.Context) (hash.Hash, error) {
	idx, err := t.table.GetTableRows(ctx)
	if err != nil {
		return hash.Hash{}, err
	}
	return idx.HashOf()
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

	return indexes.GetIndex(ctx, sch, nil, indexName)
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

// HasSequenceState returns whether the table has an AUTO_INCREMENT value.
func (t *Table) HasSequenceState(ctx context.Context) (bool, error) {
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return false, err
	}
	return schema.HasAutoIncrement(sch), nil
}

// GetSequenceState implements SequencedRelation
func (t *Table) GetSequenceState(ctx context.Context) (AutoIncrementState, error) {
	return t.GetAutoIncrementValue(ctx)
}

// GetAutoIncrementValue returns the current AUTO_INCREMENT value for this table.
func (t *Table) GetAutoIncrementValue(ctx context.Context) (AutoIncrementState, error) {
	currentValue, err := t.table.GetAutoIncrement(ctx)
	if err != nil {
		return 0, err
	}
	return AutoIncrementState(currentValue), nil
}

func (t *Table) GetSequenceSqlType(ctx context.Context) (sql.Type, bool, error) {
	sch, err := t.table.GetSchema(ctx)
	if err != nil {
		return nil, false, err
	}

	aiCol, ok := schema.GetAutoIncrementColumn(sch)
	if !ok {
		return nil, false, nil
	}

	return aiCol.TypeInfo.ToSqlType(), true, nil
}

// SetAutoIncrementValue sets the current AUTO_INCREMENT value for this table. This method does not verify that the
// value given is greater than current table values. Setting it lower than current table values will result in
// incorrect key generation on future inserts, causing duplicate key errors.
func (t *Table) SetAutoIncrementValue(ctx context.Context, val AutoIncrementState) (*Table, error) {
	table, err := t.table.SetAutoIncrement(ctx, val.CurrentValue())
	if err != nil {
		return nil, err
	}
	return &Table{table: table}, nil
}

// SetSequenceState implements sequences.SequencedRelation
func (t *Table) SetSequenceState(ctx context.Context, val AutoIncrementState) (*Table, error) {
	return t.SetAutoIncrementValue(ctx, val)
}

// SetSequenceState implements sequences.SequencedRelation
func (t *Table) TrySetSequenceState(ctx *sql.Context, newAutoIncVal AutoIncrementState) (*Table, bool, error) {
	currentMax, err := getMaxAutoIncrementValue(ctx, t)
	if err != nil {
		return nil, false, err
	}
	currentMaxVal := AutoIncrementState(currentMax)

	if !newAutoIncVal.GreaterThan(currentMaxVal) {
		return t, false, nil
	}

	newTable, err := t.SetSequenceState(ctx, newAutoIncVal)
	return newTable, true, err
}

// CoerceAutoIncrementValue converts |val| into an AUTO_INCREMENT sequence value
func CoerceAutoIncrementValue(ctx *sql.Context, val interface{}) (uint64, error) {
	switch typ := val.(type) {
	case float32:
		val = math.Round(float64(typ))
	case float64:
		val = math.Round(typ)
	}

	var err error
	val, _, err = gmstypes.Uint64.Convert(ctx, val)
	if err != nil {
		return 0, err
	}
	if val == nil || val == uint64(0) {
		return 0, nil
	}
	return val.(uint64), nil
}

type AutoIncrementState uint64

func (s AutoIncrementState) Next() (aiVal uint64, ok bool, nextState AutoIncrementState, err error) {
	if s == math.MaxUint64 {
		return uint64(math.MaxUint64), false, s, nil
	}
	return uint64(s), true, s + 1, nil
}

func (s AutoIncrementState) CurrentValue() uint64 {
	return uint64(s)
}

func (s AutoIncrementState) WithValue(v uint64) AutoIncrementState {
	return AutoIncrementState(v)
}

func (s AutoIncrementState) WithSQLValue(ctx *sql.Context, v interface{}) (AutoIncrementState, error) {
	given, err := CoerceAutoIncrementValue(ctx, v)
	if err != nil {
		return s, err
	}
	return AutoIncrementState(given), nil
}

func (s AutoIncrementState) GreaterThan(other AutoIncrementState) bool {
	return s > other
}

func (s AutoIncrementState) Merge(other AutoIncrementState) (AutoIncrementState, bool) {
	if s > other {
		return s, true
	}
	return other, true
}

func (s AutoIncrementState) AtEnd() bool {
	return s == math.MaxUint64
}

// getMaxAutoIncrementValue gets the highest value in a table's AUTO INCREMENT column
func getMaxAutoIncrementValue(ctx *sql.Context, table *Table) (uint64, error) {
	// First, establish whether to update this table based on the given value and its current max value.
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return 0, err
	}

	aiCol, ok := schema.GetAutoIncrementColumn(sch)
	if !ok {
		return 0, nil
	}

	var indexData durable.Index
	aiIndex, ok := sch.Indexes().GetIndexByColumnNames(aiCol.Name)
	if ok {
		indexes, err := table.GetIndexSet(ctx)
		if err != nil {
			return 0, err
		}

		indexData, err = indexes.GetIndex(ctx, sch, nil, aiIndex.Name())
		if err != nil {
			return 0, err
		}
	} else {
		indexData, err = table.GetRowData(ctx)
		if err != nil {
			return 0, err
		}
	}

	maxValue, err := getMaxIndexValue(ctx, indexData)
	if err != nil {
		return 0, err
	}
	return CoerceAutoIncrementValue(ctx, maxValue)
}

// getMaxIndexValue reads the highest value for the first column in an index.
func getMaxIndexValue(ctx *sql.Context, indexData durable.Index) (interface{}, error) {
	idx, err := durable.ProllyMapFromIndex(indexData)
	if err != nil {
		return 0, err
	}

	iter, err := idx.IterAllReverse(ctx)
	if err != nil {
		return 0, err
	}

	kd, _ := idx.Descriptors()
	k, _, err := iter.Next(ctx)
	if err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	// TODO: is the auto-inc column always the first column in the index?
	return tree.GetField(ctx, kd, 0, k, idx.NodeStore())
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

func (t *Table) DebugString(ctx context.Context, ns tree.NodeStore) string {
	return t.table.DebugString(ctx, ns)
}
