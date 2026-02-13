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
	"unicode"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrNoConflictsResolved = errors.New("no conflicts resolved")

// IsValidTableName checks if name is a valid identifier, and doesn't end with space characters
func IsValidTableName(name string) bool {
	if len(name) == 0 || unicode.IsSpace(rune(name[len(name)-1])) {
		return false
	}
	return IsValidIdentifier(name)
}

// IsValidIdentifier returns true according to MySQL's quoted identifier rules.
// Docs here: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
func IsValidIdentifier(name string) bool {
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
	if t.Format() == types.Format_DOLT {
		art, err := t.GetArtifacts(ctx)
		if err != nil {
			return false, err
		}

		return art.HasConflicts(ctx)
	}

	panic("Unsupported format: " + t.Format().VersionString())
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
	if t.Format() == types.Format_DOLT {
		artIdx, err := t.table.GetArtifacts(ctx)
		if err != nil {
			return 0, err
		}
		return artIdx.ConflictCount(ctx)
	}

	panic("Unsupported format: " + t.Format().VersionString())
}

// NumConstraintViolations returns the number of constraint violations for this table.
func (t *Table) NumConstraintViolations(ctx context.Context) (uint64, error) {
	if t.Format() == types.Format_DOLT {
		artIdx, err := t.table.GetArtifacts(ctx)
		if err != nil {
			return 0, err
		}
		return artIdx.ConstraintViolationCount(ctx)
	}

	panic("Unsupported format: " + t.Format().VersionString())
}

// ClearConflicts deletes all merge conflicts for this table.
func (t *Table) ClearConflicts(ctx context.Context) (*Table, error) {
	if t.Format() == types.Format_DOLT {
		return t.clearArtifactConflicts(ctx)
	}

	panic("Unsupported format: " + t.Format().VersionString())
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
	if t.Format() == types.Format_DOLT {
		return t.getProllyConflictSchemas(ctx, tblName)
	}

	panic("Unsupported format: " + t.Format().VersionString())
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

// GetAutoIncrementValue returns the current AUTO_INCREMENT value for this table.
func (t *Table) GetAutoIncrementValue(ctx context.Context) (uint64, error) {
	return t.table.GetAutoIncrement(ctx)
}

// SetAutoIncrementValue sets the current AUTO_INCREMENT value for this table. This method does not verify that the
// value given is greater than current table values. Setting it lower than current table values will result in
// incorrect key generation on future inserts, causing duplicate key errors.
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

func (t *Table) DebugString(ctx context.Context, ns tree.NodeStore) string {
	return t.table.DebugString(ctx, ns)
}
