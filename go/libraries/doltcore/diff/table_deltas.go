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

package diff

import (
	"context"
	"fmt"
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/prolly/tree"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type TableDiffType int

const (
	AddedTable TableDiffType = iota
	ModifiedTable
	RenamedTable
	RemovedTable
)

// TableDelta represents the change of a single table between two roots.
// FromFKs and ToFKs contain Foreign Keys that constrain columns in this table,
// they do not contain Foreign Keys that reference this table.
type TableDelta struct {
	FromName         string
	ToName           string
	FromTable        *doltdb.Table
	ToTable          *doltdb.Table
	FromNodeStore    tree.NodeStore
	ToNodeStore      tree.NodeStore
	FromVRW          types.ValueReadWriter
	ToVRW            types.ValueReadWriter
	FromSch          schema.Schema
	ToSch            schema.Schema
	FromFks          []doltdb.ForeignKey
	ToFks            []doltdb.ForeignKey
	ToFksParentSch   map[string]schema.Schema
	FromFksParentSch map[string]schema.Schema
}

// GetStagedUnstagedTableDeltas represents staged and unstaged changes as TableDelta slices.
func GetStagedUnstagedTableDeltas(ctx context.Context, roots doltdb.Roots) (staged, unstaged []TableDelta, err error) {
	staged, err = GetTableDeltas(ctx, roots.Head, roots.Staged)
	if err != nil {
		return nil, nil, err
	}

	unstaged, err = GetTableDeltas(ctx, roots.Staged, roots.Working)
	if err != nil {
		return nil, nil, err
	}

	return staged, unstaged, nil
}

// GetTableDeltas returns a slice of TableDelta objects for each table that changed between fromRoot and toRoot.
// It matches tables across roots by finding Schemas with Column tags in common.
func GetTableDeltas(ctx context.Context, fromRoot, toRoot *doltdb.RootValue) (deltas []TableDelta, err error) {
	fromVRW := fromRoot.VRW()
	fromNS := fromRoot.NodeStore()
	toVRW := toRoot.VRW()
	toNS := toRoot.NodeStore()

	fromDeltas := make([]TableDelta, 0)
	err = fromRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		c, err := fromRoot.GetForeignKeyCollection(ctx)
		if err != nil {
			return true, err
		}
		fks, _ := c.KeysForTable(name)
		parentSchs, err := getFkParentSchs(ctx, fromRoot, fks...)
		if err != nil {
			return false, err
		}

		fromDeltas = append(fromDeltas, TableDelta{
			FromName:         name,
			FromTable:        tbl,
			FromSch:          sch,
			FromFks:          fks,
			FromFksParentSch: parentSchs,
			FromVRW:          fromVRW,
			FromNodeStore:    fromNS,
			ToVRW:            toVRW,
			ToNodeStore:      toNS,
		})
		return
	})
	if err != nil {
		return nil, err
	}

	toDeltas := make([]TableDelta, 0)

	err = toRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		c, err := toRoot.GetForeignKeyCollection(ctx)
		if err != nil {
			return true, err
		}

		fks, _ := c.KeysForTable(name)
		parentSchs, err := getFkParentSchs(ctx, toRoot, fks...)
		if err != nil {
			return false, err
		}

		toDeltas = append(toDeltas, TableDelta{
			ToName:         name,
			ToTable:        tbl,
			ToSch:          sch,
			ToFks:          fks,
			ToFksParentSch: parentSchs,
			FromVRW:        fromVRW,
			FromNodeStore:  fromNS,
			ToVRW:          toVRW,
			ToNodeStore:    toNS,
		})
		return
	})
	if err != nil {
		return nil, err
	}

	deltas = matchTableDeltas(fromDeltas, toDeltas)
	deltas, err = filterUnmodifiedTableDeltas(deltas)
	if err != nil {
		return nil, err
	}

	return deltas, nil
}

func getFkParentSchs(ctx context.Context, root *doltdb.RootValue, fks ...doltdb.ForeignKey) (map[string]schema.Schema, error) {
	schs := make(map[string]schema.Schema)
	for _, toFk := range fks {
		toRefTable, _, ok, err := root.GetTableInsensitive(ctx, toFk.ReferencedTableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue // as the schemas are for display-only, we can skip on any missing parents (they were deleted, etc.)
		}
		toRefSch, err := toRefTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		schs[toFk.ReferencedTableName] = toRefSch
	}
	return schs, nil
}

func filterUnmodifiedTableDeltas(deltas []TableDelta) ([]TableDelta, error) {
	var filtered []TableDelta
	for _, d := range deltas {
		if d.ToTable == nil || d.FromTable == nil {
			// Table was added or dropped
			filtered = append(filtered, d)
			continue
		}

		hasChanges, err := d.HasChanges()
		if err != nil {
			return nil, err
		}

		if hasChanges {
			// Take only modified tables
			filtered = append(filtered, d)
		}
	}

	return filtered, nil
}

func matchTableDeltas(fromDeltas, toDeltas []TableDelta) (deltas []TableDelta) {
	var matchedNames []string
	from := make(map[string]TableDelta, len(fromDeltas))
	for _, f := range fromDeltas {
		from[f.FromName] = f
	}

	to := make(map[string]TableDelta, len(toDeltas))
	for _, t := range toDeltas {
		to[t.ToName] = t
		if _, ok := from[t.ToName]; ok {
			matchedNames = append(matchedNames, t.ToName)
		}
	}

	match := func(t, f TableDelta) TableDelta {
		return TableDelta{
			FromName:         f.FromName,
			ToName:           t.ToName,
			FromTable:        f.FromTable,
			ToTable:          t.ToTable,
			FromSch:          f.FromSch,
			ToSch:            t.ToSch,
			FromFks:          f.FromFks,
			ToFks:            t.ToFks,
			FromFksParentSch: f.FromFksParentSch,
			ToFksParentSch:   t.ToFksParentSch,
		}
	}

	deltas = make([]TableDelta, 0)

	for _, name := range matchedNames {
		t := to[name]
		f := from[name]
		if schemasOverlap(t.ToSch, f.FromSch) {
			matched := match(t, f)
			deltas = append(deltas, matched)
			delete(from, f.FromName)
			delete(to, t.ToName)
		}
	}

	for _, f := range from {
		for _, t := range to {
			if schemasOverlap(f.FromSch, t.ToSch) {
				matched := match(t, f)
				deltas = append(deltas, matched)
				delete(from, f.FromName)
				delete(to, t.ToName)
			}
		}
	}

	// append unmatched TableDeltas
	for _, f := range from {
		deltas = append(deltas, f)
	}
	for _, t := range to {
		deltas = append(deltas, t)
	}

	return deltas
}

func schemasOverlap(from, to schema.Schema) bool {
	f := set.NewUint64Set(from.GetAllCols().Tags)
	t := set.NewUint64Set(to.GetAllCols().Tags)
	return f.Intersection(t).Size() > 0
}

// IsAdd returns true if the table was added between the fromRoot and toRoot.
func (td TableDelta) IsAdd() bool {
	return td.FromTable == nil && td.ToTable != nil
}

// IsDrop returns true if the table was dropped between the fromRoot and toRoot.
func (td TableDelta) IsDrop() bool {
	return td.FromTable != nil && td.ToTable == nil
}

// IsRename return true if the table was renamed between the fromRoot and toRoot.
func (td TableDelta) IsRename() bool {
	if td.IsAdd() || td.IsDrop() {
		return false
	}
	return td.FromName != td.ToName
}

// HasHashChanged returns true if the hash of the table content has changed between
// the fromRoot and toRoot.
func (td TableDelta) HasHashChanged() (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	toHash, err := td.ToTable.HashOf()
	if err != nil {
		return false, err
	}

	fromHash, err := td.FromTable.HashOf()
	if err != nil {
		return false, err
	}

	return !toHash.Equal(fromHash), nil
}

// HasSchemaChanged returns true if the table schema has changed between the
// fromRoot and toRoot.
func (td TableDelta) HasSchemaChanged(ctx context.Context) (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	fromSchemaHash, err := td.FromTable.GetSchemaHash(ctx)
	if err != nil {
		return false, err
	}

	toSchemaHash, err := td.ToTable.GetSchemaHash(ctx)
	if err != nil {
		return false, err
	}

	return fromSchemaHash != toSchemaHash, nil
}

func (td TableDelta) HasPrimaryKeySetChanged() bool {
	return !schema.ArePrimaryKeySetsDiffable(td.Format(), td.FromSch, td.ToSch)
}

func (td TableDelta) HasChanges() (bool, error) {
	hashChanged, err := td.HasHashChanged()
	if err != nil {
		return false, err
	}

	return td.HasFKChanges() || td.IsRename() || td.HasPrimaryKeySetChanged() || hashChanged, nil
}

// CurName returns the most recent name of the table.
func (td TableDelta) CurName() string {
	if td.ToName != "" {
		return td.ToName
	}
	return td.FromName
}

func (td TableDelta) HasFKChanges() bool {
	return !fkSlicesAreEqual(td.FromFks, td.ToFks)
}

// GetSchemas returns the table's schema at the fromRoot and toRoot, or schema.Empty if the table did not exist.
func (td TableDelta) GetSchemas(ctx context.Context) (from, to schema.Schema, err error) {
	if td.FromSch == nil {
		td.FromSch = schema.EmptySchema
	}
	if td.ToSch == nil {
		td.ToSch = schema.EmptySchema
	}
	return td.FromSch, td.ToSch, nil
}

// Format returns the format of the tables in this delta.
func (td TableDelta) Format() *types.NomsBinFormat {
	if td.FromTable != nil {
		return td.FromTable.Format()
	}
	return td.ToTable.Format()
}

func (td TableDelta) IsKeyless(ctx context.Context) (bool, error) {
	f, t, err := td.GetSchemas(ctx)
	if err != nil {
		return false, err
	}

	// nil table is neither keyless nor keyed
	from, to := schema.IsKeyless(f), schema.IsKeyless(t)
	if td.FromTable == nil {
		return to, nil
	} else if td.ToTable == nil {
		return from, nil
	} else {
		if from && to {
			return true, nil
		} else if !from && !to {
			return false, nil
		} else {
			return false, fmt.Errorf("mismatched keyless and keyed schemas for table %s", td.CurName())
		}
	}
}

// GetRowData returns the table's row data at the fromRoot and toRoot, or an empty map if the table did not exist.
func (td TableDelta) GetRowData(ctx context.Context) (from, to durable.Index, err error) {
	if td.FromTable == nil && td.ToTable == nil {
		return nil, nil, fmt.Errorf("both from and to tables are missing from table delta")
	}

	if td.FromTable != nil {
		from, err = td.FromTable.GetRowData(ctx)
		if err != nil {
			return from, to, err
		}
	} else {
		// If there is no |FromTable| use the |ToTable|'s schema to make the index.
		from, _ = durable.NewEmptyIndex(ctx, td.FromVRW, td.FromNodeStore, td.ToSch)
	}

	if td.ToTable != nil {
		to, err = td.ToTable.GetRowData(ctx)
		if err != nil {
			return from, to, err
		}
	} else {
		// If there is no |ToTable| use the |FromTable|'s schema to make the index.
		to, _ = durable.NewEmptyIndex(ctx, td.ToVRW, td.ToNodeStore, td.FromSch)
	}

	return from, to, nil
}

func fkSlicesAreEqual(from, to []doltdb.ForeignKey) bool {
	if len(from) != len(to) {
		return false
	}

	sort.Slice(from, func(i, j int) bool {
		return from[i].Name < from[j].Name
	})
	sort.Slice(to, func(i, j int) bool {
		return to[i].Name < to[j].Name
	})

	for i := range from {
		if !from[i].DeepEquals(to[i]) {
			return false
		}
	}
	return true
}
