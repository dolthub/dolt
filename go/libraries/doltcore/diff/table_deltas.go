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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type TableDiffType int

const (
	AddedTable TableDiffType = iota
	ModifiedTable
	RenamedTable
	RemovedTable
)

type DocDiffType int

const (
	AddedDoc DocDiffType = iota
	ModifiedDoc
	RemovedDoc
)

type DocDiffs struct {
	NumAdded    int
	NumModified int
	NumRemoved  int
	DocToType   map[string]DocDiffType
	Docs        []string
}

// NewDocDiffs returns DocDiffs for Dolt Docs between two roots.
func NewDocDiffs(ctx context.Context, older *doltdb.RootValue, newer *doltdb.RootValue, docs doltdocs.Docs) (*DocDiffs, error) {
	var added []string
	var modified []string
	var removed []string
	if older != nil {
		if newer == nil {
			a, m, r, err := DocsDiff(ctx, older, nil, docs)
			if err != nil {
				return nil, err
			}
			added = a
			modified = m
			removed = r
		} else {
			a, m, r, err := DocsDiff(ctx, older, newer, docs)
			if err != nil {
				return nil, err
			}
			added = a
			modified = m
			removed = r
		}
	}
	var docNames []string
	docNames = append(docNames, added...)
	docNames = append(docNames, modified...)
	docNames = append(docNames, removed...)
	sort.Strings(docNames)

	docsToType := make(map[string]DocDiffType)
	for _, nt := range added {
		docsToType[nt] = AddedDoc
	}

	for _, nt := range modified {
		docsToType[nt] = ModifiedDoc
	}

	for _, nt := range removed {
		docsToType[nt] = RemovedDoc
	}

	return &DocDiffs{len(added), len(modified), len(removed), docsToType, docNames}, nil
}

// Len returns the number of docs in a DocDiffs
func (nd *DocDiffs) Len() int {
	return len(nd.Docs)
}

// GetDocDiffs retrieves staged and unstaged DocDiffs.
func GetDocDiffs(
	ctx context.Context,
	roots doltdb.Roots,
	drw env.DocsReadWriter,
) (*DocDiffs, *DocDiffs, error) {
	docsOnDisk, err := drw.GetDocsOnDisk()
	if err != nil {
		return nil, nil, err
	}

	notStagedDocDiffs, err := NewDocDiffs(ctx, roots.Working, nil, docsOnDisk)
	if err != nil {
		return nil, nil, err
	}

	stagedDocDiffs, err := NewDocDiffs(ctx, roots.Head, roots.Staged, docsOnDisk)
	if err != nil {
		return nil, nil, err
	}

	return stagedDocDiffs, notStagedDocDiffs, nil
}

// TableDelta represents the change of a single table between two roots.
// FromFKs and ToFKs contain Foreign Keys that constrain columns in this table,
// they do not contain Foreign Keys that reference this table.
type TableDelta struct {
	FromName       string
	ToName         string
	FromTable      *doltdb.Table
	ToTable        *doltdb.Table
	FromFks        []doltdb.ForeignKey
	ToFks          []doltdb.ForeignKey
	ToFksParentSch map[string]schema.Schema
}

// GetTableDeltas returns a slice of TableDelta objects for each table that changed between fromRoot and toRoot.
// It matches tables across roots using the tag of the first primary key column in the table's schema.
func GetTableDeltas(ctx context.Context, fromRoot, toRoot *doltdb.RootValue) (deltas []TableDelta, err error) {
	deltas, seenKeylessKeyedChanges, err := getKeylessKeyedDeltas(ctx, fromRoot, toRoot)
	if err != nil {
		return nil, err
	}

	keylessDeltas, err := getKeylessDeltas(ctx, fromRoot, toRoot)
	if err != nil {
		return nil, err
	}

	deltas = append(deltas, pruneKeylessKeyedDeltas(keylessDeltas, seenKeylessKeyedChanges)...)

	keyedDelta, err := getKeyedDeltas(ctx, fromRoot, toRoot)
	if err != nil {
		return nil, err
	}

	deltas = append(deltas, pruneKeylessKeyedDeltas(keyedDelta, seenKeylessKeyedChanges)...)

	return deltas, nil
}

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

// getKeyedDeltas returns the deltas between all keyed tables
func getKeyedDeltas(ctx context.Context, fromRoot, toRoot *doltdb.RootValue) (deltas []TableDelta, err error) {
	fromTables := make(map[uint64]*doltdb.Table)
	fromTableNames := make(map[uint64]string)
	fromTableFKs := make(map[uint64][]doltdb.ForeignKey)
	fromTableHashes := make(map[uint64]hash.Hash)

	fromFKC, err := fromRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	err = fromRoot.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		if schema.IsKeyless(sch) {
			return
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := getUniqueTag(sch)
		fromTables[pkTag] = table
		fromTableNames[pkTag] = name
		fromTableHashes[pkTag] = th
		fromTableFKs[pkTag], _ = fromFKC.KeysForTable(name)
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	toFKC, err := toRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	err = toRoot.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		if schema.IsKeyless(sch) {
			return
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		toFKs, _ := toFKC.KeysForTable(name)
		toFksParentSch, err := getFkParentSchs(ctx, toRoot, toFKs...)
		if err != nil {
			return false, err
		}

		pkTag := getUniqueTag(sch)
		oldName, ok := fromTableNames[pkTag]

		if !ok {
			deltas = append(deltas, TableDelta{
				ToName:         name,
				ToTable:        table,
				ToFks:          toFKs,
				ToFksParentSch: toFksParentSch,
			})
		} else if oldName != name ||
			fromTableHashes[pkTag] != th ||
			!fkSlicesAreEqual(fromTableFKs[pkTag], toFKs) {

			deltas = append(deltas, TableDelta{
				FromName:       fromTableNames[pkTag],
				ToName:         name,
				FromTable:      fromTables[pkTag],
				ToTable:        table,
				FromFks:        fromTableFKs[pkTag],
				ToFks:          toFKs,
				ToFksParentSch: toFksParentSch,
			})
		}

		if ok {
			delete(fromTableNames, pkTag) // consume table name
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	// all unmatched tables in fromRoot must have been dropped
	for pkTag, oldName := range fromTableNames {
		deltas = append(deltas, TableDelta{
			FromName:  oldName,
			FromTable: fromTables[pkTag],
			FromFks:   fromTableFKs[pkTag],
		})
	}

	return deltas, nil
}

// we don't have any stable identifier to a keyless table, have to do an n^2 match
// todo: this is a good reason to implement table tags
// getKeylessDeltas returns the deltas between all keyless tables
func getKeylessDeltas(ctx context.Context, fromRoot, toRoot *doltdb.RootValue) (deltas []TableDelta, err error) {
	type fromTable struct {
		tags *set.Uint64Set
		tbl  *doltdb.Table
		hsh  hash.Hash
	}

	fromTables := make(map[string]fromTable)
	err = fromRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		if !schema.IsKeyless(sch) {
			return
		}

		h, err := tbl.HashOf()
		if err != nil {
			return false, err
		}

		fromTables[name] = fromTable{
			tags: set.NewUint64Set(sch.GetAllCols().Tags),
			tbl:  tbl,
			hsh:  h,
		}
		return
	})
	if err != nil {
		return nil, err
	}

	err = toRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		if !schema.IsKeyless(sch) {
			return
		}

		toTblHash, err := tbl.HashOf()
		if err != nil {
			return false, err
		}

		delta := TableDelta{
			ToName:  name,
			ToTable: tbl,
		}

		toTableTags := set.NewUint64Set(sch.GetAllCols().Tags)
		for fromName, fromTbl := range fromTables {

			// |tbl| and |fromTbl| have the same identity
			// if they have column tags in common
			if toTableTags.Intersection(fromTbl.tags).Size() > 0 {

				// consume matched fromTable
				delete(fromTables, fromName)

				if toTblHash.Equal(fromTbl.hsh) {
					// no diff, skip table
					return
				}

				delta.FromName = fromName
				delta.FromTable = fromTbl.tbl
				break
			}
		}

		// append if matched or unmatched
		deltas = append(deltas, delta)
		return
	})
	if err != nil {
		return nil, err
	}

	// all unmatched pairs are table drops
	for name, fromPair := range fromTables {
		deltas = append(deltas, TableDelta{
			FromName:  name,
			FromTable: fromPair.tbl,
		})
	}

	return deltas, nil
}

// getKeylessKeyedDeltas returns the table deltas between tables that were previously keyless and now keyed or vice versa
func getKeylessKeyedDeltas(ctx context.Context, fromRoot, toRoot *doltdb.RootValue) (deltas []TableDelta, seenKeylessKeyedChanges map[string]bool, err error) {
	seenKeylessKeyedChanges = make(map[string]bool)
	type fromTable struct {
		tags *set.Uint64Set
		tbl  *doltdb.Table
		sch  schema.Schema
	}

	fromTables := make(map[string]fromTable)
	err = fromRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		fromTables[name] = fromTable{
			tags: set.NewUint64Set(sch.GetAllCols().Tags),
			tbl:  tbl,
			sch:  sch,
		}
		return
	})
	if err != nil {
		return nil, nil, err
	}

	err = toRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		delta := TableDelta{
			ToName:  name,
			ToTable: tbl,
		}

		// TODO: is this the most efficient way to to do this
		toTableTags := set.NewUint64Set(sch.GetAllCols().Tags)
		for fromName, fromTbl := range fromTables {
			// |tbl| and |fromTbl| have the same identity
			// if they have column tags in common
			if toTableTags.Intersection(fromTbl.tags).Size() > 0 {
				// consume matched fromTable
				delete(fromTables, fromName)

				// detect schema difference
				if (schema.IsKeyless(fromTbl.sch) && !schema.IsKeyless(sch)) || (!schema.IsKeyless(fromTbl.sch) && schema.IsKeyless(sch)) {
					delta.FromName = fromName
					delta.FromTable = fromTbl.tbl
					deltas = append(deltas, delta)
					seenKeylessKeyedChanges[delta.FromName] = true
					break
				}
			}
		}
		return
	})

	if err != nil {
		return nil, nil, err
	}

	return deltas, seenKeylessKeyedChanges, nil
}

// pruneKeylessKeyedDeltas prunes any deltas that are located in the schema change deltas.
func pruneKeylessKeyedDeltas(deltas []TableDelta, seenKeylessKeyedChanges map[string]bool) []TableDelta {
	ret := make([]TableDelta, 0)

	for _, d := range deltas {
		if !(seenKeylessKeyedChanges[d.FromName] || seenKeylessKeyedChanges[d.ToName]) {
			ret = append(ret, d)
		}
	}

	return ret
}

func getUniqueTag(sch schema.Schema) uint64 {
	if schema.IsKeyless(sch) {
		panic("keyless tables have no stable column tags")
	}
	return sch.GetPKCols().Tags[0]
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
	if td.FromTable != nil {
		from, err = td.FromTable.GetSchema(ctx)

		if err != nil {
			return nil, nil, err
		}
	} else {
		from = schema.EmptySchema
	}

	if td.ToTable != nil {
		to, err = td.ToTable.GetSchema(ctx)

		if err != nil {
			return nil, nil, err
		}
	} else {
		to = schema.EmptySchema
	}

	return from, to, nil
}

func (td TableDelta) IsKeyless(ctx context.Context) (bool, error) {
	f, t, err := td.GetSchemas(ctx)
	if err != nil {
		return false, err
	}

	from, to := schema.IsKeyless(f), schema.IsKeyless(t)

	if from && to {
		return true, nil
	} else if !from && !to {
		return false, nil
	} else {
		return false, fmt.Errorf("mismatched keyless and keyed schemas for table %s", td.CurName())
	}
}

// GetMaps returns the table's row map at the fromRoot and toRoot, or and empty map if the table did not exist.
func (td TableDelta) GetMaps(ctx context.Context) (from, to types.Map, err error) {
	if td.FromTable != nil {
		from, err = td.FromTable.GetRowData(ctx)
		if err != nil {
			return from, to, err
		}
	} else {
		from, _ = types.NewMap(ctx, td.ToTable.ValueReadWriter())
	}

	if td.ToTable != nil {
		to, err = td.ToTable.GetRowData(ctx)
		if err != nil {
			return from, to, err
		}
	} else {
		to, _ = types.NewMap(ctx, td.FromTable.ValueReadWriter())
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
