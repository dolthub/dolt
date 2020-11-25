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
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
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

type RootType int

func (rt RootType) String() string {
	switch rt {
	case WorkingRoot:
		return "working root"
	case StagedRoot:
		return "staged root"
	case CommitRoot:
		return "root value for commit"
	case HeadRoot:
		return "HEAD commit root value"
	}

	return "unknown"
}

const (
	WorkingRoot RootType = iota
	StagedRoot
	CommitRoot
	HeadRoot
	InvalidRoot
)

type RootValueUnreadable struct {
	rootType RootType
	Cause    error
}

func (rvu RootValueUnreadable) Error() string {
	return "error: Unable to read " + rvu.rootType.String()
}

// NewDocDiffs returns DocDiffs for Dolt Docs between two roots.
func NewDocDiffs(ctx context.Context, older *doltdb.RootValue, newer *doltdb.RootValue, docDetails []doltdb.DocDetails) (*DocDiffs, error) {
	var added []string
	var modified []string
	var removed []string
	if older != nil {
		if newer == nil {
			a, m, r, err := older.DocDiff(ctx, nil, docDetails)
			if err != nil {
				return nil, err
			}
			added = a
			modified = m
			removed = r
		} else {
			a, m, r, err := older.DocDiff(ctx, newer, docDetails)
			if err != nil {
				return nil, err
			}
			added = a
			modified = m
			removed = r
		}
	}
	var docs []string
	docs = append(docs, added...)
	docs = append(docs, modified...)
	docs = append(docs, removed...)
	sort.Strings(docs)

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

	return &DocDiffs{len(added), len(modified), len(removed), docsToType, docs}, nil
}

// Len returns the number of docs in a DocDiffs
func (nd *DocDiffs) Len() int {
	return len(nd.Docs)
}

// GetDocDiffs retrieves staged and unstaged DocDiffs.
func GetDocDiffs(ctx context.Context, ddb *doltdb.DoltDB, reader env.RepoStateReader) (*DocDiffs, *DocDiffs, error) {
	docDetails, err := dEnv.GetAllValidDocDetails() // TODO: Doc diffs
	if err != nil {
		return nil, nil, err
	}

	workingRoot, err := env.WorkingRoot(ctx, ddb, reader)
	if err != nil {
		return nil, nil, err
	}

	notStagedDocDiffs, err := NewDocDiffs(ctx, workingRoot, nil, docDetails)
	if err != nil {
		return nil, nil, err
	}

	headRoot, err := env.HeadRoot(ctx, ddb, reader)
	if err != nil {
		return nil, nil, err
	}

	stagedRoot, err := env.StagedRoot(ctx, ddb, reader)
	if err != nil {
		return nil, nil, err
	}

	stagedDocDiffs, err := NewDocDiffs(ctx, headRoot, stagedRoot, nil)
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
	fromTables := make(map[uint64]*doltdb.Table)
	fromTableNames := make(map[uint64]string)
	fromTableFKs := make(map[uint64][]doltdb.ForeignKey)
	fromTableHashes := make(map[uint64]hash.Hash)

	fromFKC, err := fromRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	err = fromRoot.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
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
		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		toFKs, _ := toFKC.KeysForTable(name)
		toFksParentSch := make(map[string]schema.Schema)
		for _, toFk := range toFKs {
			toRefTable, _, ok, err := toRoot.GetTableInsensitive(ctx, toFk.ReferencedTableName)
			if err != nil {
				return true, err
			}
			if !ok {
				continue // as the schemas are for display-only, we can skip on any missing parents (they were deleted, etc.)
			}
			toRefSch, err := toRefTable.GetSchema(ctx)
			if err != nil {
				return true, err
			}
			toFksParentSch[toFk.ReferencedTableName] = toRefSch
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
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

func GetStagedUnstagedTableDeltas(ctx context.Context,  ddb *doltdb.DoltDB, reader env.RepoStateReader) (staged, unstaged []TableDelta, err error) {
	headRoot, err := env.HeadRoot(ctx, ddb, reader)
	if err != nil {
		return nil, nil, RootValueUnreadable{HeadRoot, err}
	}

	stagedRoot, err := env.StagedRoot(ctx, ddb, reader)
	if err != nil {
		return nil, nil, RootValueUnreadable{StagedRoot, err}
	}

	workingRoot, err := env.WorkingRoot(ctx, ddb, reader)
	if err != nil {
		return nil, nil, RootValueUnreadable{WorkingRoot, err}
	}

	staged, err = GetTableDeltas(ctx, headRoot, stagedRoot)
	if err != nil {
		return nil, nil, err
	}

	unstaged, err = GetTableDeltas(ctx, stagedRoot, workingRoot)
	if err != nil {
		return nil, nil, err
	}

	return staged, unstaged, nil
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
