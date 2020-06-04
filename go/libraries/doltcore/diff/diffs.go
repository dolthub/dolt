// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

type TableDiffType int

const (
	AddedTable TableDiffType = iota
	ModifiedTable
	RemovedTable
	RenamedTable
)

type TableDiffs struct {
	NumAdded    int
	NumModified int
	NumRemoved  int
	TableToType map[string]TableDiffType
	Tables      []string

	// renamed tables are included in TableToType by their new name
	NewNameToOldName map[string]string
}

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

func NewTableDiffs(ctx context.Context, newer, older *doltdb.RootValue) (*TableDiffs, error) {
	matches, err := matchTablesForRoots(ctx, newer, older)

	if err != nil {
		return nil, err
	}

	var tbls []string
	tbls = append(tbls, matches.added...)
	tbls = append(tbls, matches.modified...)
	tbls = append(tbls, matches.dropped...)

	tblToType := make(map[string]TableDiffType)
	for _, tbl := range matches.added {
		tblToType[tbl] = AddedTable
	}

	for _, tbl := range matches.modified {
		tblToType[tbl] = ModifiedTable
	}

	for _, tbl := range matches.dropped {
		tblToType[tbl] = RemovedTable
	}

	for newName, _ := range matches.renamed {
		tblToType[newName] = RenamedTable
		tbls = append(tbls, newName)
	}

	sort.Strings(tbls)

	return &TableDiffs{
		NumAdded:         len(matches.added),
		NumModified:      len(matches.modified),
		NumRemoved:       len(matches.dropped),
		TableToType:      tblToType,
		Tables:           tbls,
		NewNameToOldName: matches.renamed,
	}, err
}

func (td *TableDiffs) Len() int {
	return len(td.Tables)
}

func GetTableDiffs(ctx context.Context, dEnv *env.DoltEnv) (*TableDiffs, *TableDiffs, error) {
	headRoot, err := dEnv.HeadRoot(ctx)

	if err != nil {
		return nil, nil, RootValueUnreadable{HeadRoot, err}
	}

	stagedRoot, err := dEnv.StagedRoot(ctx)

	if err != nil {
		return nil, nil, RootValueUnreadable{StagedRoot, err}
	}

	workingRoot, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return nil, nil, RootValueUnreadable{WorkingRoot, err}
	}

	stagedDiffs, err := NewTableDiffs(ctx, stagedRoot, headRoot)

	if err != nil {
		return nil, nil, err
	}

	notStagedDiffs, err := NewTableDiffs(ctx, workingRoot, stagedRoot)

	if err != nil {
		return nil, nil, err
	}

	return stagedDiffs, notStagedDiffs, nil
}

func NewDocDiffs(ctx context.Context, dEnv *env.DoltEnv, older *doltdb.RootValue, newer *doltdb.RootValue, docDetails []doltdb.DocDetails) (*DocDiffs, error) {
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

func (nd *DocDiffs) Len() int {
	return len(nd.Docs)
}

// GetDocDiffs retrieves staged and unstaged DocDiffs.
func GetDocDiffs(ctx context.Context, dEnv *env.DoltEnv) (*DocDiffs, *DocDiffs, error) {
	docDetails, err := dEnv.GetAllValidDocDetails()
	if err != nil {
		return nil, nil, err
	}

	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, nil, err
	}

	notStagedDocDiffs, err := NewDocDiffs(ctx, dEnv, workingRoot, nil, docDetails)
	if err != nil {
		return nil, nil, err
	}

	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return nil, nil, err
	}

	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return nil, nil, err
	}

	stagedDocDiffs, err := NewDocDiffs(ctx, dEnv, headRoot, stagedRoot, nil)
	if err != nil {
		return nil, nil, err
	}

	return stagedDocDiffs, notStagedDocDiffs, nil
}

type tableMatches struct {
	added     []string
	dropped   []string
	modified  []string
	unchanged []string
	renamed   map[string]string
}

// matchTablesForRoots matches all tables that exist in both roots and finds the tables that only exist in one root.
// Tables are matched by the column tag of the first primary key column.
func matchTablesForRoots(ctx context.Context, newer, older *doltdb.RootValue) (tableMatches, error) {
	tm := tableMatches{}
	tm.renamed = make(map[string]string)
	FromTableNames := make(map[uint64]string)
	FromTableHashes := make(map[uint64]hash.Hash)

	err := older.IterAllTables(ctx, func(name string, table *doltdb.Table) (stop bool, err error) {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return true, err
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
		FromTableNames[pkTag] = name
		FromTableHashes[pkTag] = th
		return false, nil
	})

	if err != nil {
		return tm, err
	}

	err = newer.IterAllTables(ctx, func(name string, table *doltdb.Table) (stop bool, err error) {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return true, err
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
		oldName, ok := FromTableNames[pkTag]

		switch {
		case !ok:
			tm.added = append(tm.added, name)
		case oldName != name:
			tm.renamed[name] = oldName
		case FromTableHashes[pkTag] != th:
			tm.modified = append(tm.modified, name)
		default:
			tm.unchanged = append(tm.unchanged, name)
		}

		if ok {
			delete(FromTableNames, pkTag) // consume table name
		}

		return false, nil
	})

	if err != nil {
		return tm, err
	}

	// all unmatched tables from older must have been dropped
	for _, oldName := range FromTableNames {
		tm.dropped = append(tm.dropped, oldName)
	}

	return tm, nil
}

type TableDelta struct {
	FromName  string
	ToName    string
	FromTable *doltdb.Table
	ToTable   *doltdb.Table
}

func GetUserTableDeltas(ctx context.Context, fromRoot, toRoot *doltdb.RootValue) ([]TableDelta, error) {
	var deltas []TableDelta
	fromTable := make(map[uint64]*doltdb.Table)
	fromTableNames := make(map[uint64]string)
	fromTableHashes := make(map[uint64]hash.Hash)

	err := fromRoot.IterUserTables(ctx, func(name string, table *doltdb.Table) (stop bool, err error) {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return true, err
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
		fromTable[pkTag] = table
		fromTableNames[pkTag] = name
		fromTableHashes[pkTag] = th
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	err = toRoot.IterUserTables(ctx, func(name string, table *doltdb.Table) (stop bool, err error) {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return true, err
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
		oldName, ok := fromTableNames[pkTag]

		if !ok {
			deltas = append(deltas, TableDelta{ToName: name, ToTable: table})
		} else if oldName != name || fromTableHashes[pkTag] != th {
			deltas = append(deltas, TableDelta{
				ToName:    name,
				FromName:  fromTableNames[pkTag],
				ToTable:   table,
				FromTable: fromTable[pkTag],
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
		deltas = append(deltas, TableDelta{FromName: oldName, FromTable: fromTable[pkTag]})
	}

	return deltas, nil
}

func (td TableDelta) IsAdd() bool {
	return td.FromTable == nil && td.ToTable != nil
}

func (td TableDelta) IsDrop() bool {
	return td.FromTable != nil && td.ToTable == nil
}

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
