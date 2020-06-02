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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"sort"

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
	tbls = append(tbls, matches.added..., )
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
		NumAdded: len(matches.added),
		NumModified: len(matches.modified),
		NumRemoved: len(matches.dropped),
		TableToType: tblToType,
		Tables: tbls,
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
	oldTableNames := make(map[uint64]string)
	oldTableHashes := make(map[uint64]hash.Hash)

	err := older.IterTables(ctx, func(name string, table *doltdb.Table) (stop bool, err error) {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return true, err
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
		oldTableNames[pkTag] = name
		oldTableHashes[pkTag] = th
		return false, nil
	})

	if err != nil {
		return tm, err
	}

	err = newer.IterTables(ctx, func(name string, table *doltdb.Table) (stop bool, err error) {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return true, err
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
		oldName, ok := oldTableNames[pkTag]

		switch {
		case !ok:
			tm.added = append(tm.added, name)
		case oldName != name:
			tm.renamed[name] = oldName
		case oldTableHashes[pkTag] != th:
			tm.modified = append(tm.modified, name)
		default:
			tm.unchanged = append(tm.unchanged, name)
		}

		if ok {
			delete(oldTableNames, pkTag) // consume table name
		}

		return false, nil
	})

	if err != nil {
		return tm, err
	}

	// all unmatched tables from older must have been dropped
	for _, oldName := range oldTableNames {
		tm.dropped = append(tm.dropped, oldName)
	}

	return tm, nil
}

// todo: to vs from
type TableDelta struct {
	NewName string
	OldName string
	NewTable *doltdb.Table
	OldTable *doltdb.Table
}

func GetTableDeltas(ctx context.Context, older, newer *doltdb.RootValue) ([]TableDelta, error) {
	var deltas []TableDelta
	oldTables := make(map[uint64]*doltdb.Table)
	oldTableNames := make(map[uint64]string)
	oldTableHashes := make(map[uint64]hash.Hash)

	err := older.IterTables(ctx, func(name string, table *doltdb.Table) (stop bool, err error) {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return true, err
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
		oldTables[pkTag] = table
		oldTableNames[pkTag] = name
		oldTableHashes[pkTag] = th
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	err = newer.IterTables(ctx, func(name string, table *doltdb.Table) (stop bool, err error) {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return true, err
		}

		th, err := table.HashOf()
		if err != nil {
			return true, err
		}

		pkTag := sch.GetPKCols().GetColumns()[0].Tag
		oldName, ok := oldTableNames[pkTag]

		if !ok {
			deltas = append(deltas, TableDelta{NewName: name, NewTable: table})
		} else if oldName != name || oldTableHashes[pkTag] != th {
			deltas = append(deltas, TableDelta{
				NewName: name,
				OldName: oldTableNames[pkTag],
				NewTable: table,
				OldTable: oldTables[pkTag],
			})
		}

		if ok {
			delete(oldTableNames, pkTag) // consume table name
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	// all unmatched tables from older must have been dropped
	for pkTag, oldName := range oldTableNames {
		deltas = append(deltas, TableDelta{OldName: oldName, OldTable: oldTables[pkTag]})
	}

	return deltas, nil
}

func (td TableDelta) IsAdd() bool {
	return td.OldTable == nil && td.NewTable != nil
}

func (td TableDelta) IsDrop() bool {
	return td.OldTable != nil && td.NewTable == nil
}

func (td TableDelta) GetSchemas(ctx context.Context) (new, old schema.Schema, err error) {
	if td.OldTable != nil {
		old, err = td.OldTable.GetSchema(ctx)

		if err != nil {
			return nil, nil, err
		}
	}

	if td.NewTable != nil {
		new, err = td.NewTable.GetSchema(ctx)

		if err != nil {
			return nil, nil, err
		}
	}

	return new, old, nil
}