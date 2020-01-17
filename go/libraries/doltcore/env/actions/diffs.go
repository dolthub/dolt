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

package actions

import (
	"context"
	"sort"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

type TableDiffType int

const (
	AddedTable TableDiffType = iota
	ModifiedTable
	RemovedTable
)

type TableDiffs struct {
	NumAdded    int
	NumModified int
	NumRemoved  int
	TableToType map[string]TableDiffType
	Tables      []string
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

func NewTableDiffs(ctx context.Context, newer, older *doltdb.RootValue) (*TableDiffs, error) {
	added, modified, removed, err := newer.TableDiff(ctx, older)

	if err != nil {
		return nil, err
	}

	var tbls []string
	tbls = append(tbls, added...)
	tbls = append(tbls, modified...)
	tbls = append(tbls, removed...)
	sort.Strings(tbls)

	tblToType := make(map[string]TableDiffType)
	for _, tbl := range added {
		tblToType[tbl] = AddedTable
	}

	for _, tbl := range modified {
		tblToType[tbl] = ModifiedTable
	}

	for _, tbl := range removed {
		tblToType[tbl] = RemovedTable
	}

	return &TableDiffs{len(added), len(modified), len(removed), tblToType, tbls}, err
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
