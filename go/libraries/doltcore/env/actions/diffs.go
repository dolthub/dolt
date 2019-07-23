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

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
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

func NewTableDiffs(ctx context.Context, newer, older *doltdb.RootValue) *TableDiffs {
	added, modified, removed := newer.TableDiff(ctx, older)

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

	return &TableDiffs{len(added), len(modified), len(removed), tblToType, tbls}
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

	stagedDiffs := NewTableDiffs(ctx, stagedRoot, headRoot)
	notStagedDiffs := NewTableDiffs(ctx, workingRoot, stagedRoot)

	return stagedDiffs, notStagedDiffs, nil
}
