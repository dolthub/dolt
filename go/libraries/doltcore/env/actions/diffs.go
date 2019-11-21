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

type NoteDiffType int

const (
	AddedNote NoteDiffType = iota
	ModifiedNote
	RemovedNote
)

type NoteDiffs struct {
	NumAdded    int
	NumModified int
	NumRemoved  int
	NoteToType  map[string]NoteDiffType
	Notes       []string
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

func NewNoteDiffs(ctx context.Context, newerLicenseBytes, newReadmeBytes []byte, older *doltdb.RootValue) (*NoteDiffs, error) {
	added, modified, removed, err := older.NoteDiff(ctx, newerLicenseBytes, newReadmeBytes)

	if err != nil {
		return nil, err
	}

	var nts []string
	nts = append(nts, added...)
	nts = append(nts, modified...)
	nts = append(nts, removed...)
	sort.Strings(nts)

	ntsToType := make(map[string]NoteDiffType)
	for _, nt := range added {
		ntsToType[nt] = AddedNote
	}

	for _, nt := range modified {
		ntsToType[nt] = ModifiedNote
	}

	for _, nt := range removed {
		ntsToType[nt] = RemovedNote
	}

	return &NoteDiffs{len(added), len(modified), len(removed), ntsToType, nts}, err
}

func (nd *NoteDiffs) Len() int {
	return len(nd.Notes)
}

func GetNoteDiffs(ctx context.Context, dEnv *env.DoltEnv) (*TableDiffs, *NoteDiffs, error) {
	// headRoot, err := dEnv.HeadRoot(ctx)

	// if err != nil {
	// 	return nil, nil, RootValueUnreadable{HeadRoot, err}
	// }

	// stagedRoot, err := dEnv.StagedRoot(ctx)

	// if err != nil {
	// 	return nil, nil, RootValueUnreadable{StagedRoot, err}
	// }

	workingRoot, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return nil, nil, RootValueUnreadable{WorkingRoot, err}
	}

	licenseText, err := dEnv.GetLocalLicenseText()
	if err != nil {
		return nil, nil, err
	}
	readmeText, err := dEnv.GetLocalReadmeText()
	if err != nil {
		return nil, nil, err
	}

	// TO DO: Implement diffs on staged notes
	// stagedTblDiffs, err := NewStagedNoteDiffs(ctx, stagedRoot, headRoot)

	if err != nil {
		return nil, nil, err
	}

	notStagedNtDiffs, err := NewNoteDiffs(ctx, licenseText, readmeText, workingRoot)

	if err != nil {
		return nil, nil, err
	}

	// return empty staged note diffs until I have `dolt add` working
	var emptyStagedNoteDiffs *TableDiffs

	return emptyStagedNoteDiffs, notStagedNtDiffs, nil
}
