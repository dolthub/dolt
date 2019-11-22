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
	DocToType  map[string]DocDiffType
	Docs       []string
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

func NewDocDiffs(ctx context.Context, newerLicenseBytes, newReadmeBytes []byte, older *doltdb.RootValue) (*DocDiffs, error) {
	added, modified, removed, err := older.DocDiff(ctx, newerLicenseBytes, newReadmeBytes)

	if err != nil {
		return nil, err
	}

	var dcs []string
	dcs = append(dcs, added...)
	dcs = append(dcs, modified...)
	dcs = append(dcs, removed...)
	sort.Strings(dcs)

	dcsToType := make(map[string]DocDiffType)
	for _, nt := range added {
		dcsToType[nt] = AddedDoc
	}

	for _, nt := range modified {
		dcsToType[nt] = ModifiedDoc
	}

	for _, nt := range removed {
		dcsToType[nt] = RemovedDoc
	}

	return &DocDiffs{len(added), len(modified), len(removed), dcsToType, dcs}, err
}

func (nd *DocDiffs) Len() int {
	return len(nd.Docs)
}

func GetDocDiffs(ctx context.Context, dEnv *env.DoltEnv) (*TableDiffs, *DocDiffs, error) {
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

	// TO DO: Implement diffs on staged docs
	// stagedTblDiffs, err := NewStagedDocDiffs(ctx, stagedRoot, headRoot)

	if err != nil {
		return nil, nil, err
	}

	notStagedNtDiffs, err := NewDocDiffs(ctx, licenseText, readmeText, workingRoot)

	if err != nil {
		return nil, nil, err
	}

	// return empty staged doc diffs until I have `dolt add` working
	var emptyStagedDocDiffs *TableDiffs

	return emptyStagedDocDiffs, notStagedNtDiffs, nil
}
