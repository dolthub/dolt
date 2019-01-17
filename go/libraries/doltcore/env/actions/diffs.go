package actions

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"sort"
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

func NewTableDiffs(newer, older *doltdb.RootValue) *TableDiffs {
	added, modified, removed := newer.TableDiff(older)

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

func GetTableDiffs(dEnv *env.DoltEnv) (*TableDiffs, *TableDiffs, error) {
	headRoot, err := dEnv.HeadRoot()

	if err != nil {
		return nil, nil, RootValueUnreadable{HeadRoot, err}
	}

	stagedRoot, err := dEnv.StagedRoot()

	if err != nil {
		return nil, nil, RootValueUnreadable{StagedRoot, err}
	}

	workingRoot, err := dEnv.WorkingRoot()

	if err != nil {
		return nil, nil, RootValueUnreadable{WorkingRoot, err}
	}

	stagedDiffs := NewTableDiffs(stagedRoot, headRoot)
	notStagedDiffs := NewTableDiffs(workingRoot, stagedRoot)

	return stagedDiffs, notStagedDiffs, nil
}
