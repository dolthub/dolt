package commands

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"os"
	"sort"
	"strings"
)

func statusUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func Status(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = statusUsage(fs)
	fs.Parse(args)

	stagedDiffs, notStagedDiffs, verr := getTableDiffs(cliEnv)

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	printStatus(cliEnv, stagedDiffs, notStagedDiffs)
	return 0
}

type tableDiffType int

const (
	addedTable tableDiffType = iota
	modifiedTable
	removedTable
)

func (tdt tableDiffType) Label() string {
	switch tdt {
	case modifiedTable:
		return "modified:"
	case removedTable:
		return "deleted:"
	case addedTable:
		return "new table:"
	}

	return "?"
}

func (tdt tableDiffType) ShortLabel() string {
	switch tdt {
	case modifiedTable:
		return "M"
	case removedTable:
		return "D"
	case addedTable:
		return "N"
	}

	return "?"
}

type tableDiffs struct {
	numAdded     int
	numModified  int
	numRemoved   int
	tableToType  map[string]tableDiffType
	sortedTables []string
}

func NewTableDiffs(newer, older *doltdb.RootValue) *tableDiffs {
	added, modified, removed := newer.TableDiff(older)

	var tbls []string
	tbls = append(tbls, added...)
	tbls = append(tbls, modified...)
	tbls = append(tbls, removed...)
	sort.Strings(tbls)

	tblToType := make(map[string]tableDiffType)
	for _, tbl := range added {
		tblToType[tbl] = addedTable
	}
	for _, tbl := range modified {
		tblToType[tbl] = modifiedTable
	}
	for _, tbl := range removed {
		tblToType[tbl] = removedTable
	}

	return &tableDiffs{len(added), len(modified), len(removed), tblToType, tbls}
}

func (td *tableDiffs) Len() int {
	return len(td.sortedTables)
}

func getTableDiffs(cliEnv *env.DoltCLIEnv) (*tableDiffs, *tableDiffs, errhand.VerboseError) {
	headRoot, err := cliEnv.HeadRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to the get at HEAD.").AddCause(err).Build()
	}

	stagedRoot, err := cliEnv.StagedRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to the get staged.").AddCause(err).Build()
	}

	workingRoot, err := cliEnv.WorkingRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to the get working.").AddCause(err).Build()
	}

	stagedDiffs := NewTableDiffs(stagedRoot, headRoot)
	notStagedDiffs := NewTableDiffs(workingRoot, stagedRoot)

	return stagedDiffs, notStagedDiffs, nil
}

const (
	branchHeader = "On branch %s\n"
	stagedHeader = `Changes to be committed:
  (use "dolt reset <table>..." to unstage)`

	workingHeader = `Changes not staged for commit:
  (use "dolt add <table>" to update what will be committed)
  (use "dolt checkout <table>" to discard changes in working directory)`

	untrackedHeader = `Untracked files:
  (use "dolt add <table>" to include in what will be committed)`

	statusFmt = "\t%-12s%s"
)

func printStatus(cliEnv *env.DoltCLIEnv, staged, notStaged *tableDiffs) {
	needGap := false
	fmt.Printf(branchHeader, cliEnv.RepoState.Branch)

	if staged.Len() > 0 {
		fmt.Println(stagedHeader)

		lines := make([]string, 0, staged.Len())
		for _, tblName := range staged.sortedTables {
			tdt := staged.tableToType[tblName]
			lines = append(lines, fmt.Sprintf(statusFmt, tdt.Label(), tblName))
		}

		fmt.Println(color.GreenString(strings.Join(lines, "\n")))
		needGap = true
	}

	if notStaged.numRemoved+notStaged.numModified > 0 {
		if needGap {
			fmt.Println()
		}

		fmt.Println(workingHeader)

		lines := make([]string, 0, notStaged.Len())
		for _, tblName := range notStaged.sortedTables {
			tdt := notStaged.tableToType[tblName]

			if tdt != addedTable {
				lines = append(lines, fmt.Sprintf(statusFmt, tdt.Label(), tblName))
			}
		}

		fmt.Println(color.RedString(strings.Join(lines, "\n")))
		needGap = true
	}

	if notStaged.numAdded > 0 {
		if needGap {
			fmt.Println()
		}

		fmt.Println(untrackedHeader)

		lines := make([]string, 0, notStaged.Len())
		for _, tblName := range notStaged.sortedTables {
			tdt := notStaged.tableToType[tblName]

			if tdt == addedTable {
				lines = append(lines, fmt.Sprintf(statusFmt, tdt.Label(), tblName))
			}
		}

		fmt.Println(color.RedString(strings.Join(lines, "\n")))
		needGap = true
	}

	if !needGap {
		fmt.Println("nothing to commit, working tree clean")
	}
}
