package actions

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/merge"
)

func MergeCommits(ctx context.Context, ddb *doltdb.DoltDB, cm1, cm2 *doltdb.Commit) (*doltdb.RootValue, map[string]*merge.MergeStats, error) {
	merger, err := merge.NewMerger(ctx, cm1, cm2, ddb.ValueReadWriter())

	if err != nil {
		return nil, nil, err
	}

	root := cm1.GetRootValue()
	tblNames := AllTables(ctx, root, cm2.GetRootValue())
	tblToStats := make(map[string]*merge.MergeStats)

	// need to validate merges can be done on all tables before starting the actual merges.
	for _, tblName := range tblNames {
		mergedTable, stats, err := merger.MergeTable(ctx, tblName)

		if err != nil {
			return nil, nil, err
		}

		if mergedTable != nil {
			tblToStats[tblName] = stats
			root = root.PutTable(ctx, ddb, tblName, mergedTable)
		} else if root.HasTable(ctx, tblName) {
			tblToStats[tblName] = &merge.MergeStats{Operation: merge.TableRemoved}
			root, err = root.RemoveTables(ctx, []string{tblName})

			if err != nil {
				return nil, nil, err
			}
		} else {
			panic("?")
		}
	}

	return root, tblToStats, nil
}

func GetTablesInConflict(ctx context.Context, dEnv *env.DoltEnv) (workingInConflict, stagedInConflict, headInConflict []string, err error) {
	var headRoot, stagedRoot, workingRoot *doltdb.RootValue

	headRoot, err = dEnv.HeadRoot(ctx)

	if err != nil {
		return
	}

	stagedRoot, err = dEnv.StagedRoot(ctx)

	if err != nil {
		return
	}

	workingRoot, err = dEnv.WorkingRoot(ctx)

	if err != nil {
		return
	}

	headInConflict = headRoot.TablesInConflict(ctx)
	stagedInConflict = stagedRoot.TablesInConflict(ctx)
	workingInConflict = workingRoot.TablesInConflict(ctx)

	return
}
