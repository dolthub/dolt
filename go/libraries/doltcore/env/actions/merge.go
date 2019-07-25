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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/merge"
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
			root, err = root.RemoveTables(ctx, tblName)

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
