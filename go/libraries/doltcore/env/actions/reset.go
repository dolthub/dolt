// Copyright 2020 Dolthub, Inc.
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
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/store/datas"
)

// resetHardTables resolves a new HEAD commit from a refSpec and updates working set roots by
// resetting the table contexts for tracked tables. New tables are ignored. Returns new HEAD
// Commit and Roots.
func resetHardTables[C doltdb.Context](ctx C, dbData env.DbData[C], cSpecStr string, roots doltdb.Roots) (*doltdb.Commit, doltdb.Roots, error) {
	ddb := dbData.Ddb
	rsr := dbData.Rsr

	var newHead *doltdb.Commit
	if cSpecStr != "" {
		cs, err := doltdb.NewCommitSpec(cSpecStr)
		if err != nil {
			return nil, doltdb.Roots{}, err
		}

		headRef, err := rsr.CWBHeadRef(ctx)
		if err != nil {
			return nil, doltdb.Roots{}, err
		}
		optCmt, err := ddb.Resolve(ctx, cs, headRef)
		if err != nil {
			return nil, doltdb.Roots{}, err
		}

		var ok bool
		if newHead, ok = optCmt.ToCommit(); !ok {
			return nil, doltdb.Roots{}, doltdb.ErrGhostCommitEncountered
		}

		roots.Head, err = newHead.GetRootValue(ctx)
		if err != nil {
			return nil, doltdb.Roots{}, err
		}
	}

	newWorking, err := MoveUntrackedTables(ctx, roots.Working, roots.Staged, roots.Head)
	if err != nil {
		return nil, doltdb.Roots{}, err
	}
	return newHead, doltdb.Roots{Head: roots.Head, Working: newWorking, Staged: roots.Head}, nil
}

// MoveUntrackedTables copies tables present in |sourceWorking| but absent from |sourceStaged|
// onto |target|, returning the resulting root. Tables that name-collide with one already in
// |target| are skipped so the target version wins. Tables whose column tags collide with one
// in |target| are dropped silently.
// TODO(elianddb): retag colliding columns instead of dropping the table.
func MoveUntrackedTables(ctx context.Context, sourceWorking, sourceStaged, target doltdb.RootValue) (doltdb.RootValue, error) {
	untracked, err := doltdb.GetAllSchemas(ctx, sourceWorking)
	if err != nil {
		return nil, err
	}

	staged, err := doltdb.GetAllSchemas(ctx, sourceStaged)
	if err != nil {
		return nil, err
	}
	for name := range staged {
		delete(untracked, name)
	}

	targetSchemas, err := doltdb.GetAllSchemas(ctx, target)
	if err != nil {
		return nil, err
	}
	targetTags := make(map[uint64]struct{})
	for _, sch := range targetSchemas {
		for _, tag := range sch.GetAllCols().Tags {
			targetTags[tag] = struct{}{}
		}
	}

	for name, sch := range untracked {
		for _, col := range sch.GetAllCols().GetColumns() {
			if _, ok := targetTags[col.Tag]; ok {
				delete(untracked, name)
				break
			}
		}
	}

	for name := range untracked {
		// Skip when target has a table of the same name so the target version wins.
		if _, exists := targetSchemas[name]; exists {
			continue
		}
		tbl, exists, err := sourceWorking.GetTable(ctx, name)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("untracked table %s does not exist in working set", name)
		}
		target, err = target.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, fmt.Errorf("failed to write table back to database: %s", err)
		}
	}

	return target, nil
}

func GetAllTableNames(ctx context.Context, root doltdb.RootValue) []doltdb.TableName {
	tableNames := make([]doltdb.TableName, 0)
	_ = root.IterTables(ctx, func(name doltdb.TableName, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		tableNames = append(tableNames, name)
		return false, nil
	})
	return tableNames
}

// ResetHardTables resets the tables in working, staged, and head based on the given parameters. Returns the new
// head commit and resulting roots
func ResetHardTables[C doltdb.Context](ctx C, dbData env.DbData[C], cSpecStr string, roots doltdb.Roots) (*doltdb.Commit, doltdb.Roots, error) {
	return resetHardTables(ctx, dbData, cSpecStr, roots)
}

// ResetHard resets the working, staged, and head to the ones in the provided roots and head ref.
// The reset can be performed on a non-current branch and working set.
// Returns an error if the reset fails.
func ResetHard[C doltdb.Context](
	ctx C,
	dbData env.DbData[C],
	doltDb *doltdb.DoltDB,
	username, email string,
	cSpecStr string,
	roots doltdb.Roots,
	headRef ref.DoltRef,
	ws *doltdb.WorkingSet,
) error {

	newHead, roots, err := resetHardTables(ctx, dbData, cSpecStr, roots)
	if err != nil {
		return err
	}

	currentWs, err := doltDb.ResolveWorkingSet(ctx, ws.Ref())
	if err != nil {
		return err
	}

	h, err := currentWs.HashOf()
	if err != nil {
		return err
	}

	// TODO - refactor this to ensure the update to the head and working set are transactional.
	err = doltDb.UpdateWorkingSet(ctx, ws.Ref(), ws.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged).ClearMerge().ClearRebase(), h, &datas.WorkingSetMeta{
		Name:        username,
		Email:       email,
		Timestamp:   uint64(time.Now().Unix()),
		Description: "reset hard",
	}, nil)
	if err != nil {
		return err
	}

	if newHead != nil {
		err = doltDb.SetHeadToCommit(ctx, headRef, newHead)
		if err != nil {
			return err
		}
	}

	return nil
}

func ResetSoftTables(ctx context.Context, tableNames []doltdb.TableName, roots doltdb.Roots) (doltdb.Roots, error) {
	tables, err := getUnionedTables(ctx, tableNames, roots.Staged, roots.Head)
	if err != nil {
		return doltdb.Roots{}, err
	}

	err = ValidateTables(ctx, tables, roots.Staged, roots.Head)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged, err = MoveTablesBetweenRoots(ctx, tables, roots.Head, roots.Staged)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return roots, nil
}

// MoveHeadToRef resolves |cSpecStr| to a commit and moves the current branch HEAD to it. The
// returned Roots has Head and Staged set to the resolved commit's root, available for callers
// composing further reset semantics on top of the HEAD move.
func MoveHeadToRef[C doltdb.Context](ctx C, dbData env.DbData[C], cSpecStr string) (doltdb.Roots, error) {
	cs, err := doltdb.NewCommitSpec(cSpecStr)
	if err != nil {
		return doltdb.Roots{}, err
	}

	headRef, err := dbData.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}
	optCmt, err := dbData.Ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return doltdb.Roots{}, err
	}
	newHead, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.Roots{}, doltdb.ErrGhostCommitEncountered
	}

	foundRoot, err := newHead.GetRootValue(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}

	// Update the head to this commit
	if err = dbData.Ddb.SetHeadToCommit(ctx, headRef, newHead); err != nil {
		return doltdb.Roots{}, err
	}

	return doltdb.Roots{
		Head:   foundRoot,
		Staged: foundRoot,
	}, err
}

func getUnionedTables(ctx context.Context, tables []doltdb.TableName, stagedRoot, headRoot doltdb.RootValue) ([]doltdb.TableName, error) {
	if len(tables) == 0 {
		var err error
		tables, err = doltdb.UnionTableNames(ctx, stagedRoot, headRoot)

		if err != nil {
			return nil, err
		}
	}

	return tables, nil
}

// CleanUntracked deletes from the working root the tables that are untracked (in working but not in staged/head). If
// |tables| is non-empty it uses only those names as candidates; otherwise it uses all working tables. Tables matching
// dolt_nonlocal_tables are always excluded. When |respectIgnoreRules| is true, tables matching dolt_ignore are also excluded. Does nothing when |dryrun| is true.
func CleanUntracked(ctx *sql.Context, roots doltdb.Roots, tables []string, dryrun bool, force bool, respectIgnoreRules bool) (doltdb.Roots, error) {
	untrackedTables := make(map[doltdb.TableName]struct{})
	for _, name := range tables {
		resolvedName, tblExists, err := resolve.TableName(ctx, roots.Working, name)
		if err != nil {
			return doltdb.Roots{}, err
		}
		if !tblExists {
			return doltdb.Roots{}, fmt.Errorf("%w: '%s'", doltdb.ErrTableNotFound, name)
		}
		untrackedTables[resolvedName] = struct{}{}
	}

	if len(tables) == 0 {
		allTableNames, err := roots.Working.GetAllTableNames(ctx, true)
		if err != nil {
			return doltdb.Roots{}, err
		}
		var candidates []doltdb.TableName
		if respectIgnoreRules {
			candidates, err = doltdb.ExcludeIgnoredTables(ctx, roots, allTableNames)
			if err != nil {
				return doltdb.Roots{}, err
			}
		} else {
			candidates = allTableNames
		}
		var nonlocalPatterns []string
		err = doltdb.GetNonlocalTablePatterns(ctx, roots.Working, doltdb.DefaultSchemaName, func(p string) {
			nonlocalPatterns = append(nonlocalPatterns, p)
		})
		if err != nil {
			return doltdb.Roots{}, err
		}
		compiled, err := doltdb.CompileTablePatterns(nonlocalPatterns)
		if err != nil {
			return doltdb.Roots{}, err
		}
		for _, tableName := range candidates {
			if compiled.TableMatchesAny(tableName.Name) {
				continue
			}
			untrackedTables[tableName] = struct{}{}
		}
	}

	headTblNames := GetAllTableNames(ctx, roots.Staged)
	for _, name := range headTblNames {
		delete(untrackedTables, name)
	}

	toDelete := make([]doltdb.TableName, 0, len(untrackedTables))
	for t := range untrackedTables {
		toDelete = append(toDelete, t)
	}

	if dryrun {
		return roots, nil
	}

	newRoot, err := roots.Working.RemoveTables(ctx, force, force, toDelete...)
	if err != nil {
		return doltdb.Roots{}, fmt.Errorf("failed to remove tables; %w", err)
	}
	roots.Working = newRoot
	return roots, nil
}
