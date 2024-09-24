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
func resetHardTables(ctx *sql.Context, dbData env.DbData, cSpecStr string, roots doltdb.Roots) (*doltdb.Commit, doltdb.Roots, error) {
	ddb := dbData.Ddb
	rsr := dbData.Rsr

	var newHead *doltdb.Commit
	if cSpecStr != "" {
		cs, err := doltdb.NewCommitSpec(cSpecStr)
		if err != nil {
			return nil, doltdb.Roots{}, err
		}

		headRef, err := rsr.CWBHeadRef()
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

	// mirroring Git behavior, untracked tables are ignored on 'reset --hard',
	// save the state of these tables and apply them to |newHead|'s root.
	//
	// as a special case, if an untracked table has a tag collision with any
	// tables in |newHead| we silently drop it from the new working set.
	// these tag collision is typically cause by table renames (bug #751).

	untracked, err := doltdb.GetAllSchemas(ctx, roots.Working)
	if err != nil {
		return nil, doltdb.Roots{}, err
	}

	// untracked tables exist in |working| but not in |staged|
	staged := GetAllTableNames(ctx, roots.Staged)
	for _, name := range staged {
		delete(untracked, name)
	}

	newWkRoot := roots.Head

	ws, err := doltdb.GetAllSchemas(ctx, newWkRoot)
	if err != nil {
		return nil, doltdb.Roots{}, err
	}
	tags := mapColumnTags(ws)

	for name, sch := range untracked {
		for _, pk := range sch.GetAllCols().GetColumns() {
			if _, ok := tags[pk.Tag]; ok {
				// |pk.Tag| collides with a schema in |newWkRoot|
				delete(untracked, name)
			}
		}
	}

	for name := range untracked {
		tbl, exists, err := roots.Working.GetTable(ctx, name)
		if err != nil {
			return nil, doltdb.Roots{}, err
		}
		if !exists {
			return nil, doltdb.Roots{}, fmt.Errorf("untracked table %s does not exist in working set", name)
		}

		newWkRoot, err = newWkRoot.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, doltdb.Roots{}, fmt.Errorf("failed to write table back to database: %s", err)
		}
	}

	// need to save the state of files that aren't tracked
	untrackedTables := make(map[doltdb.TableName]*doltdb.Table)
	wTblNames := GetAllTableNames(ctx, roots.Working)

	for _, tblName := range wTblNames {
		untrackedTables[tblName], _, err = roots.Working.GetTable(ctx, tblName)

		if err != nil {
			return nil, doltdb.Roots{}, err
		}
	}

	headTblNames := GetAllTableNames(ctx, roots.Staged)
	for _, tblName := range headTblNames {
		delete(untrackedTables, tblName)
	}

	roots.Working = newWkRoot
	roots.Staged = roots.Head

	return newHead, roots, nil
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
func ResetHardTables(ctx *sql.Context, dbData env.DbData, cSpecStr string, roots doltdb.Roots) (*doltdb.Commit, doltdb.Roots, error) {
	return resetHardTables(ctx, dbData, cSpecStr, roots)
}

// ResetHard resets the working, staged, and head to the ones in the provided roots and head ref.
// The reset can be performed on a non-current branch and working set.
// Returns an error if the reset fails.
func ResetHard(
	ctx *sql.Context,
	dbData env.DbData,
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

// ResetSoftToRef matches the `git reset --soft <REF>` pattern. It returns a new Roots with the Staged and Head values
// set to the commit specified by the spec string. The Working root is not set
func ResetSoftToRef(ctx context.Context, dbData env.DbData, cSpecStr string) (doltdb.Roots, error) {
	cs, err := doltdb.NewCommitSpec(cSpecStr)
	if err != nil {
		return doltdb.Roots{}, err
	}

	headRef, err := dbData.Rsr.CWBHeadRef()
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

// CleanUntracked deletes untracked tables from the working root.
// Evaluates untracked tables as: all working tables - all staged tables.
func CleanUntracked(ctx *sql.Context, roots doltdb.Roots, tables []string, dryrun bool, force bool) (doltdb.Roots, error) {
	untrackedTables := make(map[doltdb.TableName]struct{})

	var err error
	if len(tables) == 0 {
		tables, err = roots.Working.GetTableNames(ctx, doltdb.DefaultSchemaName)
		if err != nil {
			return doltdb.Roots{}, nil
		}
	}

	for i := range tables {
		name := tables[i]
		resolvedName, _, tblExists, err := resolve.Table(ctx, roots.Working, name)
		if err != nil {
			return doltdb.Roots{}, err
		}
		if !tblExists {
			return doltdb.Roots{}, fmt.Errorf("%w: '%s'", doltdb.ErrTableNotFound, name)
		}
		untrackedTables[resolvedName] = struct{}{}
	}

	// untracked tables = working tables - staged tables
	headTblNames := GetAllTableNames(ctx, roots.Staged)
	if err != nil {
		return doltdb.Roots{}, err
	}

	for _, name := range headTblNames {
		delete(untrackedTables, name)
	}

	newRoot := roots.Working
	var toDelete []doltdb.TableName
	for t := range untrackedTables {
		toDelete = append(toDelete, t)
	}

	newRoot, err = newRoot.RemoveTables(ctx, force, force, toDelete...)
	if err != nil {
		return doltdb.Roots{}, fmt.Errorf("failed to remove tables; %w", err)
	}

	if dryrun {
		return roots, nil
	}
	roots.Working = newRoot

	return roots, nil
}

// mapColumnTags takes a map from table name to schema.Schema and generates
// a map from column tags to table names (see RootValue.GetAllSchemas).
func mapColumnTags(tables map[doltdb.TableName]schema.Schema) (m map[uint64]string) {
	m = make(map[uint64]string, len(tables))
	for tbl, sch := range tables {
		for _, tag := range sch.GetAllCols().Tags {
			m[tag] = tbl.Name
		}
	}
	return
}
