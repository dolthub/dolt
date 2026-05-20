// Copyright 2021 Dolthub, Inc.
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
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// MoveTablesFromHeadToWorking replaces the tables named from the given head to the given working root, overwriting any
// working changes, and returns the new resulting roots
func MoveTablesFromHeadToWorking(ctx context.Context, roots doltdb.Roots, tbls []doltdb.TableName) (doltdb.Roots, error) {
	var unknownTbls []doltdb.TableName
	for _, tblName := range tbls {
		tbl, ok, err := roots.Staged.GetTable(ctx, tblName)
		if err != nil {
			return doltdb.Roots{}, err
		}
		fkc, err := roots.Staged.GetForeignKeyCollection(ctx)
		if err != nil {
			return doltdb.Roots{}, err
		}

		if !ok {
			tbl, ok, err = roots.Head.GetTable(ctx, tblName)
			if err != nil {
				return doltdb.Roots{}, err
			}

			fkc, err = roots.Head.GetForeignKeyCollection(ctx)
			if err != nil {
				return doltdb.Roots{}, err
			}

			if !ok {
				unknownTbls = append(unknownTbls, tblName)
				continue
			}
		}

		roots.Working, err = roots.Working.PutTable(ctx, tblName, tbl)
		if err != nil {
			return doltdb.Roots{}, err
		}

		roots.Working, err = roots.Working.PutForeignKeyCollection(ctx, fkc)
		if err != nil {
			return doltdb.Roots{}, err
		}
	}

	if len(unknownTbls) > 0 {
		// Return table not exist error before RemoveTables, which fails silently if the table is not on the root.
		err := validateTablesExist(ctx, roots.Working, unknownTbls)
		if err != nil {
			return doltdb.Roots{}, err
		}

		roots.Working, err = roots.Working.RemoveTables(ctx, false, false, unknownTbls...)

		if err != nil {
			return doltdb.Roots{}, err
		}
	}

	return roots, nil
}

// FindTableInRoots resolves a table by looking in all three roots (working,
// staged, head) in that order.
func FindTableInRoots(ctx *sql.Context, roots doltdb.Roots, name string) (doltdb.TableName, *doltdb.Table, bool, error) {
	tbl, root, tblExists, err := resolve.Table(ctx, roots.Working, name)
	if err != nil {
		return doltdb.TableName{}, nil, false, err
	}
	if tblExists {
		return tbl, root, true, nil
	}

	tbl, root, tblExists, err = resolve.Table(ctx, roots.Staged, name)
	if err != nil {
		return doltdb.TableName{}, nil, false, err
	}
	if tblExists {
		return tbl, root, true, nil
	}

	tbl, root, tblExists, err = resolve.Table(ctx, roots.Head, name)
	if err != nil {
		return doltdb.TableName{}, nil, false, err
	}
	if tblExists {
		return tbl, root, true, nil
	}

	return doltdb.TableName{}, nil, false, nil
}

// CheckoutWouldOverwriteUncommittedTables returns ErrCheckoutWouldOverwrite when any table
// in |roots.Working| but absent from |roots.Head| differs from the committed version on
// |targetRoot|. Tables also present in |roots.Staged| are reported as local changes, while
// tables only in |roots.Working| are reported as untracked.
func CheckoutWouldOverwriteUncommittedTables(ctx context.Context, roots doltdb.Roots, targetRoot doltdb.RootValue) error {
	workingNames, err := roots.Working.GetAllTableNames(ctx, false)
	if err != nil {
		return err
	}
	headNames, err := roots.Head.GetAllTableNames(ctx, false)
	if err != nil {
		return err
	}
	stagedNames, err := roots.Staged.GetAllTableNames(ctx, false)
	if err != nil {
		return err
	}
	headNamesSet := doltdb.NewTableNameSet(headNames)
	stagedNamesSet := doltdb.NewTableNameSet(stagedNames)

	var localChange, untracked []string
	for _, name := range workingNames {
		if headNamesSet.Contains(name) || doltdb.IsReadOnlySystemTable(name) {
			continue
		}
		targetHash, _, err := targetRoot.GetTableHash(ctx, name)
		if err != nil {
			return err
		}
		if targetHash.IsEmpty() {
			continue
		}
		workingHash, _, err := roots.Working.GetTableHash(ctx, name)
		if err != nil {
			return err
		}
		if workingHash == targetHash {
			continue
		}
		if stagedNamesSet.Contains(name) {
			localChange = append(localChange, name.Name)
		} else {
			untracked = append(untracked, name.Name)
		}
	}

	if len(localChange) > 0 || len(untracked) > 0 {
		slices.Sort(localChange)
		slices.Sort(untracked)
		return ErrCheckoutWouldOverwrite{LocalChangeTables: localChange, UntrackedTables: untracked}
	}
	return nil
}

// RootsForBranch returns the roots for checking out a branch whose head is |branchRoot|.
// |roots.Head| must be the pre-checkout head. Uncommitted tables, those present in working
// or staged but absent from the old head, are moved into the new root via
// [CarryUncommittedTables] so any column tag collisions are resolved.
func RootsForBranch(ctx context.Context, roots doltdb.Roots, branchRoot doltdb.RootValue, force bool) (doltdb.Roots, error) {
	conflicts := doltdb.NewTableNameSet(nil)
	if roots.Head == nil {
		roots.Working = branchRoot
		roots.Staged = branchRoot
		roots.Head = branchRoot
		return roots, nil
	}

	if !force {
		if err := CheckoutWouldOverwriteUncommittedTables(ctx, roots, branchRoot); err != nil {
			return doltdb.Roots{}, err
		}
	}
	// The carry step below drops source tables whose name already exists on the destination,
	// so the destination version wins when |force| skipped the conflict check.

	// Snapshot the pre-checkout roots before the three-way merge below reassigns roots.Working
	// and roots.Staged so the carry step still sees the original values.
	preCheckout := roots

	wrkTblHashes, err := threeWayMergeTableHashes(ctx, roots.Head, branchRoot, roots.Working, conflicts, force)
	if err != nil {
		return doltdb.Roots{}, err
	}

	stgTblHashes, err := threeWayMergeTableHashes(ctx, roots.Head, branchRoot, roots.Staged, conflicts, force)
	if err != nil {
		return doltdb.Roots{}, err
	}

	if conflicts.Size() > 0 {
		return doltdb.Roots{}, ErrCheckoutWouldOverwrite{LocalChangeTables: conflicts.AsStringSlice()}
	}

	workingForeignKeys, err := threeWayMergeForeignKeys(ctx, roots.Head, branchRoot, roots.Working, force)
	if err != nil {
		return doltdb.Roots{}, err
	}

	stagedForeignKeys, err := threeWayMergeForeignKeys(ctx, roots.Head, branchRoot, roots.Staged, force)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Working, err = writeTableHashes(ctx, branchRoot, wrkTblHashes)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged, err = writeTableHashes(ctx, branchRoot, stgTblHashes)
	if err != nil {
		return doltdb.Roots{}, err
	}

	// Put the merged collections first so CarryUncommittedTables layers untracked keys on top.
	roots.Working, err = roots.Working.PutForeignKeyCollection(ctx, workingForeignKeys)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged, err = roots.Staged.PutForeignKeyCollection(ctx, stagedForeignKeys)
	if err != nil {
		return doltdb.Roots{}, err
	}

	// Ignored tables stay on the source branch because dolt_ignore is branch-scoped.
	roots.Working, err = CarryUncommittedTables(ctx, preCheckout.Working, preCheckout.Head, roots.Working, ExcludeIgnored)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged, err = CarryUncommittedTables(ctx, preCheckout.Staged, preCheckout.Head, roots.Staged, ExcludeIgnored)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Head = branchRoot
	return roots, nil
}

// CleanOldWorkingSet resets the source branch's working set to its head so uncommitted
// changes do not remain after a checkout has moved them onto the destination branch.
func CleanOldWorkingSet(
	ctx *sql.Context,
	dbData env.DbData[*sql.Context],
	doltDb *doltdb.DoltDB,
	username, email string,
	initialRoots doltdb.Roots,
	initialHeadRef ref.DoltRef,
	initialWs *doltdb.WorkingSet,
) error {
	// reset the source branch's working set to the branch head, leaving the source branch unchanged
	err := ResetHard(ctx, dbData, doltDb, username, email, "", initialRoots, initialHeadRef, initialWs)
	if err != nil {
		return err
	}

	// Annoyingly, after the ResetHard above we need to get all the roots again, because the working set has changed
	cm, err := doltDb.ResolveCommitRef(ctx, initialHeadRef)
	if err != nil {
		return err
	}

	headRoot, err := cm.ResolveRootValue(ctx)
	if err != nil {
		return err
	}

	workingSet, err := doltDb.ResolveWorkingSet(ctx, initialWs.Ref())
	if err != nil {
		return err
	}

	resetRoots := doltdb.Roots{
		Head:    headRoot,
		Working: workingSet.WorkingRoot(),
		Staged:  workingSet.StagedRoot(),
	}

	// we also have to do a clean, because we the ResetHard won't touch any new tables (tables only in the working set).
	// Respect ignore rules so dolt_ignore-matched tables stay on the source branch per the documented policy.
	newRoots, err := CleanUntracked(ctx, resetRoots, []string{}, false, true, true)
	if err != nil {
		return err
	}

	h, err := workingSet.HashOf()
	if err != nil {
		return err
	}

	err = doltDb.UpdateWorkingSet(
		ctx,
		initialWs.Ref(),
		initialWs.WithWorkingRoot(newRoots.Working).WithStagedRoot(newRoots.Staged).ClearMerge().ClearRebase(),
		h,

		&datas.WorkingSetMeta{
			Name:        username,
			Email:       email,
			Timestamp:   uint64(time.Now().Unix()),
			Description: "reset hard",
		},
		nil,
	)
	if err != nil {
		return err
	}
	return nil
}

// BranchHeadRoot returns the root value at the branch head with the name given
func BranchHeadRoot(ctx context.Context, db *doltdb.DoltDB, brName string) (doltdb.RootValue, error) {
	cs, err := doltdb.NewCommitSpec(brName)
	if err != nil {
		return nil, doltdb.RootValueUnreadable{RootType: doltdb.HeadRoot, Cause: err}
	}

	optCmt, err := db.Resolve(ctx, cs, nil)
	if err != nil {
		return nil, doltdb.RootValueUnreadable{RootType: doltdb.HeadRoot, Cause: err}
	}

	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	branchRoot, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}
	return branchRoot, nil
}

// threeWayMergeTableHashes performs a 3-way merge of per-table hashes across |oldRoot|,
// |newRoot|, and |changedRoot|. Each table picks the side that changed against |oldRoot|, or
// |newRoot| when |force| is true. Tables changed on both sides go into |conflicts| so the
// caller can surface ErrCheckoutWouldOverwrite. Uncommitted tables are skipped here and
// handled by [CarryUncommittedTables].
func threeWayMergeTableHashes(ctx context.Context, oldRoot, newRoot, changedRoot doltdb.RootValue, conflicts *doltdb.TableNameSet, force bool) (map[doltdb.TableName]hash.Hash, error) {
	resultMap := make(map[doltdb.TableName]hash.Hash)
	tblNames, err := doltdb.UnionTableNames(ctx, newRoot)
	if err != nil {
		return nil, err
	}

	for _, tblName := range tblNames {
		oldHash, _, err := oldRoot.GetTableHash(ctx, tblName)
		if err != nil {
			return nil, err
		}

		newHash, _, err := newRoot.GetTableHash(ctx, tblName)
		if err != nil {
			return nil, err
		}

		changedHash, _, err := changedRoot.GetTableHash(ctx, tblName)
		if err != nil {
			return nil, err
		}

		if oldHash == changedHash {
			resultMap[tblName] = newHash
		} else if oldHash == newHash {
			resultMap[tblName] = changedHash
		} else if force {
			resultMap[tblName] = newHash
		} else {
			conflicts.Add(tblName)
		}
	}

	tblNames, err = doltdb.UnionTableNames(ctx, changedRoot)
	if err != nil {
		return nil, err
	}

	for _, tblName := range tblNames {
		if _, exists := resultMap[tblName]; !exists {
			oldHash, _, err := oldRoot.GetTableHash(ctx, tblName)
			if err != nil {
				return nil, err
			}

			changedHash, _, err := changedRoot.GetTableHash(ctx, tblName)
			if err != nil {
				return nil, err
			}

			// Skip uncommitted tables here so CarryUncommittedTables can pick them up after
			// the merged tracked state lands on the destination. Carry needs that final state
			// to detect column tag collisions and to rewrite foreign key references against
			// the destination's parent schemas.
			if oldHash == emptyHash {
				continue
			} else if force {
				resultMap[tblName] = oldHash
			} else if oldHash != changedHash {
				conflicts.Add(tblName)
			}
		}
	}

	return resultMap, nil
}

// CheckOverwrittenIgnoredTables returns an error if |overwriteIgnore| is false and any ignored
// tables in |roots.Working| would be overwritten by checking out |branchRoot|.
func CheckOverwrittenIgnoredTables(ctx context.Context, roots doltdb.Roots, branchRoot doltdb.RootValue, overwriteIgnore bool) error {
	if overwriteIgnore {
		return nil
	}

	workingTables, err := doltdb.UnionTableNames(ctx, roots.Working)
	if err != nil {
		return err
	}

	ignoredTables, err := doltdb.IdentifyIgnoredTables(ctx, roots, workingTables)
	if err != nil {
		return err
	}

	if len(ignoredTables) == 0 {
		return nil
	}

	var overwritten []string
	for _, tbl := range ignoredTables {
		currentHash, _, err := roots.Working.GetTableHash(ctx, tbl)
		if err != nil {
			return err
		}

		newHash, _, err := branchRoot.GetTableHash(ctx, tbl)
		if err != nil {
			return err
		}

		// Only flag an overwrite when the target branch has the table
		// with different content. Per Git's docs, --no-overwrite-ignore
		// aborts "when the new branch contains ignored files," so if
		// the target branch does not have the table (empty hash) there
		// is nothing to overwrite the local copy.
		if currentHash != newHash && !newHash.IsEmpty() {
			overwritten = append(overwritten, tbl.String())
		}
	}

	if len(overwritten) > 0 {
		return ErrCheckoutWouldOverwriteIgnoredTables.New(strings.Join(overwritten, "\n\t"))
	}
	return nil
}

// threeWayMergeForeignKeys performs a 3-way merge of the foreign key collections from |oldRoot|,
// |newRoot|, and |changedRoot|. If one side did not change the collection it returns the other,
// and otherwise delegates to [mergeForeignKeyChanges] to merge changes from both sides.
func threeWayMergeForeignKeys(ctx context.Context, oldRoot, newRoot, changedRoot doltdb.RootValue, force bool) (*doltdb.ForeignKeyCollection, error) {
	oldFks, err := oldRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	newFks, err := newRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	changedFks, err := changedRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	oldHash, err := oldFks.HashOf(ctx, oldRoot.VRW())
	if err != nil {
		return nil, err
	}

	newHash, err := newFks.HashOf(ctx, newRoot.VRW())
	if err != nil {
		return nil, err
	}

	changedHash, err := changedFks.HashOf(ctx, changedRoot.VRW())
	if err != nil {
		return nil, err
	}

	if oldHash == changedHash {
		return newFks, nil
	} else if oldHash == newHash {
		return changedFks, nil
	} else {
		// Both roots have modified the foreign keys. We need to do more work to merge them together into a new foreign
		// key collection.
		return mergeForeignKeyChanges(ctx, oldFks, newRoot, newFks, changedRoot, changedFks, force)
	}
}

// mergeForeignKeyChanges merges the foreign key changes from the old and changed roots into a new foreign key
// collection, or returns an error if the changes are incompatible. Changes are incompatible if the changed root
// and new root both altered foreign keys on the same table.
func mergeForeignKeyChanges(
	ctx context.Context,
	oldFks *doltdb.ForeignKeyCollection,
	newRoot doltdb.RootValue,
	newFks *doltdb.ForeignKeyCollection,
	changedRoot doltdb.RootValue,
	changedFks *doltdb.ForeignKeyCollection,
	force bool,
) (*doltdb.ForeignKeyCollection, error) {
	fksByTable := make(map[doltdb.TableName][]doltdb.ForeignKey)

	conflicts := doltdb.NewTableNameSet(nil)

	err := newRoot.IterTables(ctx, func(tblName doltdb.TableName, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		oldFksForTable, _ := oldFks.KeysForTable(tblName)
		newFksForTable, _ := newFks.KeysForTable(tblName)
		changedFksForTable, _ := changedFks.KeysForTable(tblName)

		oldHash, err := doltdb.CombinedHash(oldFksForTable)
		if err != nil {
			return true, err
		}
		newHash, err := doltdb.CombinedHash(newFksForTable)
		if err != nil {
			return true, err
		}
		changedHash, err := doltdb.CombinedHash(changedFksForTable)
		if err != nil {
			return true, err
		}

		if oldHash == changedHash {
			fksByTable[tblName] = append(fksByTable[tblName], newFksForTable...)
		} else if oldHash == newHash {
			fksByTable[tblName] = append(fksByTable[tblName], changedFksForTable...)
		} else if force {
			fksByTable[tblName] = append(fksByTable[tblName], newFksForTable...)
		} else {
			conflicts.Add(tblName)
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	err = changedRoot.IterTables(ctx, func(tblName doltdb.TableName, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		if _, exists := fksByTable[tblName]; !exists {
			oldKeys, _ := oldFks.KeysForTable(tblName)
			oldHash, err := doltdb.CombinedHash(oldKeys)
			if err != nil {
				return true, err
			}

			changedKeys, _ := changedFks.KeysForTable(tblName)
			changedHash, err := doltdb.CombinedHash(changedKeys)
			if err != nil {
				return true, err
			}

			if oldHash == emptyHash {
				fksByTable[tblName] = append(fksByTable[tblName], changedKeys...)
			} else if force {
				fksByTable[tblName] = append(fksByTable[tblName], oldKeys...)
			} else if oldHash != changedHash {
				conflicts.Add(tblName)
			}
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	if conflicts.Size() > 0 {
		return nil, ErrCheckoutWouldOverwrite{LocalChangeTables: conflicts.AsStringSlice()}
	}

	fks := make([]doltdb.ForeignKey, 0)
	for _, v := range fksByTable {
		fks = append(fks, v...)
	}

	return doltdb.NewForeignKeyCollection(fks...)
}

// writeTableHashes writes new table hash values for the root given and returns it.
// This is an inexpensive and convenient way of replacing all the tables at once.
func writeTableHashes(ctx context.Context, head doltdb.RootValue, tblHashes map[doltdb.TableName]hash.Hash) (doltdb.RootValue, error) {
	names, err := doltdb.UnionTableNames(ctx, head)
	if err != nil {
		return nil, err
	}

	var toDrop []doltdb.TableName
	for _, name := range names {
		if _, ok := tblHashes[name]; !ok {
			toDrop = append(toDrop, name)
		}
	}

	head, err = head.RemoveTables(ctx, false, false, toDrop...)
	if err != nil {
		return nil, err
	}

	for k, v := range tblHashes {
		if v == emptyHash {
			continue
		}

		head, err = head.SetTableHash(ctx, k, v)
		if err != nil {
			return nil, err
		}
	}

	return head, nil
}

// CheckoutWouldOverwriteWorkingSetChanges reports whether a checkout would overwrite
// uncommitted changes on the destination. System and dolt_ignore-matched tables are excluded.
// When both sides hold the same uncommitted changes the checkout would not lose anything, so
// it is not reported as an overwrite.
func CheckoutWouldOverwriteWorkingSetChanges(ctx context.Context, sourceRoots, destRoots doltdb.Roots) (bool, error) {
	srcW, srcS, srcH, err := trackedRootHashes(ctx, sourceRoots)
	if err != nil {
		return false, err
	}
	if maps.Equal(srcW, srcH) && maps.Equal(srcS, srcH) {
		return false, nil
	}
	destW, destS, destH, err := trackedRootHashes(ctx, destRoots)
	if err != nil {
		return false, err
	}
	if maps.Equal(destW, destH) && maps.Equal(destS, destH) {
		return false, nil
	}
	return !maps.Equal(srcW, destW) || !maps.Equal(srcS, destS), nil
}

// trackedRootHashes returns name to hash maps for working, staged, and head of |roots|, keeping
// only the tables that buildTrackedPredicate accepts. Read-only system tables and dolt_ignore
// matched tables are excluded, while writable system tables such as dolt_ignore are kept.
func trackedRootHashes(ctx context.Context, roots doltdb.Roots) (working, staged, head map[doltdb.TableName]hash.Hash, err error) {
	isTracked, err := buildTrackedPredicate(ctx, roots.Working)
	if err != nil {
		return nil, nil, nil, err
	}
	hashes := func(root doltdb.RootValue) (map[doltdb.TableName]hash.Hash, error) {
		names, err := root.GetAllTableNames(ctx, false)
		if err != nil {
			return nil, err
		}
		out := make(map[doltdb.TableName]hash.Hash, len(names))
		for _, n := range names {
			tracked, err := isTracked(n)
			if err != nil {
				return nil, err
			}
			if !tracked {
				continue
			}
			h, _, err := root.GetTableHash(ctx, n)
			if err != nil {
				return nil, err
			}
			out[n] = h
		}
		return out, nil
	}
	working, err = hashes(roots.Working)
	if err != nil {
		return nil, nil, nil, err
	}
	staged, err = hashes(roots.Staged)
	if err != nil {
		return nil, nil, nil, err
	}
	head, err = hashes(roots.Head)
	if err != nil {
		return nil, nil, nil, err
	}
	return working, staged, head, nil
}

// buildTrackedPredicate returns a function that reports whether a table is tracked for the
// purpose of detecting uncommitted-change conflicts. It returns false for read-only system
// tables and for tables matched by a dolt_ignore pattern on |root|. Writable system tables
// such as dolt_ignore are tracked so the conflict check covers the same tables the carry step
// acts on.
func buildTrackedPredicate(ctx context.Context, root doltdb.RootValue) (func(doltdb.TableName) (bool, error), error) {
	names, err := root.GetAllTableNames(ctx, false)
	if err != nil {
		return nil, err
	}
	schemas := doltdb.GetUniqueSchemaNamesFromTableNames(names)
	patternsBySchema, err := doltdb.GetIgnoredTablePatterns(ctx, doltdb.Roots{Working: root, Staged: root, Head: root}, schemas)
	if err != nil {
		return nil, err
	}
	return func(n doltdb.TableName) (bool, error) {
		if doltdb.IsReadOnlySystemTable(n) {
			return false, nil
		}
		patterns := patternsBySchema[n.Schema]
		result, ignoreErr := patterns.IsTableNameIgnored(n)
		if doltdb.AsDoltIgnoreInConflict(ignoreErr) != nil {
			return true, nil
		}
		if ignoreErr != nil {
			return false, ignoreErr
		}
		return result != doltdb.Ignore, nil
	}, nil
}

// ClearFeatureVersion creates a new version of the provided roots where all three roots have the same
// feature version. By hashing these new roots, we can easily determine whether the roots differ only by
// their feature version.
func ClearFeatureVersion(ctx context.Context, roots doltdb.Roots) (doltdb.Roots, error) {
	currentBranchFeatureVersion, _, err := roots.Head.GetFeatureVersion(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}

	modifiedWorking, err := roots.Working.SetFeatureVersion(currentBranchFeatureVersion)
	if err != nil {
		return doltdb.Roots{}, err
	}

	modifiedStaged, err := roots.Staged.SetFeatureVersion(currentBranchFeatureVersion)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return doltdb.Roots{
		Head:    roots.Head,
		Working: modifiedWorking,
		Staged:  modifiedStaged,
	}, nil
}

// RootHasUncommittedChanges returns whether the roots given have uncommitted changes, and the hashes of
// the working and staged roots are identical. This function will ignore any difference in feature
// versions between the root values.
func RootHasUncommittedChanges(roots doltdb.Roots) (hasChanges bool, workingHash hash.Hash, stagedHash hash.Hash, err error) {
	roots, err = ClearFeatureVersion(context.Background(), roots)
	if err != nil {
		return false, hash.Hash{}, hash.Hash{}, err
	}

	headHash, err := roots.Head.HashOf()
	if err != nil {
		return false, hash.Hash{}, hash.Hash{}, err
	}

	workingHash, err = roots.Working.HashOf()
	if err != nil {
		return false, hash.Hash{}, hash.Hash{}, err
	}

	stagedHash, err = roots.Staged.HashOf()
	if err != nil {
		return false, hash.Hash{}, hash.Hash{}, err
	}

	hasChanges = workingHash != stagedHash || stagedHash != headHash
	return hasChanges, workingHash, stagedHash, nil
}
