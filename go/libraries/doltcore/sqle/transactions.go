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

package sqle

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	maxTxCommitRetries = 5
)

type DoltTransaction struct {
	startRoot  *doltdb.RootValue
	workingSet ref.WorkingSetRef
	db         *doltdb.DoltDB
	rsw        env.RepoStateWriter
}

func NewDoltTransaction(startRoot *doltdb.RootValue, workingSet ref.WorkingSetRef, db *doltdb.DoltDB, rsw env.RepoStateWriter) *DoltTransaction {
	return &DoltTransaction{
		startRoot:  startRoot,
		workingSet: workingSet,
		db:         db,
		rsw:        rsw,
	}
}

func (tx DoltTransaction) String() string {
	// TODO: return more info (hashes need caching)
	return "DoltTransaction"
}

// Commit attempts to merge newRoot into the working set
// Uses the same algorithm as merge.Merger:
// |ws.root| is the root
// |newRoot| is the mergeRoot
// |tx.startRoot| is ancRoot
// if working set == ancRoot, attempt a fast-forward merge
func (tx *DoltTransaction) Commit(ctx *sql.Context, newRoot *doltdb.RootValue) (*doltdb.RootValue, error) {
	for i := 0; i < maxTxCommitRetries; i++ {
		ws, err := tx.db.ResolveWorkingSet(ctx, tx.workingSet)
		if err == doltdb.ErrWorkingSetNotFound {
			// initial commit
			err = tx.db.UpdateWorkingSet(ctx, tx.workingSet, newRoot, hash.Hash{})
			if err == datas.ErrOptimisticLockFailed {
				continue
			}
		}

		if err != nil {
			return nil, err
		}

		root := ws.RootValue()

		hash, err := ws.Struct().Hash(tx.db.Format())
		if err != nil {
			return nil, err
		}

		if rootsEqual(root, tx.startRoot) {
			// ff merge
			err = tx.db.UpdateWorkingSet(ctx, tx.workingSet, newRoot, hash)
			if err == datas.ErrOptimisticLockFailed {
				continue
			}

			return newRoot, nil
		}

		mergedRoot, stats, err := merge.MergeRoots(ctx, root, newRoot, tx.startRoot)
		if err != nil {
			return nil, err
		}

		for table, mergeStats := range stats {
			if mergeStats.Conflicts > 0 {
				// TODO: surface duplicate key errors as appropriate
				return nil, fmt.Errorf("conflict in table %s", table)
			}
		}

		err = tx.db.UpdateWorkingSet(ctx, tx.workingSet, mergedRoot, hash)
		if err == datas.ErrOptimisticLockFailed {
			continue
		}

		// TODO: this is not thread safe, but will not be necessary after migrating all clients away from using the
		//  working set stored in repo_state.json, so should be good enough for now
		return tx.updateRepoStateFile(ctx, mergedRoot, err)
	}

	// TODO: different error type for retries exhausted
	return nil, datas.ErrOptimisticLockFailed
}

func (tx *DoltTransaction) updateRepoStateFile(ctx *sql.Context, mergedRoot *doltdb.RootValue, err error) (*doltdb.RootValue, error) {
	hash, err := mergedRoot.HashOf()
	if err != nil {
		return nil, err
	}

	err = tx.rsw.SetWorkingHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	return mergedRoot, err
}

func rootsEqual(left, right *doltdb.RootValue) bool {
	if left == nil || right == nil {
		return false
	}

	lh, err := left.HashOf()
	if err != nil {
		return false
	}

	rh, err := right.HashOf()
	if err != nil {
		return false
	}

	return lh == rh
}
