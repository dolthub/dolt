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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/go-mysql-server/sql"
)

const (
	maxTxCommitRetries = 5
)

type DoltTransaction struct {
	startRoot *doltdb.RootValue
	workingSet ref.WorkingSetRef
	db *doltdb.DoltDB
}

func NewDoltTransaction(startRoot *doltdb.RootValue, workingSet ref.WorkingSetRef, db *doltdb.DoltDB) *DoltTransaction {
	return &DoltTransaction{startRoot: startRoot, workingSet: workingSet, db: db}
}

func (tx DoltTransaction) String() string {
	return ""
}

// Commit attempts to merge newRoot into the working set
// Uses the same algorithm as merge.Merger:
// |ws.root| is the root
// |newRoot| is the mergeRoot
// |tx.startRoot| is ancRoot
// if working set == ancRoot, attempt a fast-forward merge
func (tx * DoltTransaction) Commit(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	for i := 0; i < maxTxCommitRetries; i++ {
		ws, err := tx.db.ResolveWorkingSet(ctx, tx.workingSet)
		if err != nil {
			return err
		}

		root := ws.RootValue()

		hash, err := ws.Struct().Hash(tx.db.Format())
		if err != nil {
			return err
		}

		if rootsEqual(root, tx.startRoot) {
			// ff merge
			err = tx.db.UpdateWorkingSet(ctx, tx.workingSet, newRoot, hash)
			if err == datas.ErrOptimisticLockFailed {
				continue
			}
		}

		mergedRoot, _, err := merge.MergeRoots(ctx, root, newRoot, tx.startRoot)
		if err != nil {
			return err
		}

		err = tx.db.UpdateWorkingSet(ctx, tx.workingSet, mergedRoot, hash)
		if err == datas.ErrOptimisticLockFailed {
			continue
		}

		return err
	}

	// TODO: different error type for retries exhausted
	return datas.ErrOptimisticLockFailed
}

func rootsEqual(left, right *doltdb.RootValue) bool {
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