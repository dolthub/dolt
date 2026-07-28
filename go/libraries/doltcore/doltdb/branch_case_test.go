// Copyright 2026 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

// newCaseVariantDB returns a database holding only main, and the commit new branches should start at.
func newCaseVariantDB(t *testing.T) (*DoltDB, context.Context, *Commit) {
	t.Helper()
	ctx := context.Background()
	ddb, err := LoadDoltDB(ctx, types.Format_DOLT, InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	t.Cleanup(func() { ddb.Close() })
	require.NoError(t, ddb.WriteEmptyRepo(ctx, "main", "Bill Billerson", "bigbillieb@fake.horse"))

	cs, err := NewCommitSpec("main")
	require.NoError(t, err)
	optCmt, err := ddb.Resolve(ctx, cs, nil)
	require.NoError(t, err)
	commit, ok := optCmt.ToCommit()
	require.True(t, ok)
	return ddb, ctx, commit
}

// branchNames returns the sorted paths of every branch in ddb.
func branchNames(ctx context.Context, t *testing.T, ddb *DoltDB) []string {
	t.Helper()
	branches, err := ddb.GetBranches(ctx)
	require.NoError(t, err)
	names := make([]string, len(branches))
	for i, b := range branches {
		names[i] = b.GetPath()
	}
	sort.Strings(names)
	return names
}

func TestCaseVariantBranchLosingAnUpdateRaceIsRejected(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11270
	ddb, ctx, base := newCaseVariantDB(t)

	// Commit "br" between the update below reading the dataset map and committing it, the interleaving
	// that costs the update its commit and makes it run again. Only the first run commits, so the rerun
	// is left free to finish and has to reject "BR" against the reread map.
	committedBr := false
	commitBrMidUpdate := datas.PreUpdateCheck(func(ctx context.Context, _ prolly.AddressMap, _ string) error {
		if !committedBr {
			committedBr = true
			require.NoError(t, ddb.NewBranchAtCommit(ctx, ref.NewBranchRef("br"), base, nil))
		}
		return nil
	})

	db := ExposeDatabaseFromDoltDB(ddb)
	ds, err := db.GetDataset(ctx, ref.NewBranchRef("BR").String())
	require.NoError(t, err)
	addr, err := base.HashOf()
	require.NoError(t, err)

	_, err = db.SetHead(ctx, ds, addr, "", commitBrMidUpdate, failOnCaseConflict())
	var existing *ExistingRefError
	require.ErrorAs(t, err, &existing)
	require.Equal(t, "br", existing.Ref.GetPath())
	require.Equal(t, []string{"br", "main"}, branchNames(ctx, t, ddb))
}

func TestConcurrentCaseVariantBranchCreation(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11270
	ddb, ctx, base := newCaseVariantDB(t)

	// Run the creation path from two goroutines so the race detector has something to observe. Which
	// one wins is left to the scheduler because the assertions below hold for either outcome.
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, name := range []string{"br", "BR"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = ddb.NewBranchAtCommit(ctx, ref.NewBranchRef(name), base, nil)
		}()
	}
	wg.Wait()

	require.Len(t, branchNames(ctx, t, ddb), 2)

	rejected := 0
	for _, e := range errs {
		if e != nil {
			var existing *ExistingRefError
			require.ErrorAs(t, e, &existing)
			rejected++
		}
	}
	require.Equal(t, 1, rejected)
}

func TestAmbiguousRefNameReadFailsLoud(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11270
	ddb, ctx, base := newCaseVariantDB(t)

	for _, name := range []string{"br", "BR", "Br"} {
		require.NoError(t, ddb.NewBranchAtCommitUnchecked(ctx, ref.NewBranchRef(name), base, nil))
	}

	// The fourth casing matches no branch exactly and still folds onto all three.
	for _, name := range []string{"br", "BR", "Br", "bR"} {
		match, err := ddb.GetRefByNameInsensitive(ctx, name)
		require.Nil(t, match)
		require.ErrorIs(t, err, ErrAmbiguousRefName)
		require.ErrorContains(t, err, fmt.Sprintf("%q could be BR, Br, br", name))
	}
}
