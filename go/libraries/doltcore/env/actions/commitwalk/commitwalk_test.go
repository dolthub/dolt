// Copyright 2019 Dolthub, Inc.
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

package commitwalk

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	testHomeDir = "/doesnotexist/home"
	workingDir  = "/doesnotexist/work"
)

var lastUserTSMillis int64

func MonotonicNow() time.Time {
	now := time.Now()
	millis := now.UnixMilli()
	if millis <= lastUserTSMillis {
		now = time.UnixMilli(lastUserTSMillis).Add(time.Millisecond)
	}
	lastUserTSMillis = now.UnixMilli()
	return now
}

func testHomeDirFunc() (string, error) {
	return testHomeDir, nil
}

func createUninitializedEnv() *env.DoltEnv {
	initialDirs := []string{testHomeDir, workingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, workingDir)
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")
	return dEnv
}

func TestGetDotDotRevisions(t *testing.T) {
	dEnv := createUninitializedEnv()
	err := dEnv.InitRepo(context.Background(), types.Format_Default, "Bill Billerson", "bill@billerson.com", env.DefaultInitBranch)
	require.NoError(t, err)

	cs, err := doltdb.NewCommitSpec(env.DefaultInitBranch)
	require.NoError(t, err)
	opt, err := dEnv.DoltDB(ctx).Resolve(context.Background(), cs, nil)
	require.NoError(t, err)
	commit, ok := opt.ToCommit()
	require.True(t, ok)

	rv, err := commit.GetRootValue(context.Background())
	require.NoError(t, err)
	r, rvh, err := dEnv.DoltDB(ctx).WriteRootValue(context.Background(), rv)
	require.NoError(t, err)
	rv = r

	// Create 5 commits on main.
	mainCommits := make([]*doltdb.Commit, 6)
	mainCommits[0] = commit
	for i := 1; i < 6; i++ {
		mainCommits[i] = mustCreateCommit(t, dEnv.DoltDB(ctx), env.DefaultInitBranch, rvh, mainCommits[i-1])
	}

	// Create a feature branch.
	bref := ref.NewBranchRef("feature")
	err = dEnv.DoltDB(ctx).NewBranchAtCommit(context.Background(), bref, mainCommits[5], nil)
	require.NoError(t, err)

	// Create 3 commits on feature branch.
	featureCommits := []*doltdb.Commit{}
	featureCommits = append(featureCommits, mainCommits[5])
	for i := 1; i < 4; i++ {
		featureCommits = append(featureCommits, mustCreateCommit(t, dEnv.DoltDB(ctx), "feature", rvh, featureCommits[i-1]))
	}

	// Create 1 commit on main.
	mainCommits = append(mainCommits, mustCreateCommit(t, dEnv.DoltDB(ctx), env.DefaultInitBranch, rvh, mainCommits[5]))

	// Merge main to feature branch.
	featureCommits = append(featureCommits, mustCreateCommit(t, dEnv.DoltDB(ctx), "feature", rvh, featureCommits[3], mainCommits[6]))

	// Create 3 commits on feature branch.
	for i := 5; i < 8; i++ {
		featureCommits = append(featureCommits, mustCreateCommit(t, dEnv.DoltDB(ctx), "feature", rvh, featureCommits[i-1]))
	}

	// Create 3 commits on main.
	for i := 7; i < 10; i++ {
		mainCommits = append(mainCommits, mustCreateCommit(t, dEnv.DoltDB(ctx), env.DefaultInitBranch, rvh, mainCommits[i-1]))
	}

	// Branches look like this:
	//
	//                       feature:  F1--F2--F3--F4--F5--F6--F7
	//                                /            /
	// main: M0--M1--M2--M3--M4--M5/F0------------M6--M7--M8--M9

	featureHash := mustGetHash(t, featureCommits[7])
	mainHash := mustGetHash(t, mainCommits[6])
	featurePreMergeHash := mustGetHash(t, featureCommits[3])

	res, err := GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{featureHash}, dEnv.DoltDB(ctx), []hash.Hash{mainHash}, 100)
	require.NoError(t, err)

	assert.Len(t, res, 7)
	assertEqualHashes(t, featureCommits[7], res[0])
	assertEqualHashes(t, featureCommits[6], res[1])
	assertEqualHashes(t, featureCommits[5], res[2])
	assertEqualHashes(t, featureCommits[4], res[3])
	assertEqualHashes(t, featureCommits[3], res[4])
	assertEqualHashes(t, featureCommits[2], res[5])
	assertEqualHashes(t, featureCommits[1], res[6])

	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{mainHash}, dEnv.DoltDB(ctx), []hash.Hash{featureHash}, 100)
	require.NoError(t, err)
	assert.Len(t, res, 0)

	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{featureHash}, dEnv.DoltDB(ctx), []hash.Hash{mainHash}, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[7], res[0])
	assertEqualHashes(t, featureCommits[6], res[1])
	assertEqualHashes(t, featureCommits[5], res[2])

	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{featurePreMergeHash}, dEnv.DoltDB(ctx), []hash.Hash{mainHash}, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[3], res[0])
	assertEqualHashes(t, featureCommits[2], res[1])
	assertEqualHashes(t, featureCommits[1], res[2])

	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{featurePreMergeHash}, dEnv.DoltDB(ctx), []hash.Hash{mainHash}, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[3], res[0])
	assertEqualHashes(t, featureCommits[2], res[1])
	assertEqualHashes(t, featureCommits[1], res[2])

	// Three dot
	mergeBaseHash, err := merge.MergeBase(context.Background(), mainCommits[6], featureCommits[7])
	require.NoError(t, err)

	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{featureHash, mainHash}, dEnv.DoltDB(ctx), []hash.Hash{mergeBaseHash}, -1)
	require.NoError(t, err)
	assert.Len(t, res, 7)
	assertEqualHashes(t, featureCommits[7], res[0])
	assertEqualHashes(t, featureCommits[6], res[1])
	assertEqualHashes(t, featureCommits[5], res[2])
	assertEqualHashes(t, featureCommits[4], res[3])
	assertEqualHashes(t, featureCommits[3], res[4])
	assertEqualHashes(t, featureCommits[2], res[5])
	assertEqualHashes(t, featureCommits[1], res[6])

	mergeBaseHash, err = merge.MergeBase(context.Background(), mainCommits[6], featureCommits[3])
	require.NoError(t, err)

	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{featurePreMergeHash, mainHash}, dEnv.DoltDB(ctx), []hash.Hash{mergeBaseHash}, -1)
	require.NoError(t, err)
	assert.Len(t, res, 4)
	assertEqualHashes(t, featureCommits[3], res[0])
	assertEqualHashes(t, featureCommits[2], res[1])
	assertEqualHashes(t, mainCommits[6], res[2])
	assertEqualHashes(t, featureCommits[1], res[3])

	// Create a similar branch to "feature" on a forked repository and GetDotDotRevisions using that as well.
	forkEnv := mustForkDB(t, dEnv.DoltDB(ctx), "feature", featureCommits[4])

	// Create 3 commits on feature branch.
	for i := 5; i < 8; i++ {
		featureCommits[i] = mustCreateCommit(t, forkEnv.DoltDB(ctx), "feature", rvh, featureCommits[i-1])
	}

	featureHash = mustGetHash(t, featureCommits[7])
	mainHash = mustGetHash(t, mainCommits[6])
	featurePreMergeHash = mustGetHash(t, featureCommits[3])

	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{featureHash}, dEnv.DoltDB(ctx), []hash.Hash{mainHash}, 100)
	require.Error(t, err)
	res, err = GetDotDotRevisions(context.Background(), forkEnv.DoltDB(ctx), []hash.Hash{featureHash}, dEnv.DoltDB(ctx), []hash.Hash{mainHash}, 100)
	require.NoError(t, err)
	assert.Len(t, res, 7)
	assertEqualHashes(t, featureCommits[7], res[0])
	assertEqualHashes(t, featureCommits[6], res[1])
	assertEqualHashes(t, featureCommits[5], res[2])
	assertEqualHashes(t, featureCommits[4], res[3])
	assertEqualHashes(t, featureCommits[3], res[4])
	assertEqualHashes(t, featureCommits[2], res[5])
	assertEqualHashes(t, featureCommits[1], res[6])

	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{mainHash}, dEnv.DoltDB(ctx), []hash.Hash{featureHash}, 100)
	require.Error(t, err)
	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{mainHash}, forkEnv.DoltDB(ctx), []hash.Hash{featureHash}, 100)
	require.NoError(t, err)
	assert.Len(t, res, 0)

	res, err = GetDotDotRevisions(context.Background(), forkEnv.DoltDB(ctx), []hash.Hash{featureHash}, dEnv.DoltDB(ctx), []hash.Hash{mainHash}, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[7], res[0])
	assertEqualHashes(t, featureCommits[6], res[1])
	assertEqualHashes(t, featureCommits[5], res[2])

	res, err = GetDotDotRevisions(context.Background(), dEnv.DoltDB(ctx), []hash.Hash{featurePreMergeHash}, dEnv.DoltDB(ctx), []hash.Hash{mainHash}, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[3], res[0])
	assertEqualHashes(t, featureCommits[2], res[1])
	assertEqualHashes(t, featureCommits[1], res[2])

	res, err = GetDotDotRevisions(context.Background(), forkEnv.DoltDB(ctx), []hash.Hash{featurePreMergeHash}, dEnv.DoltDB(ctx), []hash.Hash{mainHash}, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[3], res[0])
	assertEqualHashes(t, featureCommits[2], res[1])
	assertEqualHashes(t, featureCommits[1], res[2])
}

func assertEqualHashes(t *testing.T, lc, rc interface{}) {
	leftCm, ok := lc.(*doltdb.Commit)
	if !ok {
		opt, ok := lc.(*doltdb.OptionalCommit)
		require.True(t, ok)
		leftCm, ok = opt.ToCommit()
		require.True(t, ok)
	}

	rightCm, ok := rc.(*doltdb.Commit)
	if !ok {
		opt, ok := rc.(*doltdb.OptionalCommit)
		require.True(t, ok)
		rightCm, ok = opt.ToCommit()
		require.True(t, ok)
	}

	assert.Equal(t, mustGetHash(t, leftCm), mustGetHash(t, rightCm))
}

func mustCreateCommit(t *testing.T, ddb *doltdb.DoltDB, bn string, rvh hash.Hash, parents ...*doltdb.Commit) *doltdb.Commit {
	cm, err := datas.NewCommitMetaWithUserTS("Bill Billerson", "bill@billerson.com", "A New Commit.", MonotonicNow())
	require.NoError(t, err)
	pcs := make([]*doltdb.CommitSpec, 0, len(parents))
	for _, parent := range parents {
		cs, err := doltdb.NewCommitSpec(mustGetHash(t, parent).String())
		require.NoError(t, err)
		pcs = append(pcs, cs)
	}
	bref := ref.NewBranchRef(bn)
	commit, err := ddb.CommitWithParentSpecs(context.Background(), rvh, bref, pcs, cm)
	require.NoError(t, err)
	return commit
}

func mustForkDB(t *testing.T, fromDB *doltdb.DoltDB, bn string, cm *doltdb.Commit) *env.DoltEnv {
	h, err := cm.HashOf()
	require.NoError(t, err)
	forkEnv := createUninitializedEnv()
	err = forkEnv.InitRepo(context.Background(), types.Format_Default, "Bill Billerson", "bill@billerson.com", env.DefaultInitBranch)
	require.NoError(t, err)
	ps := make(chan pull.Stats)
	go func() {
		for range ps {
		}
	}()
	err = forkEnv.DoltDB(ctx).PullChunks(context.Background(), "", fromDB, []hash.Hash{h}, ps, nil)
	if err == pull.ErrDBUpToDate {
		err = nil
	}
	require.NoError(t, err)
	err = forkEnv.DoltDB(ctx).SetHead(context.Background(), ref.NewBranchRef(bn), h)
	require.NoError(t, err)
	return forkEnv
}

func mustGetHash(t *testing.T, c *doltdb.Commit) hash.Hash {
	h, err := c.HashOf()
	require.NoError(t, err)
	return h
}
