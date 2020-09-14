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

package commitwalk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/datas"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	testHomeDir = "/doesnotexist/home"
	workingDir  = "/doesnotexist/work"
)

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
	env := createUninitializedEnv()
	err := env.InitRepo(context.Background(), types.Format_LD_1, "Bill Billerson", "bill@billerson.com")
	require.NoError(t, err)

	cs, err := doltdb.NewCommitSpec("master")
	require.NoError(t, err)
	commit, err := env.DoltDB.Resolve(context.Background(), cs, nil)
	require.NoError(t, err)

	rv, err := commit.GetRootValue()
	require.NoError(t, err)
	rvh, err := env.DoltDB.WriteRootValue(context.Background(), rv)
	require.NoError(t, err)

	// Create 5 commits on master.
	masterCommits := make([]*doltdb.Commit, 6)
	masterCommits[0] = commit
	for i := 1; i < 6; i++ {
		masterCommits[i] = mustCreateCommit(t, env.DoltDB, "master", rvh, masterCommits[i-1])
	}

	// Create a feature branch.
	bref := ref.NewBranchRef("feature")
	err = env.DoltDB.NewBranchAtCommit(context.Background(), bref, masterCommits[5])
	require.NoError(t, err)

	// Create 3 commits on feature branch.
	featureCommits := []*doltdb.Commit{}
	featureCommits = append(featureCommits, masterCommits[5])
	for i := 1; i < 4; i++ {
		featureCommits = append(featureCommits, mustCreateCommit(t, env.DoltDB, "feature", rvh, featureCommits[i-1]))
	}

	// Create 1 commit on master.
	masterCommits = append(masterCommits, mustCreateCommit(t, env.DoltDB, "master", rvh, masterCommits[5]))

	// Merge master to feature branch.
	featureCommits = append(featureCommits, mustCreateCommit(t, env.DoltDB, "feature", rvh, featureCommits[3], masterCommits[6]))

	// Create 3 commits on feature branch.
	for i := 5; i < 8; i++ {
		featureCommits = append(featureCommits, mustCreateCommit(t, env.DoltDB, "feature", rvh, featureCommits[i-1]))
	}

	// Create 3 commits on master.
	for i := 7; i < 10; i++ {
		masterCommits = append(masterCommits, mustCreateCommit(t, env.DoltDB, "master", rvh, masterCommits[i-1]))
	}

	// Branches look like this:
	//
	//               feature:  *--*--*--*--*--*--*
	//                        /        /
	// master: --*--*--*--*--*--------*--*--*--*

	featureHash := mustGetHash(t, featureCommits[7])
	masterHash := mustGetHash(t, masterCommits[6])
	featurePreMergeHash := mustGetHash(t, featureCommits[3])

	res, err := GetDotDotRevisions(context.Background(), env.DoltDB, featureHash, env.DoltDB, masterHash, 100)
	require.NoError(t, err)
	assert.Len(t, res, 7)
	assertEqualHashes(t, featureCommits[7], res[0])
	assertEqualHashes(t, featureCommits[6], res[1])
	assertEqualHashes(t, featureCommits[5], res[2])
	assertEqualHashes(t, featureCommits[4], res[3])
	assertEqualHashes(t, featureCommits[3], res[4])
	assertEqualHashes(t, featureCommits[2], res[5])
	assertEqualHashes(t, featureCommits[1], res[6])

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, masterHash, env.DoltDB, featureHash, 100)
	require.NoError(t, err)
	assert.Len(t, res, 0)

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, featureHash, env.DoltDB, masterHash, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[7], res[0])
	assertEqualHashes(t, featureCommits[6], res[1])
	assertEqualHashes(t, featureCommits[5], res[2])

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, featurePreMergeHash, env.DoltDB, masterHash, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[3], res[0])
	assertEqualHashes(t, featureCommits[2], res[1])
	assertEqualHashes(t, featureCommits[1], res[2])

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, featurePreMergeHash, env.DoltDB, masterHash, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[3], res[0])
	assertEqualHashes(t, featureCommits[2], res[1])
	assertEqualHashes(t, featureCommits[1], res[2])

	// Create a similar branch to "feature" on a forked repository and GetDotDotRevisions using that as well.
	forkEnv := mustForkDB(t, env.DoltDB, "feature", featureCommits[4])

	// Create 3 commits on feature branch.
	for i := 5; i < 8; i++ {
		featureCommits[i] = mustCreateCommit(t, forkEnv.DoltDB, "feature", rvh, featureCommits[i-1])
	}

	featureHash = mustGetHash(t, featureCommits[7])
	masterHash = mustGetHash(t, masterCommits[6])
	featurePreMergeHash = mustGetHash(t, featureCommits[3])

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, featureHash, env.DoltDB, masterHash, 100)
	require.Error(t, err)
	res, err = GetDotDotRevisions(context.Background(), forkEnv.DoltDB, featureHash, env.DoltDB, masterHash, 100)
	require.NoError(t, err)
	assert.Len(t, res, 7)
	assertEqualHashes(t, featureCommits[7], res[0])
	assertEqualHashes(t, featureCommits[6], res[1])
	assertEqualHashes(t, featureCommits[5], res[2])
	assertEqualHashes(t, featureCommits[4], res[3])
	assertEqualHashes(t, featureCommits[3], res[4])
	assertEqualHashes(t, featureCommits[2], res[5])
	assertEqualHashes(t, featureCommits[1], res[6])

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, masterHash, env.DoltDB, featureHash, 100)
	require.Error(t, err)
	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, masterHash, forkEnv.DoltDB, featureHash, 100)
	require.NoError(t, err)
	assert.Len(t, res, 0)

	res, err = GetDotDotRevisions(context.Background(), forkEnv.DoltDB, featureHash, env.DoltDB, masterHash, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[7], res[0])
	assertEqualHashes(t, featureCommits[6], res[1])
	assertEqualHashes(t, featureCommits[5], res[2])

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, featurePreMergeHash, env.DoltDB, masterHash, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[3], res[0])
	assertEqualHashes(t, featureCommits[2], res[1])
	assertEqualHashes(t, featureCommits[1], res[2])

	res, err = GetDotDotRevisions(context.Background(), forkEnv.DoltDB, featurePreMergeHash, env.DoltDB, masterHash, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assertEqualHashes(t, featureCommits[3], res[0])
	assertEqualHashes(t, featureCommits[2], res[1])
	assertEqualHashes(t, featureCommits[1], res[2])
}

func assertEqualHashes(t *testing.T, lc, rc *doltdb.Commit) {
	assert.Equal(t, mustGetHash(t, lc), mustGetHash(t, rc))
}

func mustCreateCommit(t *testing.T, ddb *doltdb.DoltDB, bn string, rvh hash.Hash, parents ...*doltdb.Commit) *doltdb.Commit {
	cm, err := doltdb.NewCommitMeta("Bill Billerson", "bill@billerson.com", "A New Commit.")
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
	stref, err := cm.GetStRef()
	require.NoError(t, err)
	forkEnv := createUninitializedEnv()
	err = forkEnv.InitRepo(context.Background(), types.Format_LD_1, "Bill Billerson", "bill@billerson.com")
	require.NoError(t, err)
	p1 := make(chan datas.PullProgress)
	p2 := make(chan datas.PullerEvent)
	go func() { for range p1{ } }()
	go func() { for range p2{ } }()
	err = forkEnv.DoltDB.PullChunks(context.Background(), "", fromDB, stref, p1, p2)
	require.NoError(t, err)
	err = forkEnv.DoltDB.SetHead(context.Background(), ref.NewBranchRef(bn), stref)
	require.NoError(t, err)
	return forkEnv
}

func mustGetHash(t *testing.T, c *doltdb.Commit) hash.Hash {
	h, err := c.HashOf()
	require.NoError(t, err)
	return h
}
