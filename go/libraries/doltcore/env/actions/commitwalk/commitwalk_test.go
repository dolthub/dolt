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
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB)
	return dEnv
}

func TestGetDotDotRevisions(t *testing.T) {
	env := createUninitializedEnv()
	err := env.InitRepo(context.Background(), types.Format_LD_1, "Bill Billerson", "bill@billerson.com")
	require.NoError(t, err)

	cs, err := doltdb.NewCommitSpec("HEAD", "master")
	require.NoError(t, err)
	commit, err := env.DoltDB.Resolve(context.Background(), cs)
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

	res, err := GetDotDotRevisions(context.Background(), env.DoltDB, featureHash, masterHash, 100)
	require.NoError(t, err)
	assert.Len(t, res, 7)
	assert.Equal(t, featureCommits[7], res[0])
	assert.Equal(t, featureCommits[6], res[1])
	assert.Equal(t, featureCommits[5], res[2])
	assert.Equal(t, featureCommits[4], res[3])
	assert.Equal(t, featureCommits[3], res[4])
	assert.Equal(t, featureCommits[2], res[5])
	assert.Equal(t, featureCommits[1], res[6])

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, masterHash, featureHash, 100)
	require.NoError(t, err)
	assert.Len(t, res, 0)

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, featureHash, masterHash, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assert.Equal(t, featureCommits[7], res[0])
	assert.Equal(t, featureCommits[6], res[1])
	assert.Equal(t, featureCommits[5], res[2])

	res, err = GetDotDotRevisions(context.Background(), env.DoltDB, featurePreMergeHash, masterHash, 3)
	require.NoError(t, err)
	assert.Len(t, res, 3)
	assert.Equal(t, featureCommits[3], res[0])
	assert.Equal(t, featureCommits[2], res[1])
	assert.Equal(t, featureCommits[1], res[2])
}

func mustCreateCommit(t *testing.T, ddb *doltdb.DoltDB, bn string, rvh hash.Hash, parents ...*doltdb.Commit) *doltdb.Commit {
	cm, err := doltdb.NewCommitMeta("Bill Billerson", "bill@billerson.com", "A New Commit.")
	require.NoError(t, err)
	pcs := make([]*doltdb.CommitSpec, 0, len(parents))
	for _, parent := range parents {
		cs, err := doltdb.NewCommitSpec(mustGetHash(t, parent).String(), bn)
		require.NoError(t, err)
		pcs = append(pcs, cs)
	}
	bref := ref.NewBranchRef(bn)
	commit, err := ddb.CommitWithParentSpecs(context.Background(), rvh, bref, pcs, cm)
	require.NoError(t, err)
	return commit
}

func mustGetHash(t *testing.T, c *doltdb.Commit) hash.Hash {
	h, err := c.HashOf()
	require.NoError(t, err)
	return h
}
