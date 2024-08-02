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

package commands

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/datas"
)

func TestMapCommitsWithChildrenAndPosition(t *testing.T) {
	commits := []CommitInfo{
		{commitHash: "hash1", parentHashes: []string{"hash2"}, commitMeta: &datas.CommitMeta{Description: "Commit 1"}},
		{commitHash: "hash2", parentHashes: []string{}, commitMeta: &datas.CommitMeta{Description: "Commit 2"}},
	}

	result := mapCommitsWithChildrenAndPosition(commits)

	require.Equal(t, 2, len(result))
	require.Equal(t, "hash1", result[0].Commit.commitHash)
	require.Equal(t, "hash2", result[1].Commit.commitHash)
	require.Equal(t, []string{"hash1"}, result[1].Children)
	require.Nil(t, result[0].Children)
}

func TestComputeColumnEnds(t *testing.T) {
	// Test with two commits, one parent and one child
	commits := []*commitInfoWithChildren{
		{Commit: CommitInfo{commitHash: "hash1", parentHashes: []string{"hash2"}}, Children: []string{}, Row: 0},
		{Commit: CommitInfo{commitHash: "hash2", parentHashes: []string{}}, Children: []string{"hash1"}, Row: 1},
	}
	commitsMap := map[string]*commitInfoWithChildren{
		"hash1": commits[0],
		"hash2": commits[1],
	}

	result, _ := computeColumnEnds(commits, commitsMap)
	require.Equal(t, 0, result[0].Col)
	require.Equal(t, 0, result[1].Col)

	// Test four commits:
	//
	//
	//          1A (branchA)
	//         /  \
	//   1M - 2M - 3M (main)
	commits = []*commitInfoWithChildren{
		{Commit: CommitInfo{commitHash: "3M", parentHashes: []string{"2M", "1A"}}, Children: []string{}, Row: 0},
		{Commit: CommitInfo{commitHash: "1A", parentHashes: []string{"2M"}}, Children: []string{"3M"}, Row: 1},
		{Commit: CommitInfo{commitHash: "2M", parentHashes: []string{"1M"}}, Children: []string{"1A", "3M"}, Row: 2},
		{Commit: CommitInfo{commitHash: "1M", parentHashes: []string{}}, Children: []string{"2M"}, Row: 3},
	}
	commitsMap = map[string]*commitInfoWithChildren{
		"1M": commits[3],
		"2M": commits[2],
		"1A": commits[1],
		"3M": commits[0],
	}
	result, _ = computeColumnEnds(commits, commitsMap)
	require.Equal(t, 0, result[0].Col)
	require.Equal(t, 1, result[1].Col)
	require.Equal(t, 0, result[2].Col)
	require.Equal(t, 0, result[3].Col)

}

func TestDrawCommitDotsAndBranchPaths(t *testing.T) {
	// Test with two commits, one parent and one child
	commits := []*commitInfoWithChildren{
		{Commit: CommitInfo{commitHash: "hash1", parentHashes: []string{"hash2"}, commitMeta: &datas.CommitMeta{Description: "Commit 1"}}, Children: []string{}, Row: 0},
		{Commit: CommitInfo{commitHash: "hash2", parentHashes: []string{}, commitMeta: &datas.CommitMeta{Description: "Commit 2"}}, Children: []string{"hash1"}, Row: 1},
	}
	commitsMap := map[string]*commitInfoWithChildren{
		"hash1": commits[0],
		"hash2": commits[1],
	}

	commits, commitsMap = computeColumnEnds(commits, commitsMap)
	expandGraph(commits)

	graph := drawCommitDotsAndBranchPaths(commits, commitsMap)

	require.Equal(t, "\x1b[37m*", graph[0][0])
	require.Equal(t, "\x1b[31m|", graph[1][0])
	require.Equal(t, "\x1b[31m|", graph[2][0])
	require.Equal(t, "\x1b[31m|", graph[3][0])
	require.Equal(t, "\x1b[31m|", graph[4][0])
	require.Equal(t, "\x1b[31m|", graph[5][0])
	require.Equal(t, "\x1b[37m*", graph[6][0])

}

func TestExpandGraph(t *testing.T) {
	commits := []*commitInfoWithChildren{
		{
			Commit: CommitInfo{
				commitMeta: &datas.CommitMeta{
					Description: "This is a longer commit message\nthat spans multiple lines\nfor testing purposes",
				},
			},
			Col: 0,
			Row: 0,
		},
		{
			Commit: CommitInfo{
				commitMeta: &datas.CommitMeta{
					Description: "Short commit message",
				},
			},
			Col: 1,
			Row: 1,
		},
	}
	expandGraph(commits)
	require.Equal(t, 0, commits[0].Col)
	require.Equal(t, 0, commits[0].Row)
	require.Equal(t, 3, len(commits[0].formattedMessage))
	require.Equal(t, 2, commits[1].Col)
	require.Equal(t, 8, commits[1].Row)
	require.Equal(t, 1, len(commits[1].formattedMessage))
}
