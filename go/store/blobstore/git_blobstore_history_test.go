// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package blobstore

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
)

func TestGitBlobstore_HistoryLimit(t *testing.T) {
	limit, unlimited := 2, 0
	for _, tc := range []struct {
		name        string
		limit       *int
		depth, want int
	}{
		{"default", nil, 64, 1},
		{"configured", &limit, 2, 1},
		{"unlimited", &unlimited, 65, 66},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireGitOnPath(t)
			ctx := context.Background()
			remote, local, _ := newRemoteAndLocalRepos(t, ctx)
			seed, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("seed")}, "seed")
			require.NoError(t, err)
			runner, err := git.NewRunner(remote.GitDir)
			require.NoError(t, err)
			api := git.NewGitAPIImpl(runner)
			tree, err := runner.Run(ctx, git.RunOptions{}, "rev-parse", seed+"^{tree}")
			require.NoError(t, err)
			head := git.OID(seed)
			for i := 1; i < tc.depth; i++ {
				head, err = api.CommitTree(ctx, git.OID(string(bytes.TrimSpace(tree))), &head, "parented", testIdentity())
				require.NoError(t, err)
			}
			require.NoError(t, api.UpdateRef(ctx, DoltDataRef, head, "seed chain"))
			bs, err := NewGitBlobstoreWithOptions(local.GitDir, DoltDataRef, GitBlobstoreOptions{Identity: testIdentity(), MaxHistoryCommits: tc.limit})
			require.NoError(t, err)
			_, version, err := GetBytes(ctx, bs, "manifest", AllRange)
			require.NoError(t, err)
			_, err = bs.CheckAndPutManifest(ctx, version, []byte("changed"))
			require.NoError(t, err)
			head, err = api.ResolveRefCommit(ctx, DoltDataRef)
			require.NoError(t, err)
			depth, err := api.RevListCount(ctx, head, 0)
			require.NoError(t, err)
			require.Equal(t, tc.want, depth)
		})
	}
}

func TestGitBlobstore_PruneKeepsParentWhenConfigured(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	remote, local, _ := newRemoteAndLocalRepos(t, ctx)
	_, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{
		"manifest": []byte("5:__DOLT__:lock:root:gc:tableA:10:dead:20"),
		"tableA":   []byte("live"), "dead": []byte("obsolete"),
	}, "seed")
	require.NoError(t, err)
	reset := false
	bs, err := NewGitBlobstoreWithOptions(local.GitDir, DoltDataRef, GitBlobstoreOptions{Identity: testIdentity(), ResetHistoryOnPrune: &reset})
	require.NoError(t, err)
	_, version, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	_, err = bs.CheckAndPutManifest(ctx, version, []byte("5:__DOLT__:lock2:root:gc:tableA:10"))
	require.NoError(t, err)
	runner, err := git.NewRunner(remote.GitDir)
	require.NoError(t, err)
	api := git.NewGitAPIImpl(runner)
	head, err := api.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	depth, err := api.RevListCount(ctx, head, 0)
	require.NoError(t, err)
	require.Equal(t, 2, depth)
	_, _, err = api.ResolvePathObject(ctx, head, "dead")
	require.True(t, git.IsPathNotFound(err), "obsolete entry must still be removed from the current tree")
}

type failingHistoryCountAPI struct {
	git.GitAPI
	err error
}

func (api failingHistoryCountAPI) RevListCount(context.Context, git.OID, int) (int, error) {
	return 0, api.err
}

func TestGitBlobstore_HistoryCountError(t *testing.T) {
	requireGitOnPath(t)
	ctx := context.Background()
	remote, local, _ := newRemoteAndLocalRepos(t, ctx)
	seed, err := remote.SetRefToTree(ctx, DoltDataRef, map[string][]byte{"manifest": []byte("seed")}, "seed")
	require.NoError(t, err)
	bs, err := NewGitBlobstoreWithOptions(local.GitDir, DoltDataRef, GitBlobstoreOptions{Identity: testIdentity()})
	require.NoError(t, err)
	_, version, err := GetBytes(ctx, bs, "manifest", AllRange)
	require.NoError(t, err)
	countErr := errors.New("history count failed")
	api := bs.api
	bs.api = failingHistoryCountAPI{GitAPI: api, err: countErr}
	_, err = bs.CheckAndPutManifest(ctx, version, []byte("changed"))
	require.ErrorIs(t, err, countErr)

	runner, err := git.NewRunner(remote.GitDir)
	require.NoError(t, err)
	remoteAPI := git.NewGitAPIImpl(runner)
	head, err := remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	require.Equal(t, git.OID(seed), head, "failed writes must not replace history")

	bs.api = api
	_, err = bs.CheckAndPutManifest(ctx, version, []byte("changed"))
	require.NoError(t, err)
	head, err = remoteAPI.ResolveRefCommit(ctx, DoltDataRef)
	require.NoError(t, err)
	depth, err := remoteAPI.RevListCount(ctx, head, 0)
	require.NoError(t, err)
	require.Equal(t, 2, depth, "retry must retain the original parent")
}
