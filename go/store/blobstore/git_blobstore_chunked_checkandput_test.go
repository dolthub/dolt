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

package blobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func TestGitBlobstore_CheckAndPut_ChunkedRoundTrip_CreateOnly(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(repo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 3,
	})
	require.NoError(t, err)

	want := []byte("abcdefghij") // 10 bytes -> chunked tree
	ver, err := bs.CheckAndPut(ctx, "", "big", int64(len(want)), bytes.NewReader(want))
	require.NoError(t, err)
	require.NotEmpty(t, ver)

	got, ver2, err := GetBytes(ctx, bs, "big", AllRange)
	require.NoError(t, err)
	require.Equal(t, ver, ver2)
	require.Equal(t, want, got)
}

type chunkedFailReader struct{}

func (chunkedFailReader) Read(_ []byte) (int, error) {
	return 0, errors.New("read should not be called")
}

func TestGitBlobstore_CheckAndPut_MismatchDoesNotConsumeReader_WithChunkingEnabled(t *testing.T) {
	requireGitOnPath(t)

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	// Seed any commit so actualVersion != "".
	bs0, err := NewGitBlobstoreWithOptions(repo.GitDir, DoltDataRef, GitBlobstoreOptions{Identity: testIdentity()})
	require.NoError(t, err)
	_, err = bs0.Put(ctx, "x", 1, bytes.NewReader([]byte("x")))
	require.NoError(t, err)

	bs, err := NewGitBlobstoreWithOptions(repo.GitDir, DoltDataRef, GitBlobstoreOptions{
		Identity:    testIdentity(),
		MaxPartSize: 3,
	})
	require.NoError(t, err)

	_, err = bs.CheckAndPut(ctx, "definitely-wrong", "y", 1, io.Reader(chunkedFailReader{}))
	require.Error(t, err)
	require.True(t, IsCheckAndPutError(err))
}
