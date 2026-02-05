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

package nbs

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
	"github.com/dolthub/dolt/go/store/types"
)

func TestGitBlobstoreWriteSmoke_RoundTripRootValue(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	repo, err := gitrepo.InitBare(ctx, t.TempDir()+"/repo.git")
	require.NoError(t, err)

	bs, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)

	qp := NewUnlimitedMemQuotaProvider()
	const memTableSize = 1 << 8
	cs, err := NewBSStore(ctx, constants.FormatDefaultString, bs, memTableSize, qp)
	require.NoError(t, err)

	vs := types.NewValueStore(cs)

	// Write a small value, commit it as the new root.
	ref, err := vs.WriteValue(ctx, types.String("hello gitblobstore"))
	require.NoError(t, err)

	last, err := vs.Root(ctx)
	require.NoError(t, err)

	ok, err := vs.Commit(ctx, ref.TargetHash(), last)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, vs.Close())

	// Reopen and verify the committed root and value are readable.
	bs2, err := blobstore.NewGitBlobstore(repo.GitDir, blobstore.DoltDataRef)
	require.NoError(t, err)
	cs2, err := NewBSStore(ctx, constants.FormatDefaultString, bs2, memTableSize, qp)
	require.NoError(t, err)
	vs2 := types.NewValueStore(cs2)
	defer func() { _ = vs2.Close() }()

	gotRoot, err := vs2.Root(ctx)
	require.NoError(t, err)
	require.Equal(t, ref.TargetHash(), gotRoot)

	gotVal, err := vs2.ReadValue(ctx, gotRoot)
	require.NoError(t, err)
	require.Equal(t, types.String("hello gitblobstore"), gotVal)
}
