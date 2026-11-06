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

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func TestLocalCSCacheReopensUnwrittenStoreInRequestedFormat(t *testing.T) {
	ctx := context.Background()
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)
	cache := NewLocalCSCache(fs)
	dolt := types.Format_DOLT.VersionString()
	const toy = "__TOY__"
	requesting := func(nbf string) context.Context {
		return remotesrv.WithRepoFormat(ctx, &remotesapi.ClientRepoFormat{NbfVersion: nbf})
	}

	cs, err := cache.Get(ctx, "org/repo", dolt)
	require.NoError(t, err)
	got, err := cache.Get(ctx, "org/repo", toy)
	require.NoError(t, err)
	assert.Same(t, cs, got, "only a request for a format reopens the store")

	got, err = cache.Get(requesting(toy), "org/repo", toy)
	require.NoError(t, err)
	assert.NotSame(t, cs, got)
	assert.Equal(t, toy, got.Version())
	cs = got

	c := chunks.NewChunk([]byte("written"))
	require.NoError(t, cs.Put(ctx, c, func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	}))
	ok, err := cs.Commit(ctx, c.Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, ok)
	got, err = cache.Get(requesting(dolt), "org/repo", dolt)
	require.NoError(t, err)
	assert.Same(t, cs, got, "a written store keeps its format")
	require.NoError(t, cs.Close())
}
