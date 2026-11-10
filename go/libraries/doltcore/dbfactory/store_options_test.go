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

package dbfactory

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// toyStores records what the factories ask of their store options.
type toyStores struct {
	dirs      []string
	oldGens   []string
	remoteErr error
	reuse     bool
}

func (s *toyStores) blockStore(_ *types.NomsBinFormat, dir string, _ map[string]interface{}) []nbs.StoreOption {
	s.dirs = append(s.dirs, dir)
	return nil
}

func (s *toyStores) oldGen(newGen *nbs.NomsBlockStore) []nbs.StoreOption {
	s.oldGens = append(s.oldGens, newGen.Version())
	return nil
}

func (s *toyStores) remote(map[string]interface{}) (remotestorage.RepoFormat, error) {
	return remotestorage.PlainFormat{}, s.remoteErr
}

func (s *toyStores) reusable(context.Context, chunks.ChunkStore, map[string]interface{}) bool {
	return s.reuse
}

func withStoreOptions(t *testing.T, s storeConfig) {
	prev := storeOptions
	t.Cleanup(func() { storeOptions = prev })
	storeOptions = s
}

func TestStoreOptionsReachTheFactories(t *testing.T) {
	ctx := context.Background()
	s := &toyStores{}
	withStoreOptions(t, s)

	journal, noJournal := t.TempDir(), t.TempDir()
	for _, tc := range []struct {
		url    string
		params map[string]interface{}
	}{
		{"file://" + filepath.ToSlash(journal), map[string]interface{}{DisableSingletonCacheParam: struct{}{}, ChunkJournalParam: struct{}{}}},
		{"file://" + filepath.ToSlash(noJournal), map[string]interface{}{DisableSingletonCacheParam: struct{}{}}},
		{"localbs://" + filepath.ToSlash(t.TempDir()), nil},
	} {
		db, _, _, err := CreateDB(ctx, types.Format_DOLT, tc.url, tc.params)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}
	assert.Equal(t, []string{journal, noJournal, ""}, s.dirs)
	assert.Len(t, s.oldGens, 2)
}

type toyDialer struct{}

func (toyDialer) GetGRPCDialParams(grpcendpoint.Config) (GRPCRemoteConfig, error) {
	return GRPCRemoteConfig{}, errors.New("dialed")
}

func TestStoreOptionsRemoteFormat(t *testing.T) {
	errToy := errors.New("toy")
	withStoreOptions(t, &toyStores{remoteErr: errToy})
	params := map[string]interface{}{GRPCDialProviderParam: toyDialer{}}
	_, _, _, err := CreateDB(context.Background(), types.Format_DOLT, "http://localhost:50051/org/repo", params)
	assert.ErrorIs(t, err, errToy, "the format is settled before dialing")
}

func TestStoreOptionsReusable(t *testing.T) {
	ctx := context.Background()
	s := &toyStores{reuse: true}
	withStoreOptions(t, s)

	key := filepath.Join(t.TempDir(), "repo.git")
	cs := (&chunks.MemoryStorage{}).NewView()
	gitRemoteCacheMu.Lock()
	gitRemoteCache[key] = gitRemoteCacheEntry{db: datas.NewTypesDatabase(types.NewValueStore(cs), tree.NewNodeStore(cs))}
	gitRemoteCacheMu.Unlock()
	t.Cleanup(func() {
		gitRemoteCacheMu.Lock()
		delete(gitRemoteCache, key)
		gitRemoteCacheMu.Unlock()
	})

	_, ok, err := cachedGitRemote(ctx, key, nil)
	require.NoError(t, err)
	assert.True(t, ok)

	s.reuse = false
	_, ok, err = cachedGitRemote(ctx, key, nil)
	require.NoError(t, err)
	assert.False(t, ok, "an unreusable store is evicted")
	_, ok = gitRemoteCache[key]
	assert.False(t, ok)
}

// mismatchedStore is a cached store whose manifest has since been created in another format.
type mismatchedStore struct {
	chunks.ChunkStore
}

func (mismatchedStore) Rebase(context.Context) error {
	return nbs.ErrEncryptionMismatch
}

func TestStoreOptionsReusableFileStore(t *testing.T) {
	ctx := context.Background()
	s := &toyStores{reuse: true}
	withStoreOptions(t, s)
	t.Cleanup(func() { _ = CloseAllLocalDatabases() })

	urlStr := "file://" + filepath.ToSlash(t.TempDir())
	params := map[string]interface{}{ChunkJournalParam: struct{}{}}
	db1, _, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, params)
	require.NoError(t, err)
	db2, _, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, params)
	require.NoError(t, err)
	assert.True(t, db1 == db2)

	s.reuse = false
	db3, _, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, params)
	require.NoError(t, err)
	assert.True(t, db3 != db1, "an unreusable cached store is replaced")

	path, err := earl.Parse(urlStr)
	require.NoError(t, err)
	cs := mismatchedStore{(&chunks.MemoryStorage{}).NewView()}
	singletonLock.Lock()
	stale := datas.NewTypesDatabase(types.NewValueStore(cs), tree.NewNodeStore(cs))
	singletons[path.Path] = singletonDB{ddb: stale}
	singletonLock.Unlock()
	require.NoError(t, db3.Close())
	db4, _, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, params)
	require.NoError(t, err)
	assert.True(t, db4 != stale, "a store whose manifest changed format is reopened")
}
