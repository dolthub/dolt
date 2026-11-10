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
	dirs    []string
	oldGens int
	reuse   bool
}

func (s *toyStores) blockStore(_ *types.NomsBinFormat, dir string, _ map[string]interface{}) []nbs.StoreOption {
	s.dirs = append(s.dirs, dir)
	return nil
}

func (s *toyStores) oldGen(*nbs.NomsBlockStore) []nbs.StoreOption {
	s.oldGens++
	return nil
}

var errToyRemote = errors.New("toy remote")

func (s *toyStores) remote(map[string]interface{}) (remotestorage.RepoFormat, error) {
	return nil, errToyRemote
}

func (s *toyStores) reusable(context.Context, chunks.ChunkStore, map[string]interface{}) bool {
	return s.reuse
}

type toyDialer struct{}

func (toyDialer) GetGRPCDialParams(grpcendpoint.Config) (GRPCRemoteConfig, error) {
	return GRPCRemoteConfig{}, errors.New("dialed")
}

// mismatchedStore is a cached store whose manifest has since been created in another format.
type mismatchedStore struct {
	chunks.ChunkStore
}

func (mismatchedStore) Rebase(context.Context) error {
	return nbs.ErrEncryptionMismatch
}

func TestStoreOptions(t *testing.T) {
	ctx := context.Background()
	s := &toyStores{reuse: true}
	prev := storeOptions
	t.Cleanup(func() {
		storeOptions = prev
		_ = CloseAllLocalDatabases()
	})
	storeOptions = s

	t.Run("Factories", func(t *testing.T) {
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
		assert.Equal(t, 2, s.oldGens)

		_, _, _, err := CreateDB(ctx, types.Format_DOLT, "http://localhost:50051/org/repo", map[string]interface{}{GRPCDialProviderParam: toyDialer{}})
		assert.ErrorIs(t, err, errToyRemote, "the format is settled before dialing")
	})

	t.Run("GitRemoteCache", func(t *testing.T) {
		key := filepath.Join(t.TempDir(), "repo.git")
		cs := (&chunks.MemoryStorage{}).NewView()
		gitRemoteCacheMu.Lock()
		gitRemoteCache[key] = gitRemoteCacheEntry{db: datas.NewTypesDatabase(types.NewValueStore(cs), tree.NewNodeStore(cs))}
		gitRemoteCacheMu.Unlock()
		_, ok, err := cachedGitRemote(ctx, key, nil)
		require.NoError(t, err)
		assert.True(t, ok)

		s.reuse = false
		defer func() { s.reuse = true }()
		_, ok, err = cachedGitRemote(ctx, key, nil)
		require.NoError(t, err)
		assert.False(t, ok, "an unreusable store is evicted")
		_, ok = gitRemoteCache[key]
		assert.False(t, ok)
	})

	t.Run("FileSingleton", func(t *testing.T) {
		urlStr := "file://" + filepath.ToSlash(t.TempDir())
		params := map[string]interface{}{ChunkJournalParam: struct{}{}}
		db1, _, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, params)
		require.NoError(t, err)
		s.reuse = false
		db2, _, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, params)
		s.reuse = true
		require.NoError(t, err)
		assert.True(t, db2 != db1, "an unreusable cached store is replaced")

		path, err := earl.Parse(urlStr)
		require.NoError(t, err)
		cs := mismatchedStore{(&chunks.MemoryStorage{}).NewView()}
		stale := datas.NewTypesDatabase(types.NewValueStore(cs), tree.NewNodeStore(cs))
		singletonLock.Lock()
		singletons[path.Path] = singletonDB{ddb: stale}
		singletonLock.Unlock()
		require.NoError(t, db2.Close())
		db3, _, _, err := CreateDB(ctx, types.Format_DOLT, urlStr, params)
		require.NoError(t, err)
		assert.True(t, db3 != stale, "a store whose manifest changed format is reopened")
	})
}
