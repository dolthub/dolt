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
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
	"github.com/dolthub/dolt/go/store/types"
)

func hookVar[T any](t *testing.T, v *T, f T) {
	orig := *v
	*v = f
	t.Cleanup(func() { *v = orig })
}

type insecureDialer struct{}

func (insecureDialer) GetGRPCDialParams(c grpcendpoint.Config) (GRPCRemoteConfig, error) {
	return GRPCRemoteConfig{Endpoint: c.Endpoint, DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}}, nil
}

func TestFactoryHooks(t *testing.T) {
	ctx := context.Background()
	var dirs []string
	var oldGens int
	hookVar(t, &storeOptions, func(_ *types.NomsBinFormat, dir string, params map[string]interface{}) []nbs.StoreOption {
		dirs = append(dirs, dir)
		return nil
	})
	hookVar(t, &oldGenStoreOptions, func(*nbs.NomsBlockStore) []nbs.StoreOption {
		oldGens++
		return nil
	})

	_, _, _, err := CreateDB(ctx, types.Format_DOLT, "mem://", nil)
	require.NoError(t, err)
	require.Contains(t, dirs, "")

	nomsDir := filepath.Join(t.TempDir(), "noms")
	require.NoError(t, os.MkdirAll(nomsDir, 0o755))
	params := map[string]interface{}{ChunkJournalParam: struct{}{}, DisableSingletonCacheParam: struct{}{}}
	db, _, _, err := CreateDB(ctx, types.Format_DOLT, earl.FileUrlFromPath(filepath.ToSlash(nomsDir), os.PathSeparator), params)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.Contains(t, dirs, nomsDir)
	require.NotZero(t, oldGens)

	errRemote := errors.New("remote")
	hookVar(t, &newRemoteChunkStore, func(context.Context, *types.NomsBinFormat, string, string, bool, remotesapi.ChunkStoreServiceClient, map[string]interface{}) (*remotestorage.DoltChunkStore, error) {
		return nil, errRemote
	})
	_, _, _, err = CreateDB(ctx, types.Format_DOLT, "http://localhost:1/org/repo", map[string]interface{}{GRPCDialProviderParam: insecureDialer{}})
	require.ErrorIs(t, err, errRemote)

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}
	var evicted []string
	hookVar(t, &evictCachedGitRemote, func(_ context.Context, cacheRepo string, _ map[string]interface{}) error {
		evicted = append(evicted, cacheRepo)
		return nil
	})
	remote, err := gitrepo.InitBare(ctx, filepath.Join(shortTempDir(t), "remote.git"))
	require.NoError(t, err)
	_, err = remote.SetRefToTree(ctx, "refs/heads/main", map[string][]byte{"README": []byte("seed\n")}, "seed")
	require.NoError(t, err)
	params = map[string]interface{}{GitCacheRootParam: filepath.Join(shortTempDir(t), DoltDir, GitRemoteCacheDirName)}
	db, _, _, err = CreateDB(ctx, types.Format_DOLT, "git+file://"+filepath.ToSlash(remote.GitDir), params)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NotEmpty(t, evicted)
}
