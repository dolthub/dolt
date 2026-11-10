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

package env

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

// ToyStorage records what environments ask of their storage config. Tests in this directory install it with
// UseToyStorage.
type ToyStorage struct {
	// Loaded holds the DBLoadParams of each environment as it loaded its database.
	Loaded  []map[string]interface{}
	Copied  []*doltdb.DoltDB
	LoadErr error
	CopyErr error
}

type toyDialer struct {
	dbfactory.GRPCDialProvider
}

func (s *ToyStorage) loadParams(dEnv *DoltEnv) (map[string]interface{}, error) {
	s.Loaded = append(s.Loaded, maps.Clone(dEnv.DBLoadParams))
	return map[string]interface{}{"toy_load": "1"}, s.LoadErr
}

func (s *ToyStorage) remoteParams(params map[string]interface{}, dialer dbfactory.GRPCDialProvider) error {
	params["toy_remote"] = dialer
	return nil
}

func (s *ToyStorage) remoteDialer(_ *doltdb.DoltDB, dialer dbfactory.GRPCDialProvider) dbfactory.GRPCDialProvider {
	return toyDialer{dialer}
}

func (s *ToyStorage) copyParams(_ context.Context, src *doltdb.DoltDB) (map[string]string, error) {
	s.Copied = append(s.Copied, src)
	return map[string]string{"toy_copy": "1"}, s.CopyErr
}

func (s *ToyStorage) restoreParams(context.Context, *doltdb.DoltDB) (map[string]string, error) {
	return map[string]string{"toy_restore": "1"}, nil
}

func (s *ToyStorage) createParamsFor(_ context.Context, name string) (map[string]string, error) {
	return map[string]string{"toy_create": name}, nil
}

// UseToyStorage makes environments use a new ToyStorage until the test ends.
func UseToyStorage(t *testing.T) *ToyStorage {
	prev := storage
	t.Cleanup(func() { storage = prev })
	s := &ToyStorage{}
	storage = s
	return s
}

func TestStorageLoadParams(t *testing.T) {
	ctx := context.Background()
	s := UseToyStorage(t)
	dEnv, _ := createTestEnv(false, false)
	dEnv.AddDBLoadParams(map[string]string{"added": "1"})
	params, err := dEnv.LoadParams()
	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"toy_load": "1", "added": "1"}, params)
	require.NoError(t, dEnv.InitRepo(ctx, types.Format_DOLT, "name", "email@example.com", DefaultInitBranch))
	require.Len(t, s.Loaded, 2, "the database loads with the storage params")

	s.LoadErr = errors.New("toy")
	dEnv, _ = createTestEnv(false, false)
	LoadDoltDB(ctx, dEnv)
	assert.ErrorIs(t, dEnv.DBLoadError, s.LoadErr)
}

type testDialer struct{}

func (testDialer) GetGRPCDialParams(grpcendpoint.Config) (dbfactory.GRPCRemoteConfig, error) {
	return dbfactory.GRPCRemoteConfig{}, nil
}

func TestStorageRemotes(t *testing.T) {
	ctx := context.Background()
	s := UseToyStorage(t)
	r := NewRemote("origin", "file:///tmp/remote", map[string]string{"own": "x"})
	params, err := r.DBFactoryParams(testDialer{})
	require.NoError(t, err)
	assert.Equal(t, testDialer{}, params["toy_remote"])

	src, err := doltdb.LoadDoltDB(ctx, types.Format_DOLT, doltdb.InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	defer src.Close()
	push, err := r.ForPushFrom(ctx, src)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"own": "x", "toy_copy": "1"}, push.Params)
	assert.Equal(t, map[string]string{"own": "x"}, r.Params, "the remote itself is unchanged")
	assert.Equal(t, []*doltdb.DoltDB{src}, s.Copied)
	assert.Equal(t, toyDialer{testDialer{}}, RemoteDialerFor(src, testDialer{}))

	restore, err := RestoreParams(ctx, src)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"toy_restore": "1"}, restore)
	create, err := CreateParamsForClient(ctx, "db")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"toy_create": "db"}, create)
}
