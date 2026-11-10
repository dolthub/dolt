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

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// storeOptions returns the options of a store that a factory opens with |params|. |dir| is a local store's
// directory, or "".
var storeOptions = func(nbf *types.NomsBinFormat, dir string, params map[string]interface{}) []nbs.StoreOption {
	return nil
}

// oldGenStoreOptions returns the options of the old generation of local store |newGen|.
var oldGenStoreOptions = func(newGen *nbs.NomsBlockStore) []nbs.StoreOption {
	return nil
}

// evictCachedGitRemote is called before a git remote's cached store in |cacheRepo| is reused for |params|, and
// may remove it from the cache.
var evictCachedGitRemote = func(ctx context.Context, cacheRepo string, params map[string]interface{}) error {
	return nil
}

// newRemoteChunkStore opens the store of a remote served over gRPC with |params|.
var newRemoteChunkStore = func(ctx context.Context, nbf *types.NomsBinFormat, path, host string, wsValidate bool, client remotesapi.ChunkStoreServiceClient, params map[string]interface{}) (*remotestorage.DoltChunkStore, error) {
	return remotestorage.NewDoltChunkStoreFromPath(ctx, nbf, path, host, wsValidate, client)
}
