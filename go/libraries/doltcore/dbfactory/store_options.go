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

	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// storeOptions configures the stores that the factories open. Dolt Enterprise replaces it.
var storeOptions storeConfig = plainStores{}

type storeConfig interface {
	// blockStore returns the options of a store opened with |params|; |dir| is a local store's directory, or "".
	blockStore(nbf *types.NomsBinFormat, dir string, params map[string]interface{}) []nbs.StoreOption
	// oldGen returns the options of the old generation of local store |newGen|.
	oldGen(newGen *nbs.NomsBlockStore) []nbs.StoreOption
	// remote returns the format of a remote repository opened with |params|.
	remote(params map[string]interface{}) (remotestorage.RepoFormat, error)
	// reusable reports whether cached store |cs| can serve a database opened with |params|.
	reusable(ctx context.Context, cs chunks.ChunkStore, params map[string]interface{}) bool
}

type plainStores struct{}

func (plainStores) blockStore(*types.NomsBinFormat, string, map[string]interface{}) []nbs.StoreOption {
	return nil
}

func (plainStores) oldGen(*nbs.NomsBlockStore) []nbs.StoreOption {
	return nil
}

func (plainStores) remote(map[string]interface{}) (remotestorage.RepoFormat, error) {
	return remotestorage.PlainFormat{}, nil
}

func (plainStores) reusable(context.Context, chunks.ChunkStore, map[string]interface{}) bool {
	return true
}

// ReusableStore reports whether cached store |cs| can serve a database opened with |params|.
func ReusableStore(ctx context.Context, cs chunks.ChunkStore, params map[string]interface{}) bool {
	return storeOptions.reusable(ctx, cs, params)
}

// DialerAs returns the first of |d| and the dialers it wraps, through their Unwrap methods, that is a T.
func DialerAs[T any](d GRPCDialProvider) (T, bool) {
	for d != nil {
		if t, ok := d.(T); ok {
			return t, true
		}
		u, ok := d.(interface{ Unwrap() GRPCDialProvider })
		if !ok {
			break
		}
		d = u.Unwrap()
	}
	var zero T
	return zero, false
}
