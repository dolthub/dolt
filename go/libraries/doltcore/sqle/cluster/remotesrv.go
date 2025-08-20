// Copyright 2022 Dolthub, Inc.
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

package cluster

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/store/hash"
)

type remotesrvStoreCache struct {
	remotesrv.DBCache
	controller *Controller
}

func (s remotesrvStoreCache) Get(ctx context.Context, path, nbfVerStr string) (remotesrv.RemoteSrvStore, error) {
	rss, err := s.DBCache.Get(ctx, path, nbfVerStr)
	if err != nil {
		return nil, err
	}
	return remotesrvStore{RemoteSrvStore: rss, path: path, controller: s.controller}, nil
}

type remotesrvStore struct {
	remotesrv.RemoteSrvStore
	controller *Controller
	path       string
}

func (rss remotesrvStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	res, err := rss.RemoteSrvStore.Commit(ctx, current, last)
	if err == nil && res {
		rss.controller.recordSuccessfulRemoteSrvCommit(rss.path)
	}
	return res, err
}
