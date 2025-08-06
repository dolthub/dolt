// Copyright 2019 Dolthub, Inc.
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
	"path/filepath"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/nbs"
)

const (
	defaultMemTableSize = 128 * 1024 * 1024
)

type LocalCSCache struct {
	mu  *sync.Mutex
	dbs map[string]remotesrv.RemoteSrvStore

	fs filesys.Filesys
}

func NewLocalCSCache(filesys filesys.Filesys) *LocalCSCache {
	return &LocalCSCache{
		&sync.Mutex{},
		make(map[string]remotesrv.RemoteSrvStore),
		filesys,
	}
}

func (cache *LocalCSCache) Get(ctx context.Context, repopath, nbfVerStr string) (remotesrv.RemoteSrvStore, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	id := filepath.FromSlash(repopath)

	if cs, ok := cache.dbs[id]; ok {
		return cs, nil
	}

	err := cache.fs.MkDirs(id)
	if err != nil {
		return nil, err
	}
	path, err := cache.fs.Abs(id)
	if err != nil {
		return nil, err
	}

	newCS, err := nbs.NewLocalStore(ctx, nbfVerStr, path, defaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider(), false)
	if err != nil {
		return nil, err
	}

	cache.dbs[id] = newCS

	return newCS, nil
}

type SingletonCSCache struct {
	s remotesrv.RemoteSrvStore
}

func (cache SingletonCSCache) Get(_ context.Context, path, nbfVerStr string) (remotesrv.RemoteSrvStore, error) {
	return cache.s, nil
}
