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
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	if err := remotesrv.ValidateRepoPath(repopath); err != nil {
		return nil, err
	}

	id := filepath.FromSlash(repopath)

	if cs, ok := cache.dbs[id]; ok && !cache.evictUnwrittenInOtherFormat(ctx, id, cs, nbfVerStr) {
		return cs, nil
	}

	rootPath, err := cache.fs.Abs(".")
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, err
	}
	defer root.Close()

	// Root.MkdirAll keeps symlinks and concurrent path changes from creating
	// directories outside the storage root.
	err = root.MkdirAll(id, os.ModePerm)
	if err != nil {
		return nil, err
	}
	path, err := filepath.EvalSymlinks(filepath.Join(rootPath, id))
	if err != nil {
		return nil, err
	}
	resolvedRoot, err := filepath.EvalSymlinks(rootPath)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(resolvedRoot, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("repository path %q escapes storage root", repopath)
	}

	newCS, err := nbs.NewLocalStore(ctx, nbfVerStr, path, defaultMemTableSize, nbs.NewUnlimitedMemQuotaProvider(), false)
	if err != nil {
		return nil, err
	}

	cache.dbs[id] = newCS

	return newCS, nil
}

// evictUnwrittenInOtherFormat closes and forgets the store at |id| if it is empty and a client creating it asked
// for another format.
func (cache *LocalCSCache) evictUnwrittenInOtherFormat(ctx context.Context, id string, cs remotesrv.RemoteSrvStore, nbfVerStr string) bool {
	if _, ok := remotesrv.RequestedRepoFormat(ctx); !ok || cs.Version() == nbfVerStr {
		return false
	}
	empty := true
	err := cache.fs.Iter(id, false, func(path string, _ int64, _ bool) bool {
		// Opening a store creates its manifest lock file.
		if filepath.Base(path) == "LOCK" {
			return false
		}
		empty = false
		return true
	})
	if err != nil || !empty {
		return false
	}
	delete(cache.dbs, id)
	_ = cs.Close()
	return true
}

type SingletonCSCache struct {
	s remotesrv.RemoteSrvStore
}

func (cache SingletonCSCache) Get(_ context.Context, path, nbfVerStr string) (remotesrv.RemoteSrvStore, error) {
	return cache.s, nil
}
