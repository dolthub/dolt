package main

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/nbs"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

const (
	defaultMemTableSize = 128 * 1024 * 1024
)

type DBCache struct {
	mu  *sync.Mutex
	dbs map[string]*nbs.NomsBlockStore

	fs filesys.Filesys
}

func NewLocalCSCache(filesys filesys.Filesys) *DBCache {
	return &DBCache{
		&sync.Mutex{},
		make(map[string]*nbs.NomsBlockStore),
		filesys,
	}
}

func (cache *DBCache) Get(org, repo string) (*nbs.NomsBlockStore, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	id := filepath.Join(org, repo)

	if cs, ok := cache.dbs[id]; ok {
		return cs, nil
	}

	var newCS *nbs.NomsBlockStore
	if cache.fs != nil {
		err := cache.fs.MkDirs(id)

		if err != nil {
			return nil, err
		}

		newCS, err = nbs.NewLocalStore(context.TODO(), types.Format_Default.VersionString(), id, defaultMemTableSize)

		if err != nil {
			return nil, err
		}
	}

	cache.dbs[id] = newCS

	return newCS, nil
}
