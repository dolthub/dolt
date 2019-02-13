package main

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"path/filepath"
	"sync"
)

const (
	defaultMemTableSize = 128 * 1024 * 1024
)

type DBCache struct {
	mu  *sync.Mutex
	dbs map[string]chunks.ChunkStore
	fs  filesys.Filesys
}

func NewCSCache(filesys filesys.Filesys) *DBCache {
	return &DBCache{
		&sync.Mutex{},
		make(map[string]chunks.ChunkStore),
		filesys,
	}
}

func (cache *DBCache) Get(org, repo string) (chunks.ChunkStore, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	id := filepath.Join(org, repo)

	if cs, ok := cache.dbs[id]; ok {
		return cs, nil
	}

	err := cache.fs.MkDirs(id)

	if err != nil {
		return nil, err
	}

	newCS := nbs.NewLocalStore(id, defaultMemTableSize)
	cache.dbs[id] = newCS

	return newCS, nil
}
