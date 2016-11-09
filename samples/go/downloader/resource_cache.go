// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"sync"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
    "github.com/attic-labs/noms/go/marshal"
)

// ResourceCache is a Map<String, Ref<Blob>>
type resourceCache struct {
	cache   types.Map // gets updated when set is called
	orig    types.Map // original state of the map, commit is a noop if orig==cache
	rwMutex sync.RWMutex
}

func checkCacheType(c types.Value) (err error) {
    err = errors.New("resourceCache value is not Map<String, Ref<Blob>>")
    var m types.Map
	
    if err1 := marshal.Unmarshal(c, &m); err1 != nil {
		return
    }
    keyType := c.Type().Desc.(types.CompoundDesc).ElemTypes[0]
    if keyType.Kind() != types.StringKind {
        return
    }
    valueType := c.Type().Desc.(types.CompoundDesc).ElemTypes[1]
    if valueType.Kind() != types.RefKind {
        return
    }
    if valueType.Desc.(types.CompoundDesc).ElemTypes[0].Kind() != types.BlobKind {
        return
	}
    
	err = nil
	return
}

func getResourceCache(db datas.Database, dsname string) (*resourceCache, error) {
	m, ok := db.GetDataset(dsname).MaybeHeadValue()
	if ok {
		if err := checkCacheType(m); err != nil {
			return nil, err
		}
	} else {
		m = types.NewMap()
	}
	return &resourceCache{cache: m.(types.Map), orig: m.(types.Map)}, nil
}

func (c *resourceCache) commit(db datas.Database, dsname string) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	if !c.cache.Equals(c.orig) {
		meta, _ := spec.CreateCommitMetaStruct(db, "", "", nil, nil)
		dset := db.GetDataset(dsname)
		commitOptions := datas.CommitOptions{Meta: meta}
		_, err := db.Commit(dset, c.cache, commitOptions)
		if err == nil {
			c.orig = c.cache
		}
		return err
	}
	return nil
}

func (c *resourceCache) get(k types.String) (types.Ref, bool) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	if v, ok := c.cache.MaybeGet(k); ok {
		return v.(types.Ref), true
	}
	return types.Ref{}, false
}

func (c *resourceCache) set(k types.String, v types.Ref) {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	c.cache = c.cache.Set(k, v)
}

func (c *resourceCache) len() uint64 {
	return c.cache.Len()
}
