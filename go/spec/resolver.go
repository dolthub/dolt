// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"strings"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/types"
)

type Resolver struct {
	config *Config
}

func NewResolver() (Resolver, error) {
	c, err := FindNomsConfig()
	if err != nil {
		if err != NoConfig {
			return Resolver{}, err
		}
		return Resolver{}, nil
	}
	return Resolver{ c }, nil
}

func (dsr *Resolver) resolveDatabase(str string) string {
	if dsr.config != nil {
		if str == "" {
			return dsr.config.Default.Url
		}
		if val, ok := dsr.config.Db[str]; ok {
			return val.Url
		}
	}
	return str
}

func (dsr *Resolver) resolvePath(str string) string {
	if dsr.config != nil {
		split := strings.SplitN(str, separator, 2)
		db, rest := "", split[0]
		if len(split) > 1 {
			db, rest = split[0], split[1]
		}
		return dsr.resolveDatabase(db) + separator + rest
	}
	return str
}

// Resolve string to database spec. If a config is present,
//   - resolve a db alias to its db spec
//   - resolve "" to the default db spec
func (dsr *Resolver) GetDatabase(str string) (datas.Database, error) {
	return GetDatabase(dsr.resolveDatabase(str))
}

// Resolve string to a chunkstore. Like ResolveDatabase, but returns the underlying ChunkStore
func (dsr *Resolver) GetChunkStore(str string) (chunks.ChunkStore, error) {
	return GetChunkStore(dsr.resolveDatabase(str))
}

// Resolve string to a dataset. If a config is present,
//  - if no db prefix is present, assume the default db
//  - if the db prefix is an alias, replace it
func (dsr *Resolver) GetDataset(str string) (dataset.Dataset, error) {
	return GetDataset(dsr.resolvePath(str))
}

// Resolve string to a value path. If a config is present,
//  - if no db spec is present, assume the default db
//  - if the db spec is an alias, replace it
func (dsr *Resolver) GetPath(str string) (datas.Database, types.Value, error) {
	return GetPath(dsr.resolvePath(str))
}
