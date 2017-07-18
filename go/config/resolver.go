// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"fmt"
	"strings"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/verbose"
)

type Resolver struct {
	config      *Config
	dotDatapath string // set to the first datapath that was resolved
}

// A Resolver enables using db defaults, db aliases and dataset '.' replacement in command
// line arguments when a .nomsconfig file is present. To use it, create a config resolver
// before command line processing and use it to resolve each dataspec argument in
// succession.
func NewResolver() *Resolver {
	c, err := FindNomsConfig()
	if err != nil {
		if err != NoConfig {
			panic(fmt.Errorf("Failed to read .nomsconfig due to: %v", err))
		}
		return &Resolver{}
	}
	return &Resolver{c, ""}
}

// Print replacement if one occurred
func (r *Resolver) verbose(orig string, replacement string) string {
	if orig != replacement {
		if orig == "" {
			orig = `""`
		}
		verbose.Log("\tresolving %s -> %s\n", orig, replacement)
	}
	return replacement
}

// Resolve string to database name. If config is defined:
//   - replace the empty string with the default db url
//   - replace any db alias with it's url
func (r *Resolver) ResolveDbSpec(str string) string {
	if r.config != nil {
		if str == "" {
			return r.config.Db[DefaultDbAlias].Url
		}
		if val, ok := r.config.Db[str]; ok {
			return val.Url
		}
	}
	return str
}

// Resolve string to dataset or path name.
//   - replace database name as described in ResolveDatabase
//   - if this is the first call to ResolvePath, remember the
//     datapath part for subsequent calls.
//   - if this is not the first call and a "." is used, replace
//     it with the first datapath.
func (r *Resolver) ResolvePathSpec(str string) string {
	if r.config != nil {
		split := strings.SplitN(str, spec.Separator, 2)
		db, rest := "", split[0]
		if len(split) > 1 {
			db, rest = split[0], split[1]
		}
		if r.dotDatapath == "" {
			r.dotDatapath = rest
		} else if rest == "." {
			rest = r.dotDatapath
		}
		return r.ResolveDbSpec(db) + spec.Separator + rest
	}
	return str
}

// Resolve string to database spec. If a config is present,
//   - resolve a db alias to its db spec
//   - resolve "" to the default db spec
func (r *Resolver) GetDatabase(str string) (datas.Database, error) {
	sp, err := spec.ForDatabase(r.verbose(str, r.ResolveDbSpec(str)))
	if err != nil {
		return nil, err
	}
	return sp.GetDatabase(), nil
}

// Resolve string to a chunkstore. Like ResolveDatabase, but returns the underlying ChunkStore
func (r *Resolver) GetChunkStore(str string) (chunks.ChunkStore, error) {
	sp, err := spec.ForDatabase(r.verbose(str, r.ResolveDbSpec(str)))
	if err != nil {
		return nil, err
	}
	return sp.NewChunkStore(), nil
}

// Resolve string to a dataset. If a config is present,
//  - if no db prefix is present, assume the default db
//  - if the db prefix is an alias, replace it
func (r *Resolver) GetDataset(str string) (datas.Database, datas.Dataset, error) {
	sp, err := spec.ForDataset(r.verbose(str, r.ResolvePathSpec(str)))
	if err != nil {
		return nil, datas.Dataset{}, err
	}
	return sp.GetDatabase(), sp.GetDataset(), nil
}

// Resolve string to a value path. If a config is present,
//  - if no db spec is present, assume the default db
//  - if the db spec is an alias, replace it
func (r *Resolver) GetPath(str string) (datas.Database, types.Value, error) {
	sp, err := spec.ForPath(r.verbose(str, r.ResolvePathSpec(str)))
	if err != nil {
		return nil, nil, err
	}
	return sp.GetDatabase(), sp.GetValue(), nil
}
