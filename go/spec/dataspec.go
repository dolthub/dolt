// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package spec provides builders and parsers for spelling Noms databases, datasets and values.
package spec

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	flag "github.com/juju/gnuflag"
)

const (
	Separator = "::"
)

var (
	datasetRe = regexp.MustCompile("^" + datas.DatasetRe.String() + "$")
	ldbStores = map[string]*refCountingLdbStore{}
)

func GetDatabase(str string) (datas.Database, error) {
	sp, err := ParseDatabaseSpec(str)
	if err != nil {
		return nil, err
	}
	return sp.Database()
}

func GetChunkStore(str string) (chunks.ChunkStore, error) {
	sp, err := ParseDatabaseSpec(str)
	if err != nil {
		return nil, err
	}

	switch sp.Protocol {
	case "ldb":
		return getLDBStore(sp.Path), nil
	case "mem":
		return chunks.NewMemoryStore(), nil
	default:
		return nil, fmt.Errorf("Unable to create chunkstore for protocol: %s", str)
	}
}

func GetDataset(str string) (datas.Database, datas.Dataset, error) {
	sp, err := parseDatasetSpec(str)
	if err != nil {
		return nil, datas.Dataset{}, err
	}
	ds, err := sp.Dataset()
	if err != nil {
		return nil, datas.Dataset{}, err
	}
	return ds.Database(), ds, nil
}

func GetPath(str string) (datas.Database, types.Value, error) {
	sp, err := ParsePathSpec(str)
	if err != nil {
		return nil, nil, err
	}
	return sp.Value()
}

type DatabaseSpec struct {
	Protocol    string
	Path        string
	accessToken string
}

type datasetSpec struct {
	DbSpec      DatabaseSpec
	DatasetName string
}

type PathSpec struct {
	DbSpec DatabaseSpec
	Path   AbsolutePath
}

// ParseDatabaseSpec parses a database spec string into its parts
func ParseDatabaseSpec(spec string) (DatabaseSpec, error) {
	ldbDatabaseSpec := func(path string) (DatabaseSpec, error) {
		if len(path) == 0 {
			return DatabaseSpec{}, fmt.Errorf("Empty file system path")
		}
		return DatabaseSpec{Protocol: "ldb", Path: path}, nil
	}

	parts := strings.SplitN(spec, ":", 2) // [protocol] [, path]?
	protocol := parts[0]

	// If there was no ":" then this is either a mem spec, or a filesystem path.
	// This is ambiguous if the file system path is "mem" but that just means the path needs to be explicitly "ldb:mem".
	if len(parts) == 1 {
		if protocol == "mem" {
			return DatabaseSpec{Protocol: "mem"}, nil
		}
		return ldbDatabaseSpec(protocol)
	}

	path := parts[1]

	switch protocol {
	case "http", "https":
		u, err := url.Parse(spec)
		if err != nil || len(u.Host) == 0 {
			return DatabaseSpec{}, fmt.Errorf("Invalid URL: %s", spec)
		}
		token := u.Query().Get("access_token")
		return DatabaseSpec{Protocol: protocol, Path: path, accessToken: token}, nil

	case "ldb":
		return ldbDatabaseSpec(path)

	case "mem":
		return DatabaseSpec{}, fmt.Errorf(`In-memory database must be specified as "mem", not "mem:%s"`, path)

	default:
		return DatabaseSpec{}, fmt.Errorf("Invalid database protocol: %s", spec)
	}
}

func splitAndParseDatabaseSpec(spec string) (DatabaseSpec, string, error) {
	parts := strings.SplitN(spec, "::", 2)
	if len(parts) != 2 {
		return DatabaseSpec{}, "", fmt.Errorf("Missing :: separator between database and dataset: %s", spec)
	}

	dbSpec, err := ParseDatabaseSpec(parts[0])
	if err != nil {
		return DatabaseSpec{}, "", err
	}

	return dbSpec, parts[1], nil
}

func parseDatasetSpec(spec string) (datasetSpec, error) {
	dbSpec, dsName, err := splitAndParseDatabaseSpec(spec)
	if err != nil {
		return datasetSpec{}, err
	}

	if !datasetRe.MatchString(dsName) {
		return datasetSpec{}, fmt.Errorf("Invalid dataset, must match %s: %s", datas.DatasetRe.String(), dsName)
	}

	return datasetSpec{dbSpec, dsName}, nil
}

// ParsePathSpec parses a path spec string into its parts
func ParsePathSpec(spec string) (PathSpec, error) {
	dbSpec, pathStr, err := splitAndParseDatabaseSpec(spec)
	if err != nil {
		return PathSpec{}, err
	}

	path, err := NewAbsolutePath(pathStr)
	if err != nil {
		return PathSpec{}, err
	}

	return PathSpec{dbSpec, path}, nil
}

func (spec DatabaseSpec) String() string {
	return spec.Protocol + ":" + spec.Path
}

func (spec DatabaseSpec) Database() (ds datas.Database, err error) {
	switch spec.Protocol {
	case "http", "https":
		err = d.Unwrap(d.Try(func() {
			ds = datas.NewRemoteDatabase(spec.String(), "Bearer "+spec.accessToken)
		}))
	case "ldb":
		err = d.Unwrap(d.Try(func() {
			ds = datas.NewDatabase(getLDBStore(spec.Path))
		}))
	case "mem":
		ds = datas.NewDatabase(chunks.NewMemoryStore())
	default:
		err = fmt.Errorf("Invalid path prototocol: %s", spec.Protocol)
	}
	return
}

func (spec datasetSpec) Dataset() (datas.Dataset, error) {
	store, err := spec.DbSpec.Database()
	if err != nil {
		return datas.Dataset{}, err
	}

	return store.GetDataset(spec.DatasetName), nil
}

func (spec datasetSpec) String() string {
	return spec.DbSpec.String() + Separator + spec.DatasetName
}

func (spec datasetSpec) Value() (datas.Database, types.Value, error) {
	dataset, err := spec.Dataset()
	if err != nil {
		return nil, nil, err
	}

	if commit, ok := dataset.MaybeHead(); ok {
		return dataset.Database(), commit, nil
	}
	dataset.Database().Close()
	return nil, nil, fmt.Errorf("No head value for dataset: %s", spec.DatasetName)
}

func (spec PathSpec) Value() (db datas.Database, val types.Value, err error) {
	db, err = spec.DbSpec.Database()
	if err != nil {
		return
	}

	val = spec.Path.Resolve(db)
	return
}

func RegisterDatabaseFlags(flags *flag.FlagSet) {
	chunks.RegisterLevelDBFlags(flags)
}

func CreateDatabaseSpecString(protocol, path string) string {
	return fmt.Sprintf("%s:%s", protocol, path)
}

func CreateValueSpecString(protocol, path, value string) string {
	return fmt.Sprintf("%s:%s::%s", protocol, path, value)
}

func CreateHashSpecString(protocol, path string, h hash.Hash) string {
	return fmt.Sprintf("%s:%s::#%s", protocol, path, h.String())
}

func getLDBStore(path string) chunks.ChunkStore {
	if store, ok := ldbStores[path]; ok {
		store.AddRef()
		return store
	}

	store := newRefCountingLdbStore(path, func() {
		delete(ldbStores, path)
	})
	ldbStores[path] = store
	return store
}
