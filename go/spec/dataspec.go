// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/types"
)

var (
	datasetRe = regexp.MustCompile("^" + dataset.DatasetRe.String() + "$")
)

func GetDatabase(str string) (datas.Database, error) {
	sp, err := parseDatabaseSpec(str)
	if err != nil {
		return nil, err
	}
	return sp.Database()
}

func GetChunkStore(str string) (chunks.ChunkStore, error) {
	sp, err := parseDatabaseSpec(str)
	if err != nil {
		return nil, err
	}

	switch sp.Protocol {
	case "ldb":
		return chunks.NewLevelDBStoreUseFlags(sp.Path, ""), nil
	case "mem":
		return chunks.NewMemoryStore(), nil
	default:
		return nil, fmt.Errorf("Unable to create chunkstore for protocol: %s", str)
	}
}

func GetDataset(str string) (dataset.Dataset, error) {
	sp, err := parseDatasetSpec(str)
	if err != nil {
		return dataset.Dataset{}, err
	}
	return sp.Dataset()
}

func GetPath(str string) (datas.Database, types.Value, error) {
	sp, err := parsePathSpec(str)
	if err != nil {
		return nil, nil, err
	}
	return sp.Value()
}

type databaseSpec struct {
	Protocol    string
	Path        string
	accessToken string
}

type datasetSpec struct {
	DbSpec      databaseSpec
	DatasetName string
}

type pathSpec struct {
	DbSpec databaseSpec
	Path   AbsolutePath
}

func parseDatabaseSpec(spec string) (databaseSpec, error) {
	ldbDatabaseSpec := func(path string) (databaseSpec, error) {
		if len(path) == 0 {
			return databaseSpec{}, fmt.Errorf("Empty file system path")
		}
		return databaseSpec{Protocol: "ldb", Path: path}, nil
	}

	parts := strings.SplitN(spec, ":", 2) // [protocol] [, path]?
	protocol := parts[0]

	// If there was no ":" then this is either a mem spec, or a filesystem path.
	// This is ambiguous if the file system path is "mem" but that just means the path needs to be explicitly "ldb:mem".
	if len(parts) == 1 {
		if protocol == "mem" {
			return databaseSpec{Protocol: "mem"}, nil
		}
		return ldbDatabaseSpec(protocol)
	}

	path := parts[1]

	switch protocol {
	case "http", "https":
		u, err := url.Parse(spec)
		if err != nil || len(u.Host) == 0 {
			return databaseSpec{}, fmt.Errorf("Invalid URL: %s", spec)
		}
		token := u.Query().Get("access_token")
		return databaseSpec{Protocol: protocol, Path: path, accessToken: token}, nil

	case "ldb":
		return ldbDatabaseSpec(path)

	case "mem":
		return databaseSpec{}, fmt.Errorf(`In-memory database must be specified as "mem", not "mem:%s"`, path)

	default:
		return databaseSpec{}, fmt.Errorf("Invalid database protocol: %s", spec)
	}
}

func splitAndParseDatabaseSpec(spec string) (databaseSpec, string, error) {
	parts := strings.SplitN(spec, "::", 2)
	if len(parts) != 2 {
		return databaseSpec{}, "", fmt.Errorf("Missing :: separator between database and dataset: %s", spec)
	}

	dbSpec, err := parseDatabaseSpec(parts[0])
	if err != nil {
		return databaseSpec{}, "", err
	}

	return dbSpec, parts[1], nil
}

func parseDatasetSpec(spec string) (datasetSpec, error) {
	dbSpec, dsName, err := splitAndParseDatabaseSpec(spec)
	if err != nil {
		return datasetSpec{}, err
	}

	if !datasetRe.MatchString(dsName) {
		return datasetSpec{}, fmt.Errorf("Invalid dataset, must match %s: %s", dataset.DatasetRe.String(), dsName)
	}

	return datasetSpec{dbSpec, dsName}, nil
}

func parsePathSpec(spec string) (pathSpec, error) {
	dbSpec, pathStr, err := splitAndParseDatabaseSpec(spec)
	if err != nil {
		return pathSpec{}, err
	}

	path, err := NewAbsolutePath(pathStr)
	if err != nil {
		return pathSpec{}, err
	}

	return pathSpec{dbSpec, path}, nil
}

func (s databaseSpec) String() string {
	return s.Protocol + ":" + s.Path
}

func (spec databaseSpec) Database() (ds datas.Database, err error) {
	switch spec.Protocol {
	case "http", "https":
		err = d.Unwrap(d.Try(func() {
			ds = datas.NewRemoteDatabase(spec.String(), "Bearer "+spec.accessToken)
		}))
	case "ldb":
		err = d.Unwrap(d.Try(func() {
			ds = datas.NewDatabase(chunks.NewLevelDBStoreUseFlags(spec.Path, ""))
		}))
	case "mem":
		ds = datas.NewDatabase(chunks.NewMemoryStore())
	default:
		err = fmt.Errorf("Invalid path prototocol: %s", spec.Protocol)
	}
	return
}

func (spec datasetSpec) Dataset() (dataset.Dataset, error) {
	store, err := spec.DbSpec.Database()
	if err != nil {
		return dataset.Dataset{}, err
	}

	return dataset.NewDataset(store, spec.DatasetName), nil
}

func (s datasetSpec) String() string {
	return s.DbSpec.String() + "::" + s.DatasetName
}

func (spec datasetSpec) Value() (datas.Database, types.Value, error) {
	dataset, err := spec.Dataset()
	if err != nil {
		return nil, nil, err
	}

	commit, ok := dataset.MaybeHead()
	if !ok {
		dataset.Database().Close()
		return nil, nil, fmt.Errorf("No head value for dataset: %s", spec.DatasetName)
	}

	return dataset.Database(), commit, nil
}

func (spec pathSpec) Value() (db datas.Database, val types.Value, err error) {
	db, err = spec.DbSpec.Database()
	if err != nil {
		return
	}

	val = spec.Path.Resolve(db)
	return
}

func RegisterDatabaseFlags() {
	chunks.RegisterLevelDBFlags()
}
