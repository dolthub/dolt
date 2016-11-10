// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package spec provides builders and parsers for spelling Noms databases,
// datasets and values.
package spec

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
)

const (
	Separator = "::"
)

var (
	datasetRe = regexp.MustCompile("^" + datas.DatasetRe.String() + "$")
	ldbStores = map[string]*refCountingLdbStore{}
)

// SpecOptions customize Spec behavior.
type SpecOptions struct {
	// Authorization token for requests. For example, if the database is HTTP
	// this will used for an `Authorization: Bearer ${authorization}` header.
	Authorization string
}

// Spec describes a Database, Dataset, or a path to a Value. They should be
// constructed by parsing strings, either through ForDatabase, ForDataset, or
// ForPath (or their Opts variations).
type Spec struct {
	// Spec is the spec string this was parsed into.
	Spec string

	// Protocol is one of "mem", "ldb", "http", or "https".
	Protocol string

	// DatabaseName is the name of the Spec's database, which is the string after
	// "protocol:". http/https specs include their leading "//" characters.
	DatabaseName string

	// Options are the SpecOptions that the Spec was constructed with.
	Options SpecOptions

	// DatasetName is empty unless the spec was created with ForDataset.
	DatasetName string

	// Path is nil unless the spec was created with ForPath.
	Path AbsolutePath

	// db is lazily created, so it needs to be a pointer to a Database.
	db *datas.Database
}

func newSpec(spec string, dbSpec string, opts SpecOptions) (Spec, error) {
	protocol, dbName, err := parseDatabaseSpec(dbSpec)
	if err != nil {
		return Spec{}, err
	}

	return Spec{
		Spec:         spec,
		Protocol:     protocol,
		DatabaseName: dbName,
		Options:      opts,
		db:           new(datas.Database),
	}, nil
}

// ForDatabase parses a spec for a Database.
func ForDatabase(spec string) (Spec, error) {
	return ForDatabaseOpts(spec, SpecOptions{})
}

// ForDatabaseOpts parses a spec for a Database.
func ForDatabaseOpts(spec string, opts SpecOptions) (Spec, error) {
	return newSpec(spec, spec, opts)
}

// ForDataset parses a spec for a Dataset.
func ForDataset(spec string) (Spec, error) {
	return ForDatasetOpts(spec, SpecOptions{})
}

// ForDatasetOpts parses a spec for a Dataset.
func ForDatasetOpts(spec string, opts SpecOptions) (Spec, error) {
	dbSpec, dsName, err := splitDatabaseSpec(spec)
	if err != nil {
		return Spec{}, err
	}

	if !datasetRe.MatchString(dsName) {
		return Spec{}, fmt.Errorf("Dataset %s must match %s", dsName, datasetRe.String())
	}

	sp, err := newSpec(spec, dbSpec, opts)
	if err != nil {
		return Spec{}, err
	}

	sp.DatasetName = dsName
	return sp, nil
}

// ForPath parses a spec for a path to a Value.
func ForPath(spec string) (Spec, error) {
	return ForPathOpts(spec, SpecOptions{})
}

// ForPathOpts parses a spec for a path to a Value.
func ForPathOpts(spec string, opts SpecOptions) (Spec, error) {
	dbSpec, pathStr, err := splitDatabaseSpec(spec)
	if err != nil {
		return Spec{}, err
	}

	path, err := NewAbsolutePath(pathStr)
	if err != nil {
		return Spec{}, err
	}

	sp, err := newSpec(spec, dbSpec, opts)
	if err != nil {
		return Spec{}, err
	}

	sp.Path = path
	return sp, nil
}

// GetDatabase returns the Database instance that this Spec's DatabaseName
// describes. The same Database instance is returned every time, unless Close
// is called. If the Spec is closed, it is re-opened with a new Database.
func (sp Spec) GetDatabase() datas.Database {
	if *sp.db == nil {
		*sp.db = sp.createDatabase()
	}
	return *sp.db
}

// NewChunkStore returns a new ChunkStore instance that this Spec's
// DatabaseName describes. It's unusual to call this method, GetDatabase is
// more useful. Unlike GetDatabase, a new ChunkStore instance is returned every
// time. If there is no ChunkStore, for example remote databases, returns nil.
func (sp Spec) NewChunkStore() chunks.ChunkStore {
	switch sp.Protocol {
	case "http", "https":
		return nil
	case "ldb":
		return getLdbStore(sp.DatabaseName)
	case "mem":
		return chunks.NewMemoryStore()
	}
	panic("unreachable")
}

// GetDataset returns the current Dataset instance for this Spec's Database.
// GetDataset is live, so if Commit is called on this Spec's Database later, a
// new up-to-date Dataset will returned on the next call to GetDataset.  If
// this is not a Dataset spec, returns nil.
func (sp Spec) GetDataset() (ds datas.Dataset) {
	if sp.DatasetName != "" {
		ds = sp.GetDatabase().GetDataset(sp.DatasetName)
	}
	return
}

// GetValue returns the Value at this Spec's Path within its Database, or nil
// if this isn't a Path Spec or if that path isn't found.
func (sp Spec) GetValue() (val types.Value) {
	if !sp.Path.IsEmpty() {
		val = sp.Path.Resolve(sp.GetDatabase())
	}
	return
}

// Href treats the Protocol and DatabaseName as a URL, and returns its href.
// For example, the spec http://example.com/path::ds returns
// "http://example.com/path". If the Protocol is not "http" or "http", returns
// an empty string.
func (sp Spec) Href() string {
	proto := sp.Protocol
	if proto == "http" || proto == "https" {
		return proto + ":" + sp.DatabaseName
	}
	return ""
}

// Pin returns a Spec in which the dataset component, if any, has been replaced
// with the hash of the HEAD of that dataset. This "pins" the path to the state
// of the database at the current moment in time.  Returns itself if the
// PathSpec is already "pinned".
func (sp Spec) Pin() (Spec, bool) {
	var ds datas.Dataset

	if !sp.Path.IsEmpty() {
		if !sp.Path.Hash.IsEmpty() {
			// Spec is already pinned.
			return sp, true
		}

		ds = sp.GetDatabase().GetDataset(sp.Path.Dataset)
	} else {
		ds = sp.GetDataset()
	}

	commit, ok := ds.MaybeHead()
	if !ok {
		return Spec{}, false
	}

	spec := sp.Protocol + sp.DatabaseName + Separator + "#" + commit.Hash().String()
	if sp.Path.Path != nil {
		spec += sp.Path.Path.String()
	}

	pinned, err := ForPathOpts(spec, sp.Options)
	d.PanicIfError(err)
	*pinned.db = *sp.db

	return pinned, true
}

func (sp Spec) Close() error {
	db := *sp.db
	if db == nil {
		return nil
	}

	*sp.db = nil
	return db.Close()
}

func (sp Spec) createDatabase() datas.Database {
	switch sp.Protocol {
	case "http", "https":
		return datas.NewRemoteDatabase(sp.Href(), sp.Options.Authorization)
	case "ldb":
		return datas.NewDatabase(getLdbStore(sp.DatabaseName))
	case "mem":
		return datas.NewDatabase(chunks.NewMemoryStore())
	}
	panic("unreachable")
}

func parseDatabaseSpec(spec string) (protocol, name string, err error) {
	if len(spec) == 0 {
		err = fmt.Errorf("Empty spec")
		return
	}

	parts := strings.SplitN(spec, ":", 2) // [protocol] [, path]?

	// If there was no ":" then this is either a mem spec, or a filesystem path.
	// This is ambiguous if the file system path is "mem" but that just means the
	// path needs to be explicitly "ldb:mem".
	if len(parts) == 1 {
		if spec == "mem" {
			protocol = "mem"
		} else {
			protocol, name = "ldb", spec
		}
		return
	}

	switch parts[0] {
	case "ldb":
		protocol, name = parts[0], parts[1]

	case "http", "https":
		var u *url.URL
		u, err = url.Parse(spec)
		if err == nil && u.Host == "" {
			err = fmt.Errorf("%s has empty host", spec)
		}
		if err == nil {
			protocol, name = parts[0], parts[1]
		}

	case "mem":
		err = fmt.Errorf(`In-memory database must be specified as "mem", not "mem:"`)

	default:
		err = fmt.Errorf("Invalid database protocol %s in %s", protocol, spec)
	}
	return
}

func splitDatabaseSpec(spec string) (string, string, error) {
	lastIdx := strings.LastIndex(spec, Separator)
	if lastIdx == -1 {
		return "", "", fmt.Errorf("Missing %s after database in %s", Separator, spec)
	}

	return spec[:lastIdx], spec[lastIdx+len(Separator):], nil
}

func getLdbStore(path string) chunks.ChunkStore {
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
