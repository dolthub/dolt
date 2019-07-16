// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"regexp"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// DatasetRe is a regexp that matches a legal Dataset name anywhere within the
// target string.
var DatasetRe = regexp.MustCompile(`[a-zA-Z0-9\-_/]+`)

// DatasetFullRe is a regexp that matches a only a target string that is
// entirely legal Dataset name.
var DatasetFullRe = regexp.MustCompile("^" + DatasetRe.String() + "$")

// Dataset is a named Commit within a Database.
type Dataset struct {
	db   Database
	id   string
	head types.Value
}

func newDataset(db Database, id string, head types.Value) Dataset {
	// precondition checks
	d.PanicIfFalse(head == nil || IsCommit(head))
	return Dataset{db, id, head}
}

// Database returns the Database object in which this Dataset is stored.
// WARNING: This method is under consideration for deprecation.
func (ds Dataset) Database() Database {
	return ds.db
}

// ID returns the name of this Dataset.
func (ds Dataset) ID() string {
	return ds.id
}

// MaybeHead returns the current Head Commit of this Dataset, which contains
// the current root of the Dataset's value tree, if available. If not, it
// returns a new Commit and 'false'.
func (ds Dataset) MaybeHead() (types.Struct, bool) {
	if ds.head == nil {
		return types.Struct{}, false
	}
	return ds.head.(types.Struct), true
}

// MaybeHeadRef returns the Ref of the current Head Commit of this Dataset,
// which contains the current root of the Dataset's value tree, if available.
// If not, it returns an empty Ref and 'false'.
func (ds Dataset) MaybeHeadRef() (types.Ref, bool) {
	if ds.head == nil {
		return types.Ref{}, false
	}
	return types.NewRef(ds.head, ds.Database().Format()), true
}

// HasHead() returns 'true' if this dataset has a Head Commit, false otherwise.
func (ds Dataset) HasHead() bool {
	return ds.head != nil
}

// MaybeHeadValue returns the Value field of the current head Commit, if
// available. If not it returns nil and 'false'.
func (ds Dataset) MaybeHeadValue() (types.Value, bool) {
	if c, ok := ds.MaybeHead(); ok {
		return c.Get(ValueField), true
	}
	return nil, false
}

func IsValidDatasetName(name string) bool {
	return DatasetFullRe.MatchString(name)
}
