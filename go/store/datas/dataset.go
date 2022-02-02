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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"regexp"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/types"
)

// DatasetRe is a regexp that matches a legal Dataset name anywhere within the
// target string.
var DatasetRe = regexp.MustCompile(`[a-zA-Z0-9\-_/]+`)

// DatasetFullRe is a regexp that matches a only a target string that is
// entirely legal Dataset name.
var DatasetFullRe = regexp.MustCompile("^" + DatasetRe.String() + "$")

// Dataset is a named value within a Database. Different head values may be stored in a dataset. Most commonly, this is
// a commit, but other values are also supported in some cases.
type Dataset struct {
	db   *database
	id   string
	head types.Value
}

func newDataset(db *database, id string, head types.Value) (Dataset, error) {
	check := head == nil

	var err error
	if !check {
		check, err = IsCommit(head)

		if err != nil {
			return Dataset{}, err
		}
	}

	if !check {
		check, err = IsTag(head)

		if err != nil {
			return Dataset{}, err
		}
	}

	if !check {
		check, err = IsWorkingSet(head)

		if err != nil {
			return Dataset{}, err
		}
	}

	// precondition checks
	d.PanicIfFalse(check)
	return Dataset{db, id, head}, nil
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
func (ds Dataset) MaybeHeadRef() (types.Ref, bool, error) {
	if ds.head == nil {
		return types.Ref{}, false, nil
	}
	ref, err := types.NewRef(ds.head, ds.db.Format())

	if err != nil {
		return types.Ref{}, false, err
	}

	return ref, true, nil
}

// HasHead() returns 'true' if this dataset has a Head Commit, false otherwise.
func (ds Dataset) HasHead() bool {
	return ds.head != nil
}

// MaybeHeadValue returns the Value field of the current head Commit, if
// available. If not it returns nil and 'false'.
func (ds Dataset) MaybeHeadValue() (types.Value, bool, error) {
	if c, ok := ds.MaybeHead(); ok {
		return c.MaybeGet(ValueField)
	}
	return nil, false, nil
}

func IsValidDatasetName(name string) bool {
	return DatasetFullRe.MatchString(name)
}
