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

package spec

import (
	"context"
	"errors"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// AbsolutePath describes the location of a Value within a Noms database.
//
// To locate a value relative to some other value, see Path. To locate a value
// globally, see Spec.
//
// For more on paths, absolute paths, and specs, see:
// https://github.com/attic-labs/noms/blob/master/doc/spelling.md.
type AbsolutePath struct {
	// Dataset is the dataset this AbsolutePath is rooted at. Only one of
	// Dataset and Hash should be set.
	Dataset string
	// Hash is the hash this AbsolutePath is rooted at. Only one of Dataset and
	// Hash should be set.
	Hash hash.Hash
}

// NewAbsolutePath attempts to parse 'str' and return an AbsolutePath.
func NewAbsolutePath(str string) (AbsolutePath, error) {
	if len(str) == 0 {
		return AbsolutePath{}, errors.New("empty path")
	}

	var h hash.Hash
	var dataset string

	if str[0] == '#' {
		tail := str[1:]
		if len(tail) < hash.StringLen {
			return AbsolutePath{}, errors.New("invalid hash: " + tail)
		}

		hashStr := tail[:hash.StringLen]
		if h2, ok := hash.MaybeParse(hashStr); ok {
			h = h2
		} else {
			return AbsolutePath{}, errors.New("invalid hash: " + hashStr)
		}
	} else {
		// This form is only used in the noms command. Commands like `noms show` that use a path with '.' separation will
		// no longer work
		dataset = str
	}

	return AbsolutePath{Hash: h, Dataset: dataset}, nil
}

// Resolve returns the Value reachable by 'p' in 'db'.
func (p AbsolutePath) Resolve(ctx context.Context, db datas.Database, vrw types.ValueReadWriter) (val types.Value, err error) {
	if len(p.Dataset) > 0 {
		var ok bool
		ds, err := db.GetDataset(ctx, p.Dataset)
		if err != nil {
			return nil, err
		}

		if val, ok = ds.MaybeHead(); !ok {
			val = nil
		}
	} else if !p.Hash.IsEmpty() {
		var err error
		val, err = vrw.ReadValue(ctx, p.Hash)
		if err != nil {
			return nil, err
		}
	} else {
		panic("Unreachable")
	}

	return
}

func (p AbsolutePath) IsEmpty() bool {
	return p.Dataset == "" && p.Hash.IsEmpty()
}

func (p AbsolutePath) String() (str string) {
	if p.IsEmpty() {
		return ""
	}

	if len(p.Dataset) > 0 {
		str = p.Dataset
	} else if !p.Hash.IsEmpty() {
		str = "#" + p.Hash.String()
	} else {
		panic("Unreachable")
	}

	return str
}
