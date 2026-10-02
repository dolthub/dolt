// Copyright 2024 Dolthub, Inc.
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

package diff

import (
	"context"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
)

// AlignMaps aligns the primary keys of |from| and |to| so they can
// be diffed by [prolly.DiffMaps]. In this version, if primary key sets
// have changed, it returns [diff.ErrPrimaryKeySetChanged].
//
// TODO(#12013): Support diffing across primary key set changes.
func AlignMaps(ctx context.Context, from, to prolly.Map, fsch, tsch schema.Schema) (prolly.Map, prolly.Map, schema.Schema, schema.Schema, error) {
	if fsch == nil || tsch == nil || schema.IsKeyless(fsch) || schema.IsKeyless(tsch) {
		return from, to, fsch, tsch, nil
	}
	if schema.ArePrimaryKeySetsDiffable(fsch, tsch) {
		return from, to, fsch, tsch, nil
	}

	fromKD, _ := from.Descriptors()
	toKD, _ := to.Descriptors()
	if fromKD != nil && toKD != nil && fromKD.Equals(toKD) && equalPKCols(fsch, tsch) {
		return from, to, fsch, tsch, nil
	}

	return prolly.Map{}, prolly.Map{}, nil, nil, ErrPrimaryKeySetChanged.New()
}

func equalPKCols(fsch, tsch schema.Schema) bool {
	if fsch.GetPKCols().Size() != tsch.GetPKCols().Size() {
		return false
	}
	for i := 0; i < fsch.GetPKCols().Size(); i++ {
		fc := fsch.GetPKCols().GetByIndex(i)
		tc := tsch.GetPKCols().GetByIndex(i)
		if fc.Tag != tc.Tag && !strings.EqualFold(fc.Name, tc.Name) {
			return false
		}
	}
	return true
}
