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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
)

// CheckPrimaryKeys verifies that |from| and |to| have compatible
// primary key sets to be diffed by [prolly.DiffMaps]. If primary key
// sets have changed, it returns [diff.ErrPrimaryKeySetChanged].
//
// TODO(#12013): Support diffing across primary key set changes.
func CheckPrimaryKeys(from, to prolly.Map, fsch, tsch schema.Schema) error {
	if fsch == nil || tsch == nil {
		return nil
	}
	if schema.IsKeyless(fsch) || schema.IsKeyless(tsch) {
		return nil
	}
	if schema.ArePrimaryKeySetsDiffable(fsch, tsch) {
		return nil
	}

	fromKD, _ := from.Descriptors()
	toKD, _ := to.Descriptors()
	if fromKD != nil && toKD != nil && fromKD.Equals(toKD) && fsch.GetPKCols().Size() == tsch.GetPKCols().Size() {
		equal := true
		for i := 0; i < fsch.GetPKCols().Size(); i++ {
			fc := fsch.GetPKCols().GetByIndex(i)
			tc := tsch.GetPKCols().GetByIndex(i)
			if fc.Tag != tc.Tag && !strings.EqualFold(fc.Name, tc.Name) {
				equal = false
				break
			}
		}
		if equal {
			return nil
		}
	}

	return ErrPrimaryKeySetChanged.New()
}
