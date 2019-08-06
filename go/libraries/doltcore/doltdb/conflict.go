// Copyright 2019 Liquidata, Inc.
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

package doltdb

import "github.com/liquidata-inc/dolt/go/store/types"

type Conflict struct {
	Base       types.Value
	Value      types.Value
	MergeValue types.Value
}

func NewConflict(base, value, mergeValue types.Value) Conflict {
	if base == nil {
		base = types.NullValue
	}
	if value == nil {
		value = types.NullValue
	}
	if mergeValue == nil {
		mergeValue = types.NullValue
	}
	return Conflict{base, value, mergeValue}
}

func ConflictFromTuple(tpl types.Tuple) (Conflict, error) {
	base, err := tpl.Get(0)

	if err != nil {
		return Conflict{}, err
	}

	val, err := tpl.Get(1)

	if err != nil {
		return Conflict{}, err
	}

	mv, err := tpl.Get(2)

	if err != nil {
		return Conflict{}, err
	}
	return Conflict{base, val, mv}, nil
}

func (c Conflict) ToNomsList(vrw types.ValueReadWriter) (types.Tuple, error) {
	return types.NewTuple(vrw.Format(), c.Base, c.Value, c.MergeValue)
}
