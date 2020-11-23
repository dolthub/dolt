// Copyright 2020 Dolthub, Inc.
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

package lookup

import "github.com/dolthub/dolt/go/store/types"

// AboveAll represents the position beyond the maximum possible value.
type AboveAll struct{}

var _ Cut = AboveAll{}

// Compare implements Cut.
func (AboveAll) Compare(Cut) (int, error) {
	return 1, nil
}

// Equals implements Cut.
func (AboveAll) Equals(c Cut) bool {
	_, ok := c.(AboveAll)
	return ok
}

// Format implements Cut.
func (aa AboveAll) Format() *types.NomsBinFormat {
	return types.Format_Default
}

// Less implements Cut.
func (AboveAll) Less(types.Tuple) (bool, error) {
	return false, nil
}

// String implements Cut.
func (AboveAll) String() string {
	return "AboveAll"
}

// TypeAsLowerBound implements Cut.
func (AboveAll) TypeAsLowerBound() BoundType {
	panic("AboveAll TypeAsLowerBound should be unreachable")
}

// TypeAsUpperBound implements Cut.
func (AboveAll) TypeAsUpperBound() BoundType {
	panic("AboveAll TypeAsUpperBound should be unreachable")
}
