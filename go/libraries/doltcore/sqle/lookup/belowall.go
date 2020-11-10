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

// BelowAll represents the position beyond the minimum possible value.
type BelowAll struct{}

var _ Cut = BelowAll{}

// Compare implements Cut.
func (BelowAll) Compare(Cut) (int, error) {
	return -1, nil
}

// Equals implements Cut.
func (BelowAll) Equals(c Cut) bool {
	_, ok := c.(BelowAll)
	return ok
}

// Format implements Cut.
func (ba BelowAll) Format() *types.NomsBinFormat {
	return types.Format_Default
}

// Less implements Cut.
func (BelowAll) Less(types.Tuple) (bool, error) {
	return true, nil
}

// String implements Cut.
func (BelowAll) String() string {
	return "BelowAll"
}

// TypeAsLowerBound implements Cut.
func (BelowAll) TypeAsLowerBound() BoundType {
	panic("BelowAll TypeAsLowerBound should be unreachable")
}

// TypeAsUpperBound implements Cut.
func (BelowAll) TypeAsUpperBound() BoundType {
	panic("BelowAll TypeAsUpperBound should be unreachable")
}
