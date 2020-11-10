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

import (
	"fmt"

	"github.com/dolthub/dolt/go/store/types"
)

// Above represents the position immediately above the contained key.
type Above struct {
	key types.Tuple
}

var _ Cut = Above{}

// Compare implements Cut.
func (a Above) Compare(c Cut) (int, error) {
	switch c := c.(type) {
	case AboveAll:
		return -1, nil
	case BelowAll:
		return 1, nil
	case Above:
		ok, err := a.key.Less(a.key.Format(), c.key)
		if err != nil {
			return 0, err
		}
		if ok {
			return -1, nil
		}
		if a.key.Equals(c.key) {
			return 0, nil
		}
		return 1, nil
	case Below:
		ok, err := a.key.Less(a.key.Format(), c.key)
		if err != nil {
			return 0, err
		}
		if ok {
			return -1, nil
		}
		return 1, nil
	default:
		panic(fmt.Errorf("unrecognized Cut type '%T'", c))
	}
}

// Equals implements Cut.
func (a Above) Equals(c Cut) bool {
	other, ok := c.(Above)
	if !ok {
		return false
	}
	return a.key.Equals(other.key)
}

// Format implements Cut.
func (a Above) Format() *types.NomsBinFormat {
	if a.key.Empty() {
		return types.Format_Default
	}
	return a.key.Format()
}

// Less implements Cut.
func (a Above) Less(tpl types.Tuple) (bool, error) {
	return a.key.Less(a.key.Format(), tpl)
}

// String implements Cut.
func (a Above) String() string {
	return fmt.Sprintf("Above[%s]", cutKeyToString(a.key))
}

// TypeAsLowerBound implements Cut.
func (Above) TypeAsLowerBound() BoundType {
	return Open
}

// TypeAsUpperBound implements Cut.
func (Above) TypeAsUpperBound() BoundType {
	return Closed
}
