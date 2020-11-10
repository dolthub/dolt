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

// Above represents the position immediately below the contained key.
type Below struct {
	key types.Tuple
}

var _ Cut = Below{}

// Compare implements Cut.
func (b Below) Compare(c Cut) (int, error) {
	switch c := c.(type) {
	case AboveAll:
		return -1, nil
	case BelowAll:
		return 1, nil
	case Below:
		ok, err := b.key.Less(b.key.Format(), c.key)
		if err != nil {
			return 0, err
		}
		if ok {
			return -1, nil
		}
		if b.key.Equals(c.key) {
			return 0, nil
		}
		return 1, nil
	case Above:
		ok, err := c.key.Less(c.key.Format(), b.key)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return -1, nil
	default:
		panic(fmt.Errorf("unrecognized Cut type '%T'", c))
	}
}

// Equals implements Cut.
func (b Below) Equals(c Cut) bool {
	other, ok := c.(Below)
	if !ok {
		return false
	}
	return b.key.Equals(other.key)
}

// Format implements Cut.
func (b Below) Format() *types.NomsBinFormat {
	if b.key.Empty() {
		return types.Format_Default
	}
	return b.key.Format()
}

// Less implements Cut.
func (b Below) Less(tpl types.Tuple) (bool, error) {
	ok, err := b.key.Less(b.key.Format(), tpl)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	return b.key.Equals(tpl), nil
}

// String implements Cut.
func (b Below) String() string {
	return fmt.Sprintf("Below[%s]", cutKeyToString(b.key))
}

// TypeAsLowerBound implements Cut.
func (Below) TypeAsLowerBound() BoundType {
	return Closed
}

// TypeAsUpperBound implements Cut.
func (Below) TypeAsUpperBound() BoundType {
	return Open
}
