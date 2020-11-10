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
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/store/types"
)

// Cut represents a position on the line of all possible values.
type Cut interface {
	// Compare returns an integer stating the relative position of the calling Cut to the given Cut.
	Compare(Cut) (int, error)
	// Equals returns whether the given Cut equals the calling Cut.
	Equals(Cut) bool
	// Format returns the NomsBinFormat.
	Format() *types.NomsBinFormat
	// Less returns whether the calling Cut represents a position smaller than the given tuple.
	Less(types.Tuple) (bool, error)
	// String returns the Cut as a string for debugging purposes. Will panic on errors.
	String() string
	// TypeAsLowerBound returns the bound type if the calling Cut is the lower bound of a range.
	TypeAsLowerBound() BoundType
	// TypeAsLowerBound returns the bound type if the calling Cut is the upper bound of a range.
	TypeAsUpperBound() BoundType
}

// GetKey returns the inner tuple from the given Cut.
func GetKey(c Cut) types.Tuple {
	switch c := c.(type) {
	case Below:
		return c.key
	case Above:
		return c.key
	default:
		panic(fmt.Errorf("GetKey on '%T' should be impossible", c))
	}
}

func cutKeyToString(tpl types.Tuple) string {
	var vals []string
	iter, err := tpl.Iterator()
	if err != nil {
		panic(err)
	}
	for iter.HasMore() {
		_, val, err := iter.Next()
		if err != nil {
			panic(err)
		}
		str, err := types.EncodedValue(context.Background(), val)
		if err != nil {
			panic(err)
		}
		vals = append(vals, str)
	}
	return fmt.Sprintf("%s", strings.Join(vals, ","))
}
