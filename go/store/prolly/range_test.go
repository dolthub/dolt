// Copyright 2022 Dolthub, Inc.
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

package prolly

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/val"
)

func TestRangeBounds(t *testing.T) {
	twoCol := val.NewTupleDescriptor(
		val.Type{Enc: val.Int32Enc},
		val.Type{Enc: val.Int32Enc},
	)

	tests := []struct {
		name      string
		testRange Range
		inside    []val.Tuple
		outside   []val.Tuple
	}{
		{
			name:      "range [1,2:4,2]",
			testRange: ClosedRange(intTuple(1, 2), intTuple(4, 2), twoCol),
			inside: []val.Tuple{
				intTuple(1, 2), intTuple(1, 3), intTuple(2, 1), intTuple(2, 2),
				intTuple(2, 3), intTuple(3, 1), intTuple(3, 2), intTuple(3, 3),
				intTuple(4, 1), intTuple(4, 2),
			},
			outside: []val.Tuple{
				intTuple(1, 1), intTuple(4, 3),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rng := test.testRange
			for _, tup := range test.inside {
				inStart, inStop := rng.AboveStart(tup), rng.BelowStop(tup)
				assert.True(t, inStart && inStop,
					"%s should be in range %s \n",
					rng.Desc.Format(tup), rng.format())
			}
			for _, tup := range test.outside {
				inStart, inStop := rng.AboveStart(tup), rng.BelowStop(tup)
				assert.False(t, inStart && inStop,
					"%s should not be in range %s \n",
					rng.Desc.Format(tup), rng.format())

			}
		})
	}
}
