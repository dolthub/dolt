// Copyright 2021 Dolthub, Inc.
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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangeMerge(t *testing.T) {
	tests := []struct {
		name   string
		ranges []Range
		merged []Range
	}{
		{
			name:   "smoke test",
			ranges: []Range{},
			merged: []Range{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, err := MergeRanges(test.ranges...)
			require.NoError(t, err)
			assert.Equal(t, test.merged, m)
		})
	}
}

func TestRangeSort(t *testing.T) {
	tests := []struct {
		name   string
		ranges []Range
	}{
		{
			name:   "smoke test",
			ranges: []Range{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cp := copyRangeSlice(test.ranges)
			rand.Shuffle(len(cp), func(i, j int) {
				cp[i], cp[j] = cp[j], cp[i]
			})
			SortRanges(cp)
			assert.Equal(t, test.ranges, cp)
		})
	}
}

func copyRangeSlice(s []Range) (c []Range) {
	c = make([]Range, len(s))
	for i := range s {
		c[i] = Range{
			Start:   s[i].Start,
			Stop:    s[i].Stop,
			KeyDesc: s[i].KeyDesc,
		}
	}
	return
}
