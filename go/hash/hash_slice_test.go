// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package hash

import (
	"sort"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestHashSliceSort(t *testing.T) {
	assert := assert.New(t)

	rs := HashSlice{}
	for i := 1; i <= 3; i++ {
		for j := 1; j <= 3; j++ {
			h := Hash{}
			for k := 1; k <= j; k++ {
				h[k-1] = byte(i)
			}
			rs = append(rs, h)
		}
	}

	rs2 := HashSlice(make([]Hash, len(rs)))
	copy(rs2, rs)
	sort.Sort(sort.Reverse(rs2))
	assert.False(rs.Equals(rs2))

	sort.Sort(rs2)
	assert.True(rs.Equals(rs2))
}
