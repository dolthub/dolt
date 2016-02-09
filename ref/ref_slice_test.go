package ref

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefSliceSort(t *testing.T) {
	assert := assert.New(t)

	rs := RefSlice{}
	for i := 1; i <= 3; i++ {
		for j := 1; j <= 3; j++ {
			d := Sha1Digest{}
			for k := 1; k <= j; k++ {
				d[k-1] = byte(i)
			}
			rs = append(rs, New(d))
		}
	}

	rs2 := RefSlice(make([]Ref, len(rs)))
	copy(rs2, rs)
	sort.Sort(sort.Reverse(rs2))
	assert.False(rs.Equals(rs2))

	sort.Sort(rs2)
	assert.True(rs.Equals(rs2))
}
