// Copyright 2023 Dolthub, Inc.
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

package index

import (
	"math"
	"math/bits"
	"sort"

	"github.com/dolthub/go-mysql-server/sql/expression/function/spatial"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// LexFloat maps the float64 into an uint64 representation in lexicographical order
// For negative floats, we flip all the bits
// For non-negative floats, we flip the signed bit
func LexFloat(f float64) uint64 {
	b := math.Float64bits(f)
	if b>>63 == 1 {
		return ^b
	}
	return b ^ (1 << 63)
}

// UnLexFloat maps the lexicographic uint64 representation of a float64 back into a float64
// For negative int64s, we flip all the bits
// For non-negative int64s, we flip the signed bit
func UnLexFloat(b uint64) float64 {
	if b>>63 == 1 {
		b = b ^ (1 << 63)
	} else {
		b = ^b
	}
	return math.Float64frombits(b)
}

// ZValue takes a Point and interleaves the bits into a [2]uint64
// It will put the bits in this order: x_0, y_0, x_1, y_1 ... x_63, Y_63
func ZValue(p types.Point) [2]uint64 {
	xLex, yLex := LexFloat(p.X), LexFloat(p.Y)

	res := [2]uint64{}
	for i := 0; i < 2; i++ {
		for j := 0; j < 32; j++ {
			x, y := (xLex&1)<<1, yLex&1
			res[1-i] |= (x | y) << (2 * j)
			xLex, yLex = xLex>>1, yLex>>1
		}
	}

	return res
}

// UnZValue takes a [2]uint64 Z-Value and converts it back to a sql.Point
func UnZValue(z [2]uint64) types.Point {
	x, y, zv := uint64(0), uint64(0), z[0]
	for i := 0; i < 32; i++ {
		y |= (zv & 1) << (32 - i)
		zv >>= 1
		x |= (zv & 1) << (32 - i)
		zv >>= 1
	}
	x, y, zv = x<<32, y<<32, z[1]
	for i := 0; i < 32; i++ {
		y |= (zv & 1) << (32 - i)
		zv >>= 1
		x |= (zv & 1) << (32 - i)
		zv >>= 1
	}
	xf := UnLexFloat(x)
	yf := UnLexFloat(y)
	return types.Point{X: xf, Y: yf}
}

func ZSort(points []types.Point) []types.Point {
	sort.Slice(points, func(i, j int) bool {
		zi, zj := ZValue(points[i]), ZValue(points[j])
		if zi[0] == zj[0] {
			return zi[1] < zj[1]
		}
		return zi[0] < zj[0]
	})
	return points
}

// ZAddr converts the GeometryValue into a key: (min_z_val, level)
// Note: there is an inefficiency here where small polygons may be placed into a level that's significantly larger
func ZAddr(v types.GeometryValue) [17]byte {
	bbox := spatial.FindBBox(v)
	zMin := ZValue(types.Point{X: bbox[0], Y: bbox[1]})
	zMax := ZValue(types.Point{X: bbox[2], Y: bbox[3]})

	addr := [17]byte{}
	for i := 0; i < 8; i++ {
		addr[i] = byte((zMin[0] >> (8 * (7 - i))) & 0xFF)
	}
	for i := 0; i < 8; i++ {
		addr[8 + i] = byte((zMin[1] >> (8 * (7 - i))) & 0xFF)
	}

	addr[16]  = uint8(bits.LeadingZeros64(zMin[0] ^ zMax[0]) + bits.LeadingZeros64(zMin[1] ^ zMax[1]))
	return addr
}
