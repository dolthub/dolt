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
	"bytes"
	"math"
	"sort"

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

// ZValue takes a Point and interleaves the bits into a [16]byte
// It will put the bits in this order: x_0, y_0, x_1, y_1 ... x_63, Y_63
func ZValue(p types.Point) [16]byte {
	xLex := LexFloat(p.X)
	yLex := LexFloat(p.Y)

	res := [16]byte{}
	for i := 0; i < 16; i++ {
		for j := 0; j < 4; j++ {
			x, y := byte((xLex&1)<<1), byte(yLex&1)
			res[15-i] |= (x | y) << (2 * j)
			xLex, yLex = xLex>>1, yLex>>1
		}
	}
	return res
}

// UnZValue takes a [16]byte Z-Value and converts it back to a sql.Point
func UnZValue(z [16]byte) types.Point {
	var x, y uint64
	for i := 15; i >= 0; i-- {
		zv := uint64(z[i])
		for j := 3; j >= 0; j-- {
			y |= (zv & 1) << (63 - (4*i + j))
			zv >>= 1

			x |= (zv & 1) << (63 - (4*i + j))
			zv >>= 1
		}
	}
	xf := UnLexFloat(x)
	yf := UnLexFloat(y)
	return types.Point{X: xf, Y: yf}
}

func ZSort(points []types.Point) []types.Point {
	sort.Slice(points, func(i, j int) bool {
		zi, zj := ZValue(points[i]), ZValue(points[j])
		return bytes.Compare(zi[:], zj[:]) < 0
	})
	return points
}
