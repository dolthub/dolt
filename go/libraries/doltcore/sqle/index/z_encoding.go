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
	"encoding/binary"
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

// InterleaveUInt64 interleaves the bits of the uint64s x and y.
// The first 32 bits of x and y must be 0.
// Example:
// 0000 0000 0000 0000 0000 0000 0000 0000 abcd efgh ijkl mnop abcd efgh ijkl mnop
// 0000 0000 0000 0000 abcd efgh ijkl mnop 0000 0000 0000 0000 abcd efgh ijkl mnop
// 0000 0000 abcd efgh 0000 0000 ijkl mnop 0000 0000 abcd efgh 0000 0000 ijkl mnop
// 0000 abcd 0000 efgh 0000 ijkl 0000 mnop 0000 abcd 0000 efgh 0000 ijkl 0000 mnop
// 00ab 00cd 00ef 00gh 00ij 00kl 00mn 00op 00ab 00cd 00ef 00gh 00ij 00kl 00mn 00op
// 0a0b 0c0d 0e0f 0g0h 0i0j 0k0l 0m0n 0o0p 0a0b 0c0d 0e0f 0g0h 0i0j 0k0l 0m0n 0o0p
// Alternatively, just precompute all the results from 0 to 0x0000FFFFF
func InterleaveUInt64(x, y uint64) uint64 {
	x = (x | (x << 16)) & 0x0000FFFF0000FFFF
	y = (y | (y << 16)) & 0x0000FFFF0000FFFF

	x = (x | (x << 8)) & 0x00FF00FF00FF00FF
	y = (y | (y << 8)) & 0x00FF00FF00FF00FF

	x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F
	y = (y | (y << 4)) & 0x0F0F0F0F0F0F0F0F

	x = (x | (x << 2)) & 0x3333333333333333
	y = (y | (y << 2)) & 0x3333333333333333

	x = (x | (x << 1)) & 0x5555555555555555
	y = (y | (y << 1)) & 0x5555555555555555

	return x | (y << 1)
}

// ZValue takes a Point and interleaves the bits into a [2]uint64
// It will put the bits in this order: x_0, y_0, x_1, y_1 ... x_63, Y_63
func ZValue(p types.Point) (z [2]uint64) {
	xLex, yLex := LexFloat(p.X), LexFloat(p.Y)
	z[0], z[1] = InterleaveUInt64(xLex>>32, yLex>>32), InterleaveUInt64(xLex&0xFFFFFFFF, yLex&0xFFFFFFFF)
	return
}

// UnInterleaveUint64 splits up the bits of the uint64 z into two uint64s
// The first 32 bits of x and y must be 0.
// Example:
// abcd efgh ijkl mnop abcd efgh ijkl mnop abcd efgh ijkl mnop abcd efgh ijkl mnop 0x5555555555555555
// 0b0d 0f0h 0j0l 0n0p 0b0d 0f0h 0j0l 0n0p 0b0d 0f0h 0j0l 0n0p 0b0d 0f0h 0j0l 0n0p x | x >> 1
// 0bbd dffh hjjl lnnp pbbd dffh hjjl lnnp pbbd dffh hjjl lnnp pnbd dffh hjjl lnnp 0x3333333333333333
// 00bd 00fh 00jl 00np 00bd 00fh 00jl 00np 00bd 00fh 00jl 00np 00bd 00fh 00jl 00np x | x >> 2
// 0000 bdfh fhjl jlnp npbd bdfh fhjl jlnp npdb bdfh fhjl jlnp npdb bdfh fhjl jlnp 0x0F0F0F0F0F0F0F0F
// 0000 bdfh 0000 jlnp 0000 bdfh 0000 jlnp 0000 bdfh 0000 jlnp 0000 bdfh 0000 jlnp x | x >> 4
// 0000 bdfh bdfh jlnp jlnp bdfh bdfh jlnp jlnp bdfh bdfh jlnp jlnp bdfh bdfh jlnp 0x00FF00FF00FF00FF
// 0000 0000 bdfh jlnp 0000 0000 bdfh jlnp 0000 0000 bdfh jlnp 0000 0000 bdfh jlnp x | x >> 8
// 0000 0000 0000 0000 bdfh jlnp bdfh jlnp bdfh jlnp bdfh jlnp bdfh jlnp bdfh jlnp 0x0000FFFF0000FFFF
// 0000 0000 0000 0000 bdfh jlnp bdfh jlnp 0000 0000 0000 0000 bdfh jlnp bdfh jlnp x | x >> 16
// 0000 0000 0000 0000 bdfh jlnp bdfh jlnp bdfh jlnp bdfh jlnp bdfh jlnp bdfh jlnp 0x00000000FFFFFFFF
// 0000 0000 0000 0000 0000 0000 0000 0000 bdfh jlnp bdfh jlnp bdfh jlnp bdfh jlnp
func UnInterleaveUint64(z uint64) (x, y uint64) {
	x, y = z, z>>1

	x &= 0x5555555555555555
	x |= x >> 1
	y &= 0x5555555555555555
	y |= y >> 1

	x &= 0x3333333333333333
	x |= x >> 2
	y &= 0x3333333333333333
	y |= y >> 2

	x &= 0x0F0F0F0F0F0F0F0F
	x |= x >> 4
	y &= 0x0F0F0F0F0F0F0F0F
	y |= y >> 4

	x &= 0x00FF00FF00FF00FF
	x |= x >> 8
	y &= 0x00FF00FF00FF00FF
	y |= y >> 8

	x &= 0x0000FFFF0000FFFF
	x |= x >> 16
	y &= 0x0000FFFF0000FFFF
	y |= y >> 16

	x &= 0xFFFFFFFF
	y &= 0xFFFFFFFF
	return
}

// UnZValue takes a [2]uint64 Z-Value and converts it back to a sql.Point
func UnZValue(z [2]uint64) types.Point {
	xl, yl := UnInterleaveUint64(z[0])
	xr, yr := UnInterleaveUint64(z[1])
	xf := UnLexFloat((xl << 32) | xr)
	yf := UnLexFloat((yl << 32) | yr)
	return types.Point{X: xf, Y: yf}
}

func ZSort(points []types.Point) []types.Point {
	sort.Slice(points, func(i, j int) bool {
		zi, zj := ZValue(points[i]), ZValue(points[j])
		return zi[0] < zj[0] || (zi[0] == zj[0] && zi[1] < zi[1])
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
	binary.BigEndian.PutUint64(addr[1:], zMin[0])
	binary.BigEndian.PutUint64(addr[9:], zMin[1])
	if res := zMin[0] ^ zMax[0]; res != 0 {
		addr[0] = byte(64 - bits.LeadingZeros64(res)/2)
	} else {
		addr[0] = byte(32 + bits.LeadingZeros64(zMin[1]^zMax[1])/2)
	}
	return addr
}

// ZAddrSort converts the GeometryValue into a key: (min_z_val, level)
// Note: there is an inefficiency here where small polygons may be placed into a level that's significantly larger
func ZAddrSort(geoms []types.GeometryValue) []types.GeometryValue {
	sort.Slice(geoms, func(i, j int) bool {
		zi, zj := ZAddr(geoms[i]), ZAddr(geoms[j])
		return bytes.Compare(zi[:], zj[:]) < 0
	})
	return geoms
}
