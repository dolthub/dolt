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
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/dolthub/go-mysql-server/sql/expression/function/spatial"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/val"
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

// ZVal consists of uint64 x and y with bits their interleaved
// ZVal[0] contains the upper 64 bits of x and y interleaved
// ZVal[1] contains the lower 64 bits of x and y interleaved
type ZVal = [2]uint64

// ZValue takes a Point, Lexes the x and y values, and interleaves the bits into a [2]uint64
// It will put the bits in this order: x_0, y_0, x_1, y_1 ... x_63, Y_63
func ZValue(p types.Point) (z ZVal) {
	xLex, yLex := LexFloat(p.X), LexFloat(p.Y)
	z[0], z[1] = InterleaveUInt64(xLex>>32, yLex>>32), InterleaveUInt64(xLex&0xFFFFFFFF, yLex&0xFFFFFFFF)
	return
}

// UnZValue takes a ZVal and converts it back to a sql.Point
func UnZValue(z [2]uint64) types.Point {
	xl, yl := UnInterleaveUint64(z[0])
	xr, yr := UnInterleaveUint64(z[1])
	xf := UnLexFloat((xl << 32) | xr)
	yf := UnLexFloat((yl << 32) | yr)
	return types.Point{X: xf, Y: yf}
}

// ZMask masks in pairs by shifting based off of level (shift amount)
func ZMask(level byte, zVal ZVal) val.Cell {
	cell := val.Cell{}
	cell[0] = level
	if level < 32 {
		shamt := level << 1
		binary.BigEndian.PutUint64(cell[1:], zVal[0])
		binary.BigEndian.PutUint64(cell[9:], (zVal[1]>>shamt)<<shamt)
	} else {
		shamt := (level - 32) << 1
		binary.BigEndian.PutUint64(cell[1:], (zVal[0]>>shamt)<<shamt)
	}
	return cell
}

// ZCell converts the GeometryValue into a Cell
// Note: there is an inefficiency here where small polygons may be placed into a level that's significantly larger
func ZCell(v types.GeometryValue) val.Cell {
	bbox := spatial.FindBBox(v)
	zMin := ZValue(types.Point{X: bbox[0], Y: bbox[1]})
	zMax := ZValue(types.Point{X: bbox[2], Y: bbox[3]})

	// Level rounds up by adding 1 and dividing by two (same as a left shift by 1)
	var level byte
	if zMin[0] != zMax[0] {
		level = byte((bits.Len64(zMin[0]^zMax[0])+1)>>1) + 32
	} else {
		level = byte((bits.Len64(zMin[1]^zMax[1]) + 1) >> 1)
	}
	return ZMask(level, zMin)
}

// ZRange is a pair of two ZVals
// ZRange[0] is the lower bound (z-min)
// ZRange[1] is the upper bound (z-max)
type ZRange = [2]ZVal

// mergeZRanges combines the z-ranges in acc with zRange by either
// 1. combining the last ZRange in acc with zRange if the ranges are next to each other or
// 2. appending zRange to acc
func mergeZRanges(acc []ZRange, zRange ZRange) []ZRange {
	n := len(acc) - 1
	if n >= 0 && acc[n][1][0] == zRange[0][0] && zRange[0][1]-acc[n][1][1] == 1 {
		acc[n][1] = zRange[1]
		return acc
	}
	return append(acc, zRange)
}

// zRangeSize retrieves the approximate size of the zRange
// it only takes the top 64 bits of the difference
// it accepts and returns a shift-amount so that comparison between two zRangeSizes are consistent
func zRangeSize(zRange ZRange, shamt int) (uint64, int) {
	zVal := ZVal{}
	zVal[0] = zRange[1][0] - zRange[0][0]
	if zRange[1][1] < zRange[0][1] {
		zVal[0] -= 1
		zVal[1] = ^zRange[1][1] - zRange[0][1]
	} else {
		zVal[1] = zRange[1][1] - zRange[0][1]
	}
	if shamt == -1 {
		shamt = bits.LeadingZeros64(zVal[0])
	}
	zVal[0] = zVal[0] << shamt
	zVal[1] = zVal[1] >> (64 - shamt)
	return zVal[0] | zVal[1], shamt
}

// Thresholds to stop splitting ZRanges
const cutThresh = 0.02
const depthThresh = 4

// Masks for every other bit to avoid un-interleaving
// Depending on prefixLength these will be shifted to either fill x or y values with 0s or 1s
// while not altering the bits of their counterparts
const xMask = 0x5555555555555555
const yMask = 0xAAAAAAAAAAAAAAAA

// shouldCut checks if the size of the removed ZRange divided by the size of the whole ZRange is smaller than cutThresh
// This is used to get splitZRanges to stop recursing
func shouldCut(cutRange ZRange, size float64, shamt int) bool {
	cut, _ := zRangeSize(cutRange, shamt)
	return (float64(cut) / size) >= cutThresh
}

// isContinuous checks if the provided zRange is entirely within the bounding box
func isContinuous(zl, zh uint64, prefixLength int) bool {
	mask := uint64(math.MaxUint64 >> prefixLength)
	return (zl&mask) == 0 && (zh&mask) == mask
}

// splitZRanges is a helper function to SplitZRanges
func splitZRanges(zRange ZRange, zSize float64, zShamt, depth int, acc []ZRange) []ZRange {
	// prevent too much splitting and point lookup is continuous
	if depth == 0 || zRange[0] == zRange[1] {
		return mergeZRanges(acc, zRange)
	}

	zl, zh := zRange[0], zRange[1]
	zRangeL, zRangeR := zRange, zRange
	if zl[0] != zh[0] {
		prefixLength := bits.LeadingZeros64(zl[0] ^ zh[0])
		if zl[1] == 0 && zh[1] == math.MaxUint64 && isContinuous(zl[0], zh[0], prefixLength) {
			return mergeZRanges(acc, zRange)
		}

		// upper bound for left range; set 0 fill with 1s
		suffixLength := 64 - prefixLength
		zRangeL[1][0] |= yMask >> prefixLength       // set suffix to all 1s
		zRangeL[1][0] &= ^(1 << (suffixLength - 1))  // set first suffix bit to 0
		zRangeL[1][1] |= yMask >> (prefixLength % 2) // set suffix to all 1s

		// lower bound for right range; set 1 fill with 0s
		suffixMask := uint64(math.MaxUint64<<suffixLength) | (xMask >> prefixLength)
		zRangeR[0][0] &= suffixMask                  // set suffix to all 0s
		zRangeR[0][0] |= 1 << (suffixLength - 1)     // set first suffix bit to 1
		zRangeR[0][1] &= xMask << (prefixLength % 2) // set suffix to all 0s
	} else {
		prefixLength := bits.LeadingZeros64(zl[1] ^ zh[1])
		if isContinuous(zl[1], zh[1], prefixLength) {
			return mergeZRanges(acc, zRange)
		}

		// upper bound for left range; set 0 fill with 1s
		suffixLength := 64 - prefixLength
		zRangeL[1][1] |= yMask >> prefixLength      // set suffix to all 1s
		zRangeL[1][1] &= ^(1 << (suffixLength - 1)) // set at prefix to 0

		// lower bound for right range; set 1 fill with 0s
		suffixMask := uint64(math.MaxUint64<<suffixLength) | (xMask >> prefixLength)
		zRangeR[0][1] &= suffixMask              // set suffix to all 0s
		zRangeR[0][1] |= 1 << (suffixLength - 1) // set at prefix to 1
	}

	if !shouldCut(ZRange{zRangeL[1], zRangeR[0]}, zSize, zShamt) {
		return mergeZRanges(acc, zRange)
	}

	// recurse on left and right ranges
	acc = splitZRanges(zRangeL, zSize, zShamt, depth-1, acc)
	acc = splitZRanges(zRangeR, zSize, zShamt, depth-1, acc)

	return acc
}

// SplitZRanges takes a ZRange and splits it into continuous ZRanges within the bounding box
// A ZRange is continuous if
// 1. it is a point (the lower and upper bounds are equal)
// 2. the ranges are within a cell (the suffixes of the bounds range from 00...0 to 11...1)
func SplitZRanges(zRange ZRange) []ZRange {
	zSize, zShamt := zRangeSize(zRange, -1)
	return splitZRanges(zRange, float64(zSize), zShamt, depthThresh, make([]ZRange, 0, 128))
}
