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
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/dolthub/go-mysql-server/sql/expression/function/spatial"
	"github.com/dolthub/go-mysql-server/sql/types"
	assert "github.com/stretchr/testify/require"
)

// these are sorted
var ps = []types.Point{
	{X: -2, Y: -2}, // 0
	{X: -1, Y: -2},
	{X: -2, Y: -1},
	{X: -1, Y: -1},
	{X: 0, Y: -2}, // 4
	{X: 1, Y: -2},
	{X: 2, Y: -2},
	{X: 0, Y: -1},
	{X: 1, Y: -1}, // 8
	{X: 2, Y: -1},
	{X: -2, Y: 0},
	{X: -2, Y: 1},
	{X: -1, Y: 0}, // 12
	{X: -1, Y: 1},
	{X: -2, Y: 2},
	{X: -1, Y: 2},
	{X: 0, Y: 0}, // 16
	{X: 1, Y: 0},
	{X: 0, Y: 1},
	{X: 1, Y: 1},
	{X: 2, Y: 0}, // 20
	{X: 2, Y: 1},
	{X: 0, Y: 2},
	{X: 1, Y: 2},
	{X: 2, Y: 2}, // 24
}

func TestLexFloat(t *testing.T) {
	t.Run("test edge case lex float values", func(t *testing.T) {
		assert.Equal(t, uint64(0x0010000000000000), LexFloat(-math.MaxFloat64))
		assert.Equal(t, uint64(0x7ffffffffffffffe), LexFloat(-math.SmallestNonzeroFloat64))
		assert.Equal(t, uint64(0x8000000000000000), LexFloat(0.0))
		assert.Equal(t, uint64(0x8000000000000001), LexFloat(math.SmallestNonzeroFloat64))
		assert.Equal(t, uint64(0xffefffffffffffff), LexFloat(math.MaxFloat64))
		assert.Equal(t, uint64(0xfff8000000000001), LexFloat(math.NaN()))
		assert.Equal(t, uint64(0x0007fffffffffffe), LexFloat(-math.NaN()))
		assert.Equal(t, uint64(0xfff0000000000000), LexFloat(math.Inf(1)))
		assert.Equal(t, uint64(0x000fffffffffffff), LexFloat(math.Inf(-1)))
	})

	t.Run("test reverse lex float values", func(t *testing.T) {
		assert.Equal(t, -math.MaxFloat64, UnLexFloat(0x0010000000000000))
		assert.Equal(t, -math.SmallestNonzeroFloat64, UnLexFloat(0x7ffffffffffffffe))
		assert.Equal(t, 0.0, UnLexFloat(0x8000000000000000))
		assert.Equal(t, math.SmallestNonzeroFloat64, UnLexFloat(0x8000000000000001))
		assert.Equal(t, math.MaxFloat64, UnLexFloat(0xffefffffffffffff))
		assert.True(t, math.IsNaN(UnLexFloat(0xfff8000000000001)))
		assert.True(t, math.IsNaN(UnLexFloat(0xfff7fffffffffffe)))
		assert.True(t, math.IsInf(UnLexFloat(0xfff0000000000000), 1))
		assert.True(t, math.IsInf(UnLexFloat(0x000fffffffffffff), -1))
	})

	t.Run("test sort lex float", func(t *testing.T) {
		// math.NaN not here, because NaN != NaN
		sortedFloats := []float64{
			math.Inf(-1),
			-math.MaxFloat64,
			-1.0,
			-0.5,
			-0.123456789,
			-math.SmallestNonzeroFloat64,
			-0.0,
			0.0,
			math.SmallestNonzeroFloat64,
			0.5,
			0.987654321,
			1.0,
			math.MaxFloat64,
			math.Inf(1),
		}

		randFloats := append([]float64{}, sortedFloats...)
		rand.Shuffle(len(randFloats), func(i, j int) {
			randFloats[i], randFloats[j] = randFloats[j], randFloats[i]
		})
		sort.Slice(randFloats, func(i, j int) bool {
			l1 := LexFloat(randFloats[i])
			l2 := LexFloat(randFloats[j])
			return l1 < l2
		})
		assert.Equal(t, sortedFloats, randFloats)
	})
}

func TestZValue(t *testing.T) {
	tests := []struct {
		p types.Point
		e string
	}{
		{
			p: types.Point{X: -5000, Y: -5000},
			e: "0fff30f03f3fffffffffffffffffffff",
		},
		{
			p: types.Point{X: -1, Y: -1},
			e: "300000ffffffffffffffffffffffffff",
		},
		{
			p: types.Point{X: -1, Y: 0},
			e: "90000055555555555555555555555555",
		},
		{
			p: types.Point{X: -1, Y: 1},
			e: "9aaaaa55555555555555555555555555",
		},
		{
			p: types.Point{X: 0, Y: -1},
			e: "600000aaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		{
			p: types.Point{X: 1, Y: -1},
			e: "655555aaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		{
			p: types.Point{X: 0, Y: 0},
			e: "c0000000000000000000000000000000",
		},
		{
			p: types.Point{X: 1, Y: 0},
			e: "c5555500000000000000000000000000",
		},
		{
			p: types.Point{X: 0, Y: 1},
			e: "caaaaa00000000000000000000000000",
		},
		{
			p: types.Point{X: 1, Y: 1},
			e: "cfffff00000000000000000000000000",
		},
		{
			p: types.Point{X: 2, Y: 2},
			e: "f0000000000000000000000000000000",
		},
		{
			p: types.Point{X: 50000, Y: 50000},
			e: "f000fcc03ccc00000000000000000000",
		},
	}

	t.Run("test z-values", func(t *testing.T) {
		for _, test := range tests {
			z := ZValue(test.p)
			assert.Equal(t, test.e, fmt.Sprintf("%016x%016x", z[0], z[1]))
		}
	})

	t.Run("test un-z-values", func(t *testing.T) {
		for _, test := range tests {
			v, _ := hex.DecodeString(test.e)
			z := [2]uint64{}
			z[0] = binary.BigEndian.Uint64(v[:8])
			z[1] = binary.BigEndian.Uint64(v[8:])
			assert.Equal(t, test.p, UnZValue(z))
		}
	})

	t.Run("test sorting points by z-value", func(t *testing.T) {
		sortedPoints := []types.Point{
			{X: -5000, Y: -5000},
			{X: -1, Y: -1},
			{X: 1, Y: -1},
			{X: -1, Y: 0},
			{X: -1, Y: 1},
			{X: 0, Y: 0},
			{X: 1, Y: 0},
			{X: 1, Y: 1},
			{X: 2, Y: 2},
			{X: 100, Y: 100},
		}
		randPoints := append([]types.Point{}, sortedPoints...)
		rand.Shuffle(len(randPoints), func(i, j int) {
			randPoints[i], randPoints[j] = randPoints[j], randPoints[i]
		})
		sort.Slice(randPoints, func(i, j int) bool {
			z1 := ZValue(randPoints[i])
			z2 := ZValue(randPoints[j])
			if z1[0] != z2[0] {
				return z1[0] < z2[0]
			}
			return z1[1] < z2[1]
		})
		assert.Equal(t, sortedPoints, randPoints)
	})

	t.Run("test sorting many points by z-value", func(t *testing.T) {
		randPoints := append([]types.Point{}, ps...)
		rand.Shuffle(len(randPoints), func(i, j int) {
			randPoints[i], randPoints[j] = randPoints[j], randPoints[i]
		})
		sort.Slice(randPoints, func(i, j int) bool {
			z1 := ZValue(randPoints[i])
			z2 := ZValue(randPoints[j])
			if z1[0] != z2[0] {
				return z1[0] < z2[0]
			}
			return z1[1] < z2[1]
		})
		assert.Equal(t, ps, randPoints)
	})
}

func TestZCell(t *testing.T) {
	t.Run("test points ZCell", func(t *testing.T) {
		p := types.Point{X: 1, Y: 2}
		res := ZCell(p)
		assert.Equal(t, "00e5555500000000000000000000000000", hex.EncodeToString(res[:]))
	})

	t.Run("test linestring ZCell", func(t *testing.T) {
		a := types.Point{X: 1, Y: 1}
		b := types.Point{X: 2, Y: 2}
		c := types.Point{X: 3, Y: 3}
		l := types.LineString{Points: []types.Point{a, b, c}}
		res := ZCell(l)
		assert.Equal(t, "3fc0000000000000000000000000000000", hex.EncodeToString(res[:]))
	})

	t.Run("test polygon ZCell", func(t *testing.T) {
		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p := types.Polygon{Lines: []types.LineString{l}}
		res := ZCell(p)
		assert.Equal(t, "4000000000000000000000000000000000", hex.EncodeToString(res[:]))
	})

	t.Run("test low level linestring", func(t *testing.T) {
		line := types.LineString{Points: []types.Point{
			{X: 0, Y: 0},
			{X: math.SmallestNonzeroFloat64, Y: math.SmallestNonzeroFloat64},
		}}
		poly := types.Polygon{Lines: []types.LineString{line}}
		z := ZCell(poly)
		assert.Equal(t, "01c0000000000000000000000000000000", hex.EncodeToString(z[:]))
	})

	t.Run("test high level linestring", func(t *testing.T) {
		line := types.LineString{Points: []types.Point{
			{X: -1, Y: -1},
			{X: 1, Y: 1},
		}}
		poly := types.Polygon{Lines: []types.LineString{line}}
		z := ZCell(poly)
		assert.Equal(t, "4000000000000000000000000000000000", hex.EncodeToString(z[:]))
	})

	t.Run("test sorting many points by z-cell", func(t *testing.T) {
		sortedGeoms := make([]types.GeometryValue, len(ps))
		for i, p := range ps {
			sortedGeoms[i] = p
		}
		randGeoms := append([]types.GeometryValue{}, sortedGeoms...)
		rand.Shuffle(len(randGeoms), func(i, j int) {
			randGeoms[i], randGeoms[j] = randGeoms[j], randGeoms[i]
		})
		sort.Slice(randGeoms, func(i, j int) bool {
			zi, zj := ZCell(randGeoms[i]), ZCell(randGeoms[j])
			return bytes.Compare(zi[:], zj[:]) < 0
		})
		assert.Equal(t, sortedGeoms, randGeoms)
	})

	t.Run("test sorting linestring by z-cell", func(t *testing.T) {
		sortedLines := []types.GeometryValue{
			types.LineString{Points: []types.Point{ps[24], ps[24]}},
			types.LineString{Points: []types.Point{ps[16], ps[19]}},
			types.LineString{Points: []types.Point{ps[0], ps[3]}},
			types.LineString{Points: []types.Point{ps[19], ps[24]}},
			types.LineString{Points: []types.Point{ps[3], ps[19]}},
		}
		randPoints := append([]types.GeometryValue{}, sortedLines...)
		rand.Shuffle(len(randPoints), func(i, j int) {
			randPoints[i], randPoints[j] = randPoints[j], randPoints[i]
		})
		sort.Slice(randPoints, func(i, j int) bool {
			zi, zj := ZCell(randPoints[i]), ZCell(randPoints[j])
			return bytes.Compare(zi[:], zj[:]) < 0
		})
		assert.Equal(t, sortedLines, randPoints)
	})
}

var testZVals = []ZVal{
	{0, 0},  // (0, 0)
	{0, 1},  // (1, 0)
	{0, 2},  // (0, 1)
	{0, 3},  // (1, 1)
	{0, 4},  // (2, 0)
	{0, 5},  // (3, 0)
	{0, 6},  // (2, 1)
	{0, 7},  // (3, 1)
	{0, 8},  // (0, 2)
	{0, 9},  // (1, 2)
	{0, 10}, // (0, 3)
	{0, 11}, // (1, 3)
	{0, 12}, // (2, 2)
	{0, 13}, // (3, 2)
	{0, 14}, // (2, 3)
	{0, 15}, // (3, 3)
	{0, 16}, // (4, 0)
}

func TestSplitZRanges(t *testing.T) {
	depth := 4
	t.Run("split point z-range", func(t *testing.T) {
		zRange := ZRange{testZVals[0], testZVals[0]} // (0, 0) -> (0, 0)
		zRanges := SplitZRanges(zRange, depth)
		assert.Equal(t, []ZRange{zRange}, zRanges)

		zRange = ZRange{testZVals[1], testZVals[1]} // (1, 0) -> (1, 0)
		zRanges = SplitZRanges(zRange, depth)
		assert.Equal(t, []ZRange{zRange}, zRanges)

		zRange = ZRange{testZVals[2], testZVals[2]} // (0, 1) -> (0, 1)
		zRanges = SplitZRanges(zRange, depth)
		assert.Equal(t, []ZRange{zRange}, zRanges)

		zRange = ZRange{testZVals[3], testZVals[3]} // (1, 1) -> (1, 1)
		zRanges = SplitZRanges(zRange, depth)
		assert.Equal(t, []ZRange{zRange}, zRanges)
	})

	t.Run("split continuous z-ranges", func(t *testing.T) {
		zRange := ZRange{testZVals[0], testZVals[1]} // (0, 0) -> (1, 0)
		zRanges := SplitZRanges(zRange, depth)
		assert.Equal(t, []ZRange{zRange}, zRanges)

		zRange = ZRange{testZVals[0], testZVals[3]} // (0, 0) -> (1, 1)
		zRanges = SplitZRanges(zRange, depth)
		assert.Equal(t, []ZRange{zRange}, zRanges)
	})

	t.Run("split small non-continuous z-ranges", func(t *testing.T) {
		zRange := ZRange{testZVals[0], testZVals[2]} // (0, 0) -> (0, 1)
		zRanges := SplitZRanges(zRange, depth)
		assert.Equal(t, []ZRange{{testZVals[0], testZVals[0]}, {testZVals[2], testZVals[2]}}, zRanges)
	})

	t.Run("split small non-continuous z-range that should have a merge", func(t *testing.T) {
		zRange := ZRange{testZVals[0], testZVals[6]} // (0, 0) -> (2, 1)
		zRanges := SplitZRanges(zRange, depth)
		assert.Equal(t, []ZRange{{testZVals[0], testZVals[4]}, {testZVals[6], testZVals[6]}}, zRanges)
	})

	t.Run("split x-axis bbox", func(t *testing.T) {
		zRange := ZRange{{0, 0}, {0, 16}} // (0, 0) -> (4, 0)
		zRanges := SplitZRanges(zRange, depth)
		assert.Equal(t, []ZRange{{testZVals[0], testZVals[1]}, {testZVals[4], testZVals[5]}, {testZVals[16], testZVals[16]}}, zRanges)
	})

	t.Run("split y-axis bbox", func(t *testing.T) {
		zRange := ZRange{{0, 0}, {0, 32}} // (0, 0) -> (0, 2)
		zRanges := SplitZRanges(zRange, depth)
		res := []ZRange{
			{{0, 0}, {0, 0}},
			{{0, 2}, {0, 2}},
			{{0, 8}, {0, 8}},
			{{0, 10}, {0, 10}},
			{{0, 32}, {0, 32}},
		}
		assert.Equal(t, res, zRanges)
	})

	t.Run("split medium x-axis bbox", func(t *testing.T) {
		zRange := ZRange{{0, 0}, {0, uint64(1 << 42)}} // (0, 0) -> (2^21, 0)
		zRanges := SplitZRanges(zRange, depth)
		res := []ZRange{
			{{0x0, 0x00000000000}, {0x0, 0x00555555555}},
			{{0x0, 0x01000000000}, {0x0, 0x01555555555}},
			{{0x0, 0x04000000000}, {0x0, 0x04555555555}},
			{{0x0, 0x05000000000}, {0x0, 0x05555555555}},
			{{0x0, 0x10000000000}, {0x0, 0x10555555555}},
			{{0x0, 0x11000000000}, {0x0, 0x11555555555}},
			{{0x0, 0x14000000000}, {0x0, 0x14555555555}},
			{{0x0, 0x15000000000}, {0x0, 0x15555555555}},
			{{0x0, 0x40000000000}, {0x0, 0x40000000000}},
		}
		assert.Equal(t, res, zRanges)
	})

	t.Run("split medium y-axis bbox", func(t *testing.T) {
		zRange := ZRange{{0, 0}, {0, uint64(1 << 43)}} // (0, 0) -> (0, 2^21)
		zRanges := SplitZRanges(zRange, depth)
		res := []ZRange{
			{{0x0, 0x00000000000}, {0x0, 0x00aaaaaaaaa}},
			{{0x0, 0x02000000000}, {0x0, 0x02aaaaaaaaa}},
			{{0x0, 0x08000000000}, {0x0, 0x08aaaaaaaaa}},
			{{0x0, 0x0a000000000}, {0x0, 0x0aaaaaaaaaa}},
			{{0x0, 0x20000000000}, {0x0, 0x20aaaaaaaaa}},
			{{0x0, 0x22000000000}, {0x0, 0x22aaaaaaaaa}},
			{{0x0, 0x28000000000}, {0x0, 0x28aaaaaaaaa}},
			{{0x0, 0x2a000000000}, {0x0, 0x2aaaaaaaaaa}},
			{{0x0, 0x80000000000}, {0x0, 0x80000000000}},
		}
		assert.Equal(t, res, zRanges)
	})

	t.Run("split x-axis bbox", func(t *testing.T) {
		zRange := ZRange{{0, 0x0B}, {0, 0x25}} // (1, 3) -> (3, 4)
		zRanges := SplitZRanges(zRange, depth)
		res := []ZRange{
			{{0, 0x0B}, {0, 0x0B}},
			{{0, 0x0E}, {0, 0x0F}},
			{{0, 0x21}, {0, 0x21}},
			{{0, 0x24}, {0, 0x25}},
		}
		assert.Equal(t, res, zRanges)
	})

	t.Run("split large x-axis bbox", func(t *testing.T) {
		zRange := ZRange{{0, 0}, {1, 0}} // (0, 0) -> (2^33, 0)
		zRanges := SplitZRanges(zRange, depth)
		res := []ZRange{
			{{0x0, 0x0000000000000000}, {0x0, 0x0155555555555555}},
			{{0x0, 0x0400000000000000}, {0x0, 0x0555555555555555}},
			{{0x0, 0x1000000000000000}, {0x0, 0x1155555555555555}},
			{{0x0, 0x1400000000000000}, {0x0, 0x1555555555555555}},
			{{0x0, 0x4000000000000000}, {0x0, 0x4155555555555555}},
			{{0x0, 0x4400000000000000}, {0x0, 0x4555555555555555}},
			{{0x0, 0x5000000000000000}, {0x0, 0x5155555555555555}},
			{{0x0, 0x5400000000000000}, {0x0, 0x5555555555555555}},
			{{0x1, 0x0000000000000000}, {0x1, 0x0000000000000000}},
		}
		assert.Equal(t, res, zRanges)
	})

	t.Run("split large y-axis bbox", func(t *testing.T) {
		zRange := ZRange{{0, 0}, {2, 0}} // (0, 0) -> (0, 2^66)
		zRanges := SplitZRanges(zRange, depth)
		res := []ZRange{
			{{0x0, 0x0000000000000000}, {0x0, 0x02aaaaaaaaaaaaaa}},
			{{0x0, 0x0800000000000000}, {0x0, 0x0aaaaaaaaaaaaaaa}},
			{{0x0, 0x2000000000000000}, {0x0, 0x22aaaaaaaaaaaaaa}},
			{{0x0, 0x2800000000000000}, {0x0, 0x2aaaaaaaaaaaaaaa}},
			{{0x0, 0x8000000000000000}, {0x0, 0x82aaaaaaaaaaaaaa}},
			{{0x0, 0x8800000000000000}, {0x0, 0x8aaaaaaaaaaaaaaa}},
			{{0x0, 0xa000000000000000}, {0x0, 0xa2aaaaaaaaaaaaaa}},
			{{0x0, 0xa800000000000000}, {0x0, 0xaaaaaaaaaaaaaaaa}},
			{{0x2, 0x0000000000000000}, {0x2, 0x0000000000000000}},
		}
		assert.Equal(t, res, zRanges)
	})

	t.Run("split seattle bbox range", func(t *testing.T) {
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{
			{X: -122.48, Y: 47.41},
			{X: -122.48, Y: 47.79},
			{X: -122.16, Y: 47.79},
			{X: -122.16, Y: 47.41},
			{X: -122.48, Y: 47.41},
		}}}}
		bbox := spatial.FindBBox(poly)
		zMin := ZValue(types.Point{X: bbox[0], Y: bbox[1]})
		zMax := ZValue(types.Point{X: bbox[2], Y: bbox[3]})
		zRange := ZRange{zMin, zMax}
		zRanges := SplitZRanges(zRange, depth)
		res := []ZRange{
			{{0xa555642b9e213a9d, 0xec56213a9dec5620}, {0xa555642b9e3fffff, 0xffffffffffffffff}},
			{{0xa555642b9e602a88, 0xa802202a88a80220}, {0xa555642b9e7fffff, 0xffffffffffffffff}},
			{{0xa555642b9e811015, 0x4454011015445400}, {0xa555642b9ebfffff, 0xffffffffffffffff}},
			{{0xa555642b9ec00000, 0x0000000000000000}, {0xa555642b9effffff, 0xffffffffffffffff}},
			{{0xa555642b9f202a88, 0xa802202a88a80220}, {0xa555642b9f2fffff, 0xffffffffffffffff}},
			{{0xa555642b9f302a88, 0xa802202a88a80220}, {0xa555642b9f3bfaae, 0xeaffbbfaaeeaffbb}},
			{{0xa555642b9f800000, 0x0000000000000000}, {0xa555642b9f9bfaae, 0xeaffbbfaaeeaffbb}},
			{{0xa555642b9fa00000, 0x0000000000000000}, {0xa555642b9fbbfaae, 0xeaffbbfaaeeaffbb}},
			{{0xa555642bb4011015, 0x4454011015445400}, {0xa555642bb4ffffff, 0xffffffffffffffff}},
			{{0xa555642bb5000000, 0x0000000000000000}, {0xa555642bb5bbfaae, 0xeaffbbfaaeeaffbb}},
			{{0xa555642bb6011015, 0x4454011015445400}, {0xa555642bb6ffffff, 0xffffffffffffffff}},
			{{0xa555642bb7000000, 0x0000000000000000}, {0xa555642bb7bbfaae, 0xeaffbbfaaeeaffbb}},
			{{0xa555642bbc011015, 0x4454011015445400}, {0xa555642bbc3757fd, 0xdfd57757fddfd577}},
			{{0xa555642bbc400000, 0x0000000000000000}, {0xa555642bbc7757fd, 0xdfd57757fddfd577}},
			{{0xa555642bbd000000, 0x0000000000000000}, {0xa555642bbd1bfaae, 0xeaffbbfaaeeaffbb}},
			{{0xa555642bbd200000, 0x0000000000000000}, {0xa555642bbd3352ac, 0xcad53352accad533}},
		}
		assert.Equal(t, res, zRanges)
	})
}

func BenchmarkSplitZRanges(b *testing.B) {
	b.Run("small continuous", func(b *testing.B) {
		zRange := ZRange{testZVals[0], testZVals[3]} // (0, 0) -> (1, 1)
		b.ResetTimer()
		var zRanges []ZRange
		for n := 0; n < b.N; n++ {
			zRanges = SplitZRanges(zRange, 4)
		}
		assert.Equal(b, 1, len(zRanges))
	})

	b.Run("large range, depth 2", func(b *testing.B) {
		zRange := ZRange{{0, 0}, {1, 0}} // (0, 0) -> (2^33, 0)
		b.ResetTimer()
		var zRanges []ZRange
		for n := 0; n < b.N; n++ {
			zRanges = SplitZRanges(zRange, 2)
		}
		assert.Equal(b, 3, len(zRanges))
	})

	b.Run("large range, depth 4", func(b *testing.B) {
		zRange := ZRange{{0, 0}, {1, 0}} // (0, 0) -> (2^33, 0)
		b.ResetTimer()
		var zRanges []ZRange
		for n := 0; n < b.N; n++ {
			zRanges = SplitZRanges(zRange, 4)
		}
		assert.Equal(b, 9, len(zRanges))
	})

	b.Run("large range, depth 16", func(b *testing.B) {
		zRange := ZRange{{0, 0}, {1, 0}} // (0, 0) -> (2^33, 0)
		b.ResetTimer()
		var zRanges []ZRange
		for n := 0; n < b.N; n++ {
			zRanges = SplitZRanges(zRange, 16)
		}
		assert.Equal(b, 32769, len(zRanges))
	})

	b.Run("typical range, depth 2", func(b *testing.B) {
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{
			{X: 0, Y: 0},
			{X: 10, Y: 0},
			{X: 10, Y: 10},
			{X: 0, Y: 10},
			{X: 10, Y: 10},
		}}}}
		bbox := spatial.FindBBox(poly)
		zMin := ZValue(types.Point{X: bbox[0], Y: bbox[1]})
		zMax := ZValue(types.Point{X: bbox[2], Y: bbox[3]})
		zRange := ZRange{zMin, zMax}
		var zRanges []ZRange
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			zRanges = SplitZRanges(zRange, 2)
		}
		assert.Equal(b, 4, len(zRanges))
	})

	b.Run("typical range, depth 4", func(b *testing.B) {
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{
			{X: 0, Y: 0},
			{X: 10, Y: 0},
			{X: 10, Y: 10},
			{X: 0, Y: 10},
			{X: 10, Y: 10},
		}}}}
		bbox := spatial.FindBBox(poly)
		zMin := ZValue(types.Point{X: bbox[0], Y: bbox[1]})
		zMax := ZValue(types.Point{X: bbox[2], Y: bbox[3]})
		zRange := ZRange{zMin, zMax}
		var zRanges []ZRange
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			zRanges = SplitZRanges(zRange, 4)
		}
		assert.Equal(b, 13, len(zRanges))
	})

	b.Run("typical range, depth 16", func(b *testing.B) {
		poly := types.Polygon{Lines: []types.LineString{{Points: []types.Point{
			{X: 0, Y: 0},
			{X: 10, Y: 0},
			{X: 10, Y: 10},
			{X: 0, Y: 10},
			{X: 10, Y: 10},
		}}}}
		bbox := spatial.FindBBox(poly)
		zMin := ZValue(types.Point{X: bbox[0], Y: bbox[1]})
		zMax := ZValue(types.Point{X: bbox[2], Y: bbox[3]})
		zRange := ZRange{zMin, zMax}
		var zRanges []ZRange
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			zRanges = SplitZRanges(zRange, 16)
		}
		assert.Equal(b, 21264, len(zRanges))
	})
}
