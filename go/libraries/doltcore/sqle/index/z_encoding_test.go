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
	"encoding/hex"
	"math"
	"math/rand"
	"sort"
	"testing"

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
	t.Run("test z-values", func(t *testing.T) {
		z := ZValue(types.Point{X: -5000, Y: -5000})
		assert.Equal(t, [2]uint64{0x0fff30f03f3fffff, 0xffffffffffffffff}, z)

		z = ZValue(types.Point{X: -1, Y: -1})
		assert.Equal(t, [2]uint64{0x300000ffffffffff, 0xffffffffffffffff}, z)

		z = ZValue(types.Point{X: -1, Y: 0})
		assert.Equal(t, [2]uint64{0x9000005555555555, 0x5555555555555555}, z)

		z = ZValue(types.Point{X: -1, Y: 1})
		assert.Equal(t, [2]uint64{0x9aaaaa5555555555, 0x5555555555555555}, z)

		z = ZValue(types.Point{X: 0, Y: -1})
		assert.Equal(t, [2]uint64{0x600000aaaaaaaaaa, 0xaaaaaaaaaaaaaaaa}, z)

		z = ZValue(types.Point{X: 1, Y: -1})
		assert.Equal(t, [2]uint64{0x655555aaaaaaaaaa, 0xaaaaaaaaaaaaaaaa}, z)

		z = ZValue(types.Point{X: 0, Y: 0})
		assert.Equal(t, [2]uint64{0xc000000000000000, 0x000000000000000}, z)

		z = ZValue(types.Point{X: 1, Y: 0})
		assert.Equal(t, [2]uint64{0xc555550000000000, 0x000000000000000}, z)

		z = ZValue(types.Point{X: 0, Y: 1})
		assert.Equal(t, [2]uint64{0xcaaaaa0000000000, 0x000000000000000}, z)

		z = ZValue(types.Point{X: 1, Y: 1})
		assert.Equal(t, [2]uint64{0xcfffff0000000000, 0x000000000000000}, z)

		z = ZValue(types.Point{X: 2, Y: 2})
		assert.Equal(t, [2]uint64{0xf000000000000000, 0x000000000000000}, z)

		z = ZValue(types.Point{X: 50000, Y: 50000})
		assert.Equal(t, [2]uint64{0xf000fcc03ccc0000, 0x000000000000000}, z)
	})

	t.Run("test un-z-values", func(t *testing.T) {
		z := [2]uint64{0xc000000000000000, 0x000000000000000}
		assert.Equal(t, types.Point{X: 0, Y: 0}, UnZValue(z))
		z = [2]uint64{0xdaaaaa0000000000, 0x000000000000000}
		assert.Equal(t, types.Point{X: 2, Y: 1}, UnZValue(z))
	})

	t.Run("test sorting points by z-value", func(t *testing.T) {
		sortedPoints := []types.Point{
			{X: -2, Y: -2},
			{X: -1, Y: -2},
			{X: -2, Y: -1},
			{X: -1, Y: -1},
			{X: 0, Y: -2},
			{X: 1, Y: -2},
			{X: 2, Y: -2},
			{X: 0, Y: -1},
			{X: 1, Y: -1},
			{X: 2, Y: -1},
			{X: -2, Y: 0},
			{X: -2, Y: 1},
			{X: -1, Y: 0},
			{X: -1, Y: 1},
			{X: -2, Y: 2},
			{X: -1, Y: 2},
			{X: 0, Y: 0},
			{X: 1, Y: 0},
			{X: 0, Y: 1},
			{X: 1, Y: 1},
			{X: 2, Y: 0},
			{X: 2, Y: 1},
			{X: 0, Y: 2},
			{X: 1, Y: 2},
			{X: 2, Y: 2},
		}
		randPoints := append([]types.Point{}, sortedPoints...)
		rand.Shuffle(len(randPoints), func(i, j int) {
			randPoints[i], randPoints[j] = randPoints[j], randPoints[i]
		})
		assert.Equal(t, sortedPoints, ZSort(randPoints))
	})
}

func TestZAddr(t *testing.T) {
	t.Run("test points z-addrs", func(t *testing.T) {
		p := types.Point{X: 1, Y: 2}
		res := ZAddr(p)
		assert.Equal(t, "40e5555500000000000000000000000000", hex.EncodeToString(res[:]))
	})

	t.Run("test linestring z-addrs", func(t *testing.T) {
		a := types.Point{X: 1, Y: 1}
		b := types.Point{X: 2, Y: 2}
		c := types.Point{X: 3, Y: 3}
		l := types.LineString{Points: []types.Point{a, b, c}}
		res := ZAddr(l)
		assert.Equal(t, "3fcfffff00000000000000000000000000", hex.EncodeToString(res[:]))
	})

	t.Run("test polygon z-addrs", func(t *testing.T) {
		a := types.Point{X: -1, Y: 1}
		b := types.Point{X: 1, Y: 1}
		c := types.Point{X: 1, Y: -1}
		d := types.Point{X: -1, Y: -1}
		l := types.LineString{Points: []types.Point{a, b, c, d, a}}
		p := types.Polygon{Lines: []types.LineString{l}}
		res := ZAddr(p)
		assert.Equal(t, "40300000ffffffffffffffffffffffffff", hex.EncodeToString(res[:]))
	})
}

func TestZSort(t *testing.T) {
	p1 := types.LineString{Points: []types.Point{ps[16], ps[19]}}
	p2 := types.LineString{Points: []types.Point{ps[0], ps[3]}}
	p3 := types.LineString{Points: []types.Point{ps[19], ps[24]}}
	p4 := types.LineString{Points: []types.Point{ps[3], ps[19]}}
	p5 := types.LineString{Points: []types.Point{ps[24], ps[24]}}

	t.Run("test z-addr p1", func(t *testing.T) {
		z := ZAddr(p1) // bbox: (0, 0), (1, 1)
		assert.Equal(t, "3ec0000000000000000000000000000000", hex.EncodeToString(z[:]))
	})

	t.Run("test z-addr p2", func(t *testing.T) {
		z := ZAddr(p2) // bbox: (-2, -2), (-1, -1)
		assert.Equal(t, "3f0fffffffffffffffffffffffffffffff", hex.EncodeToString(z[:]))
	})

	t.Run("test z-addr p3", func(t *testing.T) {
		z := ZAddr(p3) // bbox: (1, 1), (2, 2)
		assert.Equal(t, "3fcfffff00000000000000000000000000", hex.EncodeToString(z[:]))
	})

	t.Run("test z-addr p4", func(t *testing.T) {
		z := ZAddr(p4) // bbox: (-1, -1), (1, 1)
		assert.Equal(t, "40300000ffffffffffffffffffffffffff", hex.EncodeToString(z[:]))
	})

	t.Run("test z-addr p6", func(t *testing.T) {
		z := ZAddr(p5) // bbox: (2, 2), (2, 2)
		assert.Equal(t, "40f0000000000000000000000000000000", hex.EncodeToString(z[:]))
	})

	t.Run("test z-addr sorting", func(t *testing.T) {
		sortedGeoms := []types.GeometryValue{p1, p2, p3, p4, p5}
		randomGeoms := append([]types.GeometryValue{}, sortedGeoms...)
		rand.Shuffle(len(randomGeoms), func(i, j int) {
			randomGeoms[i], randomGeoms[j] = randomGeoms[j], randomGeoms[i]
		})
		assert.Equal(t, sortedGeoms, ZAddrSort(randomGeoms))
	})
}
