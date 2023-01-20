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
		assert.Equal(t, "0fff30f03f3fffffffffffffffffffff", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: -1, Y: -1})
		assert.Equal(t, "300000ffffffffffffffffffffffffff", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: -1, Y: 0})
		assert.Equal(t, "600000aaaaaaaaaaaaaaaaaaaaaaaaaa", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: -1, Y: 1})
		assert.Equal(t, "655555aaaaaaaaaaaaaaaaaaaaaaaaaa", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: 0, Y: -1})
		assert.Equal(t, "90000055555555555555555555555555", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: 1, Y: -1})
		assert.Equal(t, "9aaaaa55555555555555555555555555", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: 0, Y: 0})
		assert.Equal(t, "c0000000000000000000000000000000", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: 1, Y: 0})
		assert.Equal(t, "caaaaa00000000000000000000000000", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: 0, Y: 1})
		assert.Equal(t, "c5555500000000000000000000000000", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: 1, Y: 1})
		assert.Equal(t, "cfffff00000000000000000000000000", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: 2, Y: 2})
		assert.Equal(t, "f0000000000000000000000000000000", hex.EncodeToString(z[:]))

		z = ZValue(types.Point{X: 50000, Y: 50000})
		assert.Equal(t, "f000fcc03ccc00000000000000000000", hex.EncodeToString(z[:]))
	})

	t.Run("test un-z-values", func(t *testing.T) {
		v, _ := hex.DecodeString("c0000000000000000000000000000000")
		z := [16]byte{}
		for i, v := range v {
			z[i] = v
		}
		assert.Equal(t, types.Point{X: 0, Y: 0}, UnZValue(z))

		v, _ = hex.DecodeString("daaaaa00000000000000000000000000")
		z = [16]byte{}
		for i, v := range v {
			z[i] = v
		}
		assert.Equal(t, types.Point{X: 1, Y: 2}, UnZValue(z))
	})

	t.Run("test sorting points by z-value", func(t *testing.T) {
		sortedPoints := []types.Point{
			{X: -5000, Y: -5000},
			{X: -1, Y: -1},
			{X: -1, Y: 0},
			{X: -1, Y: 1},
			{X: 1, Y: -1},
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
		assert.Equal(t, sortedPoints, ZSort(randPoints))
	})
}
