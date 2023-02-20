// Copyright 2022 Dolthub, Inc.
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
	"encoding/binary"
	"github.com/dolthub/go-mysql-server/sql/types"
	"math"
	"sort"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// OpenStopRange defines a half-open Range of Tuples [start, stop).
func OpenStopRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return openStopRange(start, stop, desc)
}

// GreaterOrEqualRange defines a Range of Tuples greater than or equal to |start|.
func GreaterOrEqualRange(start val.Tuple, desc val.TupleDesc) Range {
	return greaterOrEqualRange(start, desc)
}

// LesserRange defines a Range of Tuples less than |stop|.
func LesserRange(stop val.Tuple, desc val.TupleDesc) Range {
	return lesserRange(stop, desc)
}

// PrefixRange constructs a Range for Tuples with a prefix of |prefix|.
func PrefixRange(prefix val.Tuple, desc val.TupleDesc) Range {
	return closedRange(prefix, prefix, desc)
}

// Range defines a subset of a prolly Tree Tuple index.
//
// Range can be used either to physically partition an index or
// to logically filter an index.
// A Range's physical partition is a contiguous set of Tuples
// containing every Tuple matching the Range's predicates, but
// possibly containing non-matching Tuples.
// Non-matching Tuples can be filtered from physical partitions
// by using RangeFields as logical predicates (see filteredIter).
type Range struct {
	Fields []RangeField
	Desc   val.TupleDesc
	Tup    val.Tuple
	MinX, MinY, MaxX, MaxY uint64
}

// RangeField bounds one dimension of a Range.
type RangeField struct {
	Lo, Hi Bound
	Exact  bool // Lo.Value == Hi.Value
}

type Bound struct {
	Binding   bool
	Inclusive bool
	Value     []byte
}

// aboveStart is used to find the start of the
// physical partition defined by a Range.
func (r Range) aboveStart(t val.Tuple) bool {
	order := r.Desc.Comparator()
	for i := range r.Fields {
		bound := r.Fields[i].Lo
		if !bound.Binding {
			return true
		}

		field := r.Desc.GetField(i, t)
		typ := r.Desc.Types[i]

		cmp := order.CompareValues(i, field, bound.Value, typ)
		if cmp < 0 {
			// |field| is outside Range
			return false
		}

		if r.Fields[i].Exact && cmp == 0 {
			// for exact bounds (operators '=' and 'IS')
			// we can use subsequent columns to narrow
			// physical index scans.
			// this is not possible for interval bounds.
			continue
		}

		return cmp > 0 || bound.Inclusive
	}
	return true
}

// belowStop is used to find the end of the
// physical partition defined by a Range.
func (r Range) belowStop(t val.Tuple) bool {
	order := r.Desc.Comparator()
	for i := range r.Fields {
		bound := r.Fields[i].Hi
		if !bound.Binding {
			return true
		}

		field := r.Desc.GetField(i, t)
		typ := r.Desc.Types[i]

		cmp := order.CompareValues(i, field, bound.Value, typ)
		if cmp > 0 {
			// |field| is outside Range
			return false
		}

		if r.Fields[i].Exact && cmp == 0 {
			// for exact bounds (operators '=' and 'IS')
			// we can use subsequent columns to narrow
			// physical index scans.
			// this is not possible for interval bounds.
			continue
		}

		return cmp < 0 || bound.Inclusive
	}
	return true
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

// UnZCell converts the val.Cell into a types.Point
// NOTE: this does not completely revert the conversion from types.GeometryValue
func UnZCell(v []byte) types.Point {
	var zVal [2]uint64
	zVal[0] = binary.BigEndian.Uint64(v[1:])
	zVal[1] = binary.BigEndian.Uint64(v[9:])
	return UnZValue(zVal)
}

// PartialUnZValue takes a [2]uint64 Z-Value and converts it back to a sql.Point
func PartialUnZValue(z [2]uint64) [2]uint64 {
	xl, yl := UnInterleaveUint64(z[0])
	xr, yr := UnInterleaveUint64(z[1])
	return [2]uint64{(xl << 32) | xr, (yl << 32) | yr}
}

// PartialUnZCell converts the val.Cell into a types.Point
// NOTE: this does not completely revert the conversion from types.GeometryValue
func PartialUnZCell(v []byte) [2]uint64 {
	var zVal [2]uint64
	zVal[0] = binary.BigEndian.Uint64(v[1:])
	zVal[1] = binary.BigEndian.Uint64(v[9:])
	return PartialUnZValue(zVal)
}

// Matches returns true if all of the filter predicates
// for Range |r| are true for Tuple |t|.
func (r Range) Matches(t val.Tuple) bool {
	order := r.Desc.Comparator()
	for i := range r.Fields {
		field := r.Desc.GetField(i, t)
		typ := r.Desc.Types[i]

		spatialExact := (r.MinX == r.MaxX) && (r.MinY == r.MaxY)
		if r.Fields[i].Exact || spatialExact {
			v := r.Fields[i].Lo.Value
			if order.CompareValues(i, field, v, typ) == 0 {
				continue
			}
			return false
		}

		lo := r.Fields[i].Lo
		if lo.Binding {
			cmp := order.CompareValues(i, field, lo.Value, typ)
			if cmp < 0 || (cmp == 0 && !lo.Inclusive) {
				return false
			}
		}

		hi := r.Fields[i].Hi
		if hi.Binding {
			cmp := order.CompareValues(i, field, hi.Value, typ)
			if cmp > 0 || (cmp == 0 && !hi.Inclusive) {
				return false
			}
		}

		// TODO: cache the bbox somewhere else so we don't have to unzip both everytime
		// TODO: this makes worst case much better, but fast case slightly worse
		if typ.Enc == val.CellEnc {
			point := PartialUnZCell(field)
			if  point[0] < r.MinX ||
				point[0] > r.MaxX ||
				point[1] < r.MinY ||
				point[1] > r.MaxY {
				return false
			}
		}
	}
	return true
}

func (r Range) IsPointLookup(desc val.TupleDesc) bool {
	if len(r.Fields) < len(desc.Types) {
		return false
	}
	for i := range r.Fields {
		if !r.Fields[i].Exact {
			return false
		}
	}
	return true
}

func rangeStartSearchFn(rng Range) tree.SearchFn {
	return func(nd tree.Node) int {
		// todo(andy): inline sort.Search()
		return sort.Search(nd.Count(), func(i int) (in bool) {
			// if |tup| ∈ |rng|, set |in| to true
			tup := val.Tuple(nd.GetKey(i))
			in = rng.aboveStart(tup)
			return
		})
	}
}

func rangeStopSearchFn(rng Range) tree.SearchFn {
	return func(nd tree.Node) (idx int) {
		// todo(andy): inline sort.Search()
		return sort.Search(nd.Count(), func(i int) (out bool) {
			// if |tup| ∈ |rng|, set |out| to false
			tup := val.Tuple(nd.GetKey(i))
			out = !rng.belowStop(tup)
			return
		})
	}
}

// closedRange defines an inclusive Range of Tuples from [start, stop].
func closedRange(start, stop val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = Range{
		Fields: make([]RangeField, len(desc.Types)),
		Desc:   desc,
	}
	order := desc.Comparator()

	for i := range rng.Fields {
		lo := desc.GetField(i, start)
		hi := desc.GetField(i, stop)
		rng.Fields[i] = RangeField{
			Lo:    Bound{Binding: true, Inclusive: true, Value: lo},
			Hi:    Bound{Binding: true, Inclusive: true, Value: hi},
			Exact: order.CompareValues(i, lo, hi, desc.Types[i]) == 0,
		}
	}
	return
}

// OpenStartRange defines a half-open Range of Tuples (start, stop].
func openStartRange(start, stop val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = closedRange(start, stop, desc)
	last := len(rng.Fields) - 1
	rng.Fields[last].Lo.Inclusive = false
	rng.Fields[last].Exact = false
	return rng
}

// OpenStopRange defines a half-open Range of Tuples [start, stop).
func openStopRange(start, stop val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = closedRange(start, stop, desc)
	last := len(rng.Fields) - 1
	rng.Fields[last].Hi.Inclusive = false
	rng.Fields[last].Exact = false
	return
}

// OpenRange defines a non-inclusive Range of Tuples from (start, stop).
func openRange(start, stop val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = closedRange(start, stop, desc)
	last := len(rng.Fields) - 1
	rng.Fields[last].Lo.Inclusive = false
	rng.Fields[last].Hi.Inclusive = false
	rng.Fields[last].Exact = false
	return
}

// GreaterRange defines a Range of Tuples greater than |start|.
func greaterRange(start val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = greaterOrEqualRange(start, desc)
	last := len(rng.Fields) - 1
	rng.Fields[last].Lo.Inclusive = false
	return
}

// GreaterOrEqualRange defines a Range of Tuples greater than or equal to |start|.
func greaterOrEqualRange(start val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = Range{
		Fields: make([]RangeField, len(desc.Types)),
		Desc:   desc,
	}
	for i := range rng.Fields {
		lo := desc.GetField(i, start)
		rng.Fields[i] = RangeField{
			Lo: Bound{Binding: true, Inclusive: true, Value: lo},
		}
	}
	return
}

// LesserRange defines a Range of Tuples less than |stop|.
func lesserRange(stop val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = lesserOrEqualRange(stop, desc)
	last := len(rng.Fields) - 1
	rng.Fields[last].Hi.Inclusive = false
	return
}

// LesserOrEqualRange defines a Range of Tuples less than or equal to |stop|.
func lesserOrEqualRange(stop val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = Range{
		Fields: make([]RangeField, len(desc.Types)),
		Desc:   desc,
	}
	for i := range rng.Fields {
		hi := desc.GetField(i, stop)
		rng.Fields[i] = RangeField{
			Hi: Bound{Binding: true, Inclusive: true, Value: hi},
		}
	}
	return
}
