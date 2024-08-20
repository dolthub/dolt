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
	"math"
	"sort"

	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/store/pool"
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
	// SkipRangeMatchCallback is false if any type in the index range
	// expression can return a false positive match. Strings, datetimes,
	// floats, and decimals ranges can prefix match invalid values.
	SkipRangeMatchCallback bool
	// IsContiguous indicates whether this range expression is a
	// single contiguous set of keys on disk. Permit a sequence of
	// (1) zero or more equality restrictions, (2) zero or one
	// non-equality, and (3) no further restrictions.
	IsContiguous bool
}

// RangeField bounds one dimension of a Range.
type RangeField struct {
	Lo, Hi Bound
	// BoundsAreEqual is |true| when |Lo.Value| == |Hi.Value|
	BoundsAreEqual bool
	// TargetIsUnique is |true| when the associated index is unique
	TargetIsUnique bool
}

type Bound struct {
	Binding   bool // positive or negative infinity
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

		if r.Fields[i].BoundsAreEqual && cmp == 0 {
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

		if r.Fields[i].BoundsAreEqual && cmp == 0 {
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

// Matches returns true if all the filter predicates
// for Range |r| are true for Tuple |t|.
func (r Range) Matches(t val.Tuple) bool {
	order := r.Desc.Comparator()
	for i := range r.Fields {
		field := r.Desc.GetField(i, t)
		typ := r.Desc.Types[i]

		if r.Fields[i].BoundsAreEqual {
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
	}
	return true
}

func (r Range) IsStrictKeyLookup(desc val.TupleDesc) bool {
	// a strict key is a set of non-nil equality restrictions covering every field of a unique index
	if len(r.Fields) < len(desc.Types) {
		return false
	}
	for i := range r.Fields {
		if !r.Fields[i].BoundsAreEqual {
			return false
		}
	}
	return true
}

// KeyRangeLookup will return a stop key and true if the range can be scanned
// from a start to stop tuple. Otherwise, return a nil key and false. A range
// can be key range scanned if the prefix is exact, and the final field is
// numeric or string. The stop key adds +1 to a numeric final field, and appends
// '0' to a string final field.
// TODO: support non-exact final field, and use range upper bound?
func (r Range) KeyRangeLookup(pool pool.BuffPool) (val.Tuple, bool) {
	if r.Tup == nil {
		return nil, false
	}
	n := len(r.Fields) - 1
	for i := range r.Fields {
		if r.Fields[i].Lo.Value == nil {
			if r.Fields[i].Hi.Value != nil {
				return nil, false
			}
			n = i - 1
			break
		}
		if !r.Fields[i].BoundsAreEqual {
			return nil, false
		}
	}

	if n < 0 {
		// ex: range scan
		return nil, false
	}

	for _, typ := range r.Desc.Types[n+1:] {
		if !typ.Nullable {
			// this is checked separately because fulltext descriptors
			// do not match field lengths sometimes
			// todo: why?
			return nil, false
		}
	}

	for i := n + 1; i < len(r.Fields); i++ {
		if r.Fields[i].Lo.Value != nil ||
			r.Fields[i].Hi.Value != nil {
			// these shouldn't be possible with regular index semantics,
			// but we manually inline indexes sometimes on the Dolt side,
			// and it's possible we'd change analyzer indexing semantics
			// in the future
			return nil, false
		}

	}

	return IncrementTuple(r.Tup, n, r.Desc, pool)
}

func IncrementTuple(start val.Tuple, n int, desc val.TupleDesc, pool pool.BuffPool) (val.Tuple, bool) {
	tb := val.NewTupleBuilder(desc)
	for i := 0; i < n; i++ {
		if i != n {
			// direct copy all but the last field
			tb.PutRaw(i, start.GetField(i))
		}
	}

	// last field will be incremented by one to get the exclusive key
	// range [key, key+1)
	switch desc.Types[n].Enc {
	case val.StringEnc:
		v, ok := desc.GetString(n, start)
		if !ok {
			return nil, false
		}
		tb.PutString(n, string(v)+"0")
	case val.Int8Enc:
		v, ok := desc.GetInt8(n, start)
		if !ok {
			return nil, false
		}
		tb.PutInt8(n, v+1)
	case val.Uint8Enc:
		v, ok := desc.GetUint8(n, start)
		if !ok {
			return nil, false
		}
		tb.PutUint8(n, v+1)
	case val.Int16Enc:
		v, ok := desc.GetInt16(n, start)
		if !ok {
			return nil, false
		}
		tb.PutInt16(n, v+1)
	case val.Uint16Enc:
		v, ok := desc.GetUint16(n, start)
		if !ok {
			return nil, false
		}
		tb.PutUint16(n, v+1)
	case val.Int32Enc:
		v, ok := desc.GetInt32(n, start)
		if !ok {
			return nil, false
		}
		tb.PutInt32(n, v+1)
	case val.Uint32Enc:
		v, ok := desc.GetUint32(n, start)
		if !ok {
			return nil, false
		}
		tb.PutUint32(n, v+1)
	case val.Int64Enc:
		v, ok := desc.GetInt64(n, start)
		if !ok {
			return nil, false
		}
		tb.PutInt64(n, v+1)
	case val.Uint64Enc:
		v, ok := desc.GetUint64(n, start)
		if !ok {
			return nil, false
		}
		tb.PutUint64(n, v+1)
	case val.Float32Enc:
		v, ok := desc.GetFloat32(n, start)
		if !ok {
			return nil, false
		}
		// increment the finest precision we can represent on disk (little endian)
		tb.PutFloat32(n, math.Float32frombits(math.Float32bits(v)+1))
	case val.Float64Enc:
		v, ok := desc.GetFloat64(n, start)
		if !ok {
			return nil, false
		}
		// increment the finest precision we can represent on disk (little endian)
		tb.PutFloat64(n, math.Float64frombits(math.Float64bits(v)+1))
	case val.DecimalEnc:
		v, ok := desc.GetDecimal(n, start)
		if !ok {
			return nil, false
		}
		tb.PutDecimal(n, v.Add(decimal.New(1, 0)))
	default:
		return nil, false
	}
	stop := tb.Build(pool)
	if desc.Compare(start, stop) >= 0 {
		// If cmp == 0, we lost precision serializing.
		// If cmp > 0, we overflowed and |stop| < |start|.
		// |stop| has to be strictly greater than |start|
		// for this optimization to be valid.
		return nil, false
	}
	return stop, true
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
		isEq := order.CompareValues(i, lo, hi, desc.Types[i]) == 0
		rng.Fields[i] = RangeField{
			Lo:             Bound{Binding: true, Inclusive: true, Value: lo},
			Hi:             Bound{Binding: true, Inclusive: true, Value: hi},
			BoundsAreEqual: isEq,
		}
	}
	return
}

// OpenStartRange defines a half-open Range of Tuples (start, stop].
func openStartRange(start, stop val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = closedRange(start, stop, desc)
	last := len(rng.Fields) - 1
	rng.Fields[last].Lo.Inclusive = false
	rng.Fields[last].BoundsAreEqual = false
	return rng
}

// OpenStopRange defines a half-open Range of Tuples [start, stop).
func openStopRange(start, stop val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = closedRange(start, stop, desc)
	last := len(rng.Fields) - 1
	rng.Fields[last].Hi.Inclusive = false
	rng.Fields[last].BoundsAreEqual = false
	return
}

// OpenRange defines a non-inclusive Range of Tuples from (start, stop).
func openRange(start, stop val.Tuple, desc val.TupleDesc) (rng Range) {
	rng = closedRange(start, stop, desc)
	last := len(rng.Fields) - 1
	rng.Fields[last].Lo.Inclusive = false
	rng.Fields[last].Hi.Inclusive = false
	rng.Fields[last].BoundsAreEqual = false
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
