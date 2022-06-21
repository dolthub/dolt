// Copyright 2021 Dolthub, Inc.
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
	"sort"

	"github.com/dolthub/dolt/go/store/prolly/tree"

	"github.com/dolthub/dolt/go/store/val"
)

// MergeOverlappingRanges merges overlapping ranges.
func MergeOverlappingRanges(ranges ...Range) (merged []Range) {
	if len(ranges) <= 1 {
		return ranges
	}
	ranges = SortRanges(ranges...)

	merged = make([]Range, 0, len(ranges))
	acc := ranges[0]

	for _, rng := range ranges[1:] {
		if acc.overlaps(rng) {
			acc = acc.merge(rng)
		} else {
			merged = append(merged, acc)
			acc = rng
		}
	}
	merged = append(merged, acc)
	return
}

// SortRanges sorts ranges by start bound.
func SortRanges(ranges ...Range) []Range {
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].less(ranges[j])
	})
	return ranges
}

type Bound struct {
	Value     []byte
	Inclusive bool
}

func (b Bound) binding() bool {
	return b.Value != nil
}

// RangeField bounds one dimension of a Range.
type RangeField struct {
	Lo, Hi Bound
	Exact  bool // Lo.Value == Hi.Value
	IsNull bool
}

// Range defines a contiguous set of Tuples bounded by
// RangeField predicates.
// A Range over an index must include all Tuples that
// satisfy all predicates, but might also include Tuples
// that fail to satisfy some predicates.
type Range struct {
	Fields []RangeField
	Desc   val.TupleDesc
}

func (r Range) matches(t val.Tuple) bool {
	order := r.Desc.Comparator()
	for i := range r.Fields {
		field := r.Desc.GetField(i, t)
		typ := r.Desc.Types[i]

		if r.Fields[i].IsNull {
			if field == nil {
				continue
			}
			return false
		}

		if r.Fields[i].Exact {
			v := r.Fields[i].Lo.Value
			if order.CompareValues(field, v, typ) == 0 {
				continue
			}
			return false
		}

		lo := r.Fields[i].Lo
		if lo.binding() {
			cmp := order.CompareValues(field, lo.Value, typ)
			if cmp < 0 || (cmp == 0 && !lo.Inclusive) {
				return false
			}
		}

		hi := r.Fields[i].Lo
		if hi.binding() {
			cmp := order.CompareValues(field, hi.Value, typ)
			if cmp > 0 || (cmp == 0 && !lo.Inclusive) {
				return false
			}
		}
	}
	return true
}

func (r Range) aboveStart(t val.Tuple) bool {
	order := r.Desc.Comparator()
	for i := range r.Fields {
		field := r.Desc.GetField(i, t)
		typ := r.Desc.Types[i]

		if r.Fields[i].IsNull {
			if field == nil {
				// if |field| is equal to an exact
				// bound, inspect the next field
				continue
			}
			return false
		}

		bound := r.Fields[i].Lo
		if !bound.binding() {
			return true
		}

		cmp := order.CompareValues(field, bound.Value, typ)
		if r.Fields[i].Exact {
			if cmp == 0 {
				// if |field| is equal to an exact
				// bound, inspect the next field
				continue
			}
			return false
		}

		return cmp > 0 || (bound.Inclusive && cmp == 0)
	}
	return true
}

// belowStop returns true if |t| is a member of |r|.
func (r Range) belowStop(t val.Tuple) bool {
	order := r.Desc.Comparator()
	for i := range r.Fields {
		field := r.Desc.GetField(i, t)
		typ := r.Desc.Types[i]

		if r.Fields[i].IsNull {
			if field == nil {
				// if |field| is equal to an exact
				// bound, inspect the next field
				continue
			}
			return false
		}

		bound := r.Fields[i].Hi
		if !bound.binding() {
			return true
		}

		cmp := order.CompareValues(field, bound.Value, typ)
		if r.Fields[i].Exact {
			if cmp == 0 {
				// if |field| is equal to an exact
				// bound, inspect the next field
				continue
			}
			return false
		}

		return cmp < 0 || (bound.Inclusive && cmp == 0)
	}
	return true
}

func (r Range) less(other Range) bool {
	assertTrue(len(r.Fields) == len(other.Fields))
	if len(r.Fields) == 0 {
		return false
	}

	order := r.Desc.Comparator()
	for i := range r.Fields {
		left := r.Fields[i]
		right := other.Fields[i]
		typ := r.Desc.Types[i]

		// order NULL Ranges last
		if left.IsNull || right.IsNull {
			return !left.IsNull && right.IsNull
		}

		if lesserBound(left.Lo, right.Lo, typ, order) {
			return true
		}
	}
	return false
}

func (r Range) overlaps(other Range) bool {
	assertTrue(len(r.Fields) == len(other.Fields))
	if len(r.Fields) == 0 {
		return false
	}

	order := r.Desc.Comparator()
	for i := range r.Fields {
		left := r.Fields[i]
		right := other.Fields[i]
		typ := r.Desc.Types[i]

		if left.IsNull || right.IsNull {
			if left.IsNull && right.IsNull {
				continue
			}
			return false
		}

		if left.Hi.binding() && right.Lo.binding() {
			if lesserBound(left.Hi, right.Lo, typ, order) {
				return false
			}
		}
		if right.Hi.binding() && left.Lo.binding() {
			if lesserBound(right.Hi, left.Lo, typ, order) {
				return false
			}
		}
	}
	return true
}

func (r Range) merge(other Range) Range {
	assertTrue(r.Desc.Equals(other.Desc))
	assertTrue(len(r.Fields) == len(other.Fields))

	order := r.Desc.Comparator()

	fields := make([]RangeField, len(r.Fields))
	for i := range fields {
		left := r.Fields[i]
		right := other.Fields[i]
		typ := r.Desc.Types[i]

		lo := Bound{Value: nil}
		if left.Lo.binding() && right.Lo.binding() {
			if lesserBound(left.Lo, right.Lo, typ, order) {
				lo = left.Lo
			} else {
				lo = right.Lo
			}
		}

		hi := Bound{Value: nil}
		if left.Hi.binding() && right.Hi.binding() {
			if lesserBound(left.Hi, right.Hi, typ, order) {
				hi = right.Hi
			} else {
				hi = left.Hi
			}
		}

		fields[i] = RangeField{
			Lo:     lo,
			Hi:     hi,
			Exact:  left.Exact && right.Exact,
			IsNull: left.IsNull && right.IsNull,
		}
	}

	return Range{
		Fields: fields,
		Desc:   r.Desc,
	}
}

func (r Range) isPointLookup(desc val.TupleDesc) bool {
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

func lesserBound(left, right Bound, typ val.Type, order val.TupleComparator) bool {
	cmp := order.CompareValues(left.Value, right.Value, typ)
	if cmp == 0 {
		return left.Inclusive && !right.Inclusive
	}
	return cmp < 0
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

func pointLookupSearchFn(rng Range) tree.SearchFn {
	return rangeStartSearchFn(rng)
}

//// GreaterRange defines a Range of Tuples greater than |start|.
//func GreaterRange(start val.Tuple, desc val.TupleDesc) Range {
//	return Range{
//		Start: exclusiveBound(start, desc),
//		Desc:  desc,
//	}
//}
//
//// GreaterOrEqualRange defines a Range of Tuples greater than or equal to |start|.
//func GreaterOrEqualRange(start val.Tuple, desc val.TupleDesc) Range {
//	return Range{
//		Start: inclusiveBound(start, desc),
//		Desc:  desc,
//	}
//}
//
//// LesserRange defines a Range of Tuples less than |stop|.
//func LesserRange(stop val.Tuple, desc val.TupleDesc) Range {
//	return Range{
//		Stop: exclusiveBound(stop, desc),
//		Desc: desc,
//	}
//}
//
//// LesserOrEqualRange defines a Range of Tuples less than or equal to |stop|.
//func LesserOrEqualRange(stop val.Tuple, desc val.TupleDesc) Range {
//	return Range{
//		Stop: inclusiveBound(stop, desc),
//		Desc: desc,
//	}
//}
//
//// OpenRange defines a non-inclusive Range of Tuples from |start| to |stop|.
//func OpenRange(start, stop val.Tuple, desc val.TupleDesc) Range {
//	return Range{
//		Start: exclusiveBound(start, desc),
//		Stop:  exclusiveBound(stop, desc),
//		Desc:  desc,
//	}
//}
//
//// OpenStartRange defines a half-open Range of Tuples from |start| to |stop|.
//func OpenStartRange(start, stop val.Tuple, desc val.TupleDesc) Range {
//	return Range{
//		Start: exclusiveBound(start, desc),
//		Stop:  inclusiveBound(stop, desc),
//		Desc:  desc,
//	}
//}
//
//// OpenStopRange defines a half-open Range of Tuples from |start| to |stop|.
//func OpenStopRange(start, stop val.Tuple, desc val.TupleDesc) Range {
//	return Range{
//		Start: inclusiveBound(start, desc),
//		Stop:  exclusiveBound(stop, desc),
//		Desc:  desc,
//	}
//}

// ClosedRange defines an inclusive Range of Tuples from |start| to |stop|.
func ClosedRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	//return Range{
	//	Start: inclusiveBound(start, desc),
	//	Stop:  inclusiveBound(stop, desc),
	//	Desc:  desc,
	//}
	panic("unimplemented")
}

//func inclusiveBound(tup val.Tuple, desc val.TupleDesc) (cut []RangeField) {
//	cut = make([]RangeField, len(desc.Types))
//	for i := range cut {
//		cut[i] = RangeField{
//			Value:     tup.GetField(i),
//			Inclusive: true,
//		}
//	}
//	return
//}
//
//func exclusiveBound(tup val.Tuple, desc val.TupleDesc) (cut []RangeField) {
//	cut = inclusiveBound(tup, desc)
//	cut[len(cut)-1].Inclusive = false
//	return
//}

func assertTrue(b bool) {
	if !b {
		panic("assertion failed")
	}
}
