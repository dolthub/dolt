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

// Range defines a contiguous set of Tuples bounded by
// RangeField predicates.
// A Range over an index must include all Tuples that
// satisfy all predicates, but might also include Tuples
// that fail to satisfy some predicates.
type Range struct {
	Fields []RangeField
	Desc   val.TupleDesc
}

// RangeField bounds one dimension of a Range.
type RangeField struct {
	Lo, Hi Bound
	Exact  bool // Lo.Value == Hi.Value
	IsNull bool
}

type Bound struct {
	Value     []byte
	Inclusive bool
}

func (b Bound) Binding() bool {
	return b.Value != nil
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
		if lo.Binding() {
			cmp := order.CompareValues(field, lo.Value, typ)
			if cmp < 0 || (cmp == 0 && !lo.Inclusive) {
				return false
			}
		}

		hi := r.Fields[i].Hi
		if hi.Binding() {
			cmp := order.CompareValues(field, hi.Value, typ)
			if cmp > 0 || (cmp == 0 && !hi.Inclusive) {
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
				continue
			}
			return false
		}

		bound := r.Fields[i].Lo
		if !bound.Binding() {
			return true
		}

		cmp := order.CompareValues(field, bound.Value, typ)
		if cmp < 0 {
			// |field| is outside Range
			return false
		}

		if r.Fields[i].Exact && cmp == 0 {
			// todo(andy): document exact
			// predicate subtleties
			continue
		}

		return cmp > 0 || bound.Inclusive
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
				continue
			}
			return false
		}

		bound := r.Fields[i].Hi
		if !bound.Binding() {
			return true
		}

		cmp := order.CompareValues(field, bound.Value, typ)
		if cmp > 0 {
			// |field| is outside Range
			return false
		}

		if r.Fields[i].Exact && cmp == 0 {
			// todo(andy): document exact
			// predicate subtleties
			continue
		}

		return cmp < 0 || bound.Inclusive
	}
	return true
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
