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

// PrefixRange constructs a Range for Tuples with a prefix of |prefix|.
func PrefixRange(prefix val.Tuple, desc val.TupleDesc) Range {
	return closedRange(prefix, prefix, desc)
}

// Range defines a subset of a prolly tree Tuple index.
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

		cmp := order.CompareValues(field, bound.Value, typ)
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

// belowStop is used to find the eng of the
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

		cmp := order.CompareValues(field, bound.Value, typ)
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

// matches returns true if all of the filter predicates
// for Range |r| are true for Tuple |t|.
func (r Range) matches(t val.Tuple) bool {
	order := r.Desc.Comparator()
	for i := range r.Fields {
		field := r.Desc.GetField(i, t)
		typ := r.Desc.Types[i]

		if r.Fields[i].Exact {
			v := r.Fields[i].Lo.Value
			if order.CompareValues(field, v, typ) == 0 {
				continue
			}
			return false
		}

		lo := r.Fields[i].Lo
		if lo.Binding {
			cmp := order.CompareValues(field, lo.Value, typ)
			if cmp < 0 || (cmp == 0 && !lo.Inclusive) {
				return false
			}
		}

		hi := r.Fields[i].Hi
		if hi.Binding {
			cmp := order.CompareValues(field, hi.Value, typ)
			if cmp > 0 || (cmp == 0 && !hi.Inclusive) {
				return false
			}
		}
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
			Exact: order.CompareValues(lo, hi, desc.Types[i]) == 0,
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
