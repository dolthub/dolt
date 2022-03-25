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
	"fmt"
	"sort"
	"strings"

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

// Range defines a contiguous range of Tuples starting from the
// lexicographically least Tuple that satisfies all RangeCut
// predicates, and ending at the greatest Tuple that satisfies
// all predicates. Tuples inside the Range need not satisfy
// all predicates, as long as they are in bounds.
type Range struct {
	Start, Stop []RangeCut
	Desc        val.TupleDesc
}

// AboveStart returns true if |t| is a member of |r|.
func (r Range) AboveStart(t val.Tuple) bool {
	if len(r.Start) == 0 {
		return true
	}

	cut := r.Start[0]
	if cut.nonBinding() {
		return true
	}

	v := t.GetField(0)
	if cut.Null || v == nil {
		// null values are returned iff |cut.Null|
		return cut.Null && v == nil
	}

	cmp := r.Desc.CompareField(cut.Value, 0, t)
	return cmp < 0 || (cut.Inclusive && cmp == 0)
}

// BelowStop returns true if |t| is a member of |r|.
func (r Range) BelowStop(t val.Tuple) bool {
	if len(r.Stop) == 0 {
		return true
	}

	cut := r.Stop[0]
	if cut.nonBinding() {
		return true
	}

	v := t.GetField(0)
	if cut.Null || v == nil {
		// null values are returned iff |cut.Null|
		return cut.Null && v == nil
	}

	cmp := r.Desc.CompareField(cut.Value, 0, t)
	return cmp > 0 || (cut.Inclusive && cmp == 0)
}

func (r Range) less(other Range) bool {
	assertTrue(len(r.Start) == len(other.Start))
	if len(r.Start) == 0 {
		return false
	}

	left, right := r.Start[0], other.Start[0]
	if left.nonBinding() || right.nonBinding() {
		// order unbound ranges first
		return left.nonBinding() && right.binding()
	}

	compare := r.Desc.Comparator()
	typ := r.Desc.Types[0]
	return left.lesserValue(right, typ, compare)
}

func (r Range) overlaps(other Range) bool {
	compare := r.Desc.Comparator()
	typ := r.Desc.Types[0]

	if r.Stop[0].binding() && other.Start[0].binding() {
		if r.Stop[0].lesserValue(other.Start[0], typ, compare) {
			return false
		}
	}
	if other.Stop[0].binding() && r.Start[0].binding() {
		if other.Stop[0].lesserValue(r.Start[0], typ, compare) {
			return false
		}
	}
	return true
}

func (r Range) merge(other Range) Range {
	assertTrue(r.Desc.Equals(other.Desc))
	assertTrue(len(r.Start) == len(other.Start))
	assertTrue(len(r.Stop) == len(other.Stop))

	types := r.Desc.Types
	compare := r.Desc.Comparator()

	// take the min of each RangeCut pair
	lower := make([]RangeCut, len(r.Start))
	for i := range lower {
		left, right := r.Start[i], other.Start[i]

		if left.nonBinding() || right.nonBinding() {
			lower[i] = RangeCut{Value: nil}
			continue
		}

		lower[i] = left
		if right.lesserValue(left, types[i], compare) {
			lower[i] = right
		}
	}

	// take the max of each RangeCut pair
	upper := make([]RangeCut, len(r.Stop))
	for i := range upper {
		left, right := r.Stop[i], other.Stop[i]

		if left.nonBinding() || right.nonBinding() {
			upper[i] = RangeCut{Value: nil}
			continue
		}

		upper[i] = right
		if right.lesserValue(left, types[i], compare) {
			upper[i] = left
		}
	}

	return Range{
		Start: lower,
		Stop:  upper,
		Desc:  other.Desc,
	}
}

func (r Range) format() string {
	return formatRange(r)
}

// RangeCut bounds one dimension of a Range.
type RangeCut struct {
	Value     []byte
	Inclusive bool
	Null      bool
}

func (c RangeCut) nonBinding() bool {
	return c.Value == nil && c.Null == false
}

func (c RangeCut) binding() bool {
	return c.Value != nil
}

func (c RangeCut) lesserValue(other RangeCut, typ val.Type, tc val.TupleComparator) bool {
	if c.Null || other.Null {
		// order nulls last
		return !c.Null && other.Null
	}

	cmp := tc.CompareValues(c.Value, other.Value, typ)
	if cmp == 0 {
		return c.Inclusive && !other.Inclusive
	}
	return cmp < 0
}

func rangeStartSearchFn(rng Range) searchFn {
	return linearSearchRangeStart(rng)
}

func rangeStopSearchFn(rng Range) searchFn {
	return linearSearchRangeStop(rng)
}

func linearSearchRangeStart(rng Range) searchFn {
	return func(nd Node) (idx int) {
		for idx = 0; idx < int(nd.count); idx++ {
			tup := val.Tuple(nd.getKey(idx))
			if rng.AboveStart(tup) {
				break
			}
		}
		return idx
	}
}

func linearSearchRangeStop(rng Range) searchFn {
	return func(nd Node) (idx int) {
		for idx = int(nd.count - 1); idx >= 0; idx-- {
			tup := val.Tuple(nd.getKey(idx))
			if rng.BelowStop(tup) {
				break
			}
		}
		return idx + 1
	}
}

func binarySearchRangeStart(rng Range) searchFn {
	// todo(andy): this search is broken, it fails to
	//  maintain the propertry f(i) == true implies f(i+1) == true.
	return func(nd Node) int {
		// todo(andy): inline sort.Search()
		return sort.Search(int(nd.count), func(i int) (in bool) {
			// if |tup| ∈ |rng|, set |in| to true
			tup := val.Tuple(nd.getKey(i))
			in = rng.AboveStart(tup)
			return
		})
	}
}

func binarySearchRangeStop(rng Range) searchFn {
	// todo(andy): this search is broken, it fails to
	//  maintain the propertry f(i) == true implies f(i+1) == true.
	return func(nd Node) (idx int) {
		// todo(andy): inline sort.Search()
		return sort.Search(int(nd.count), func(i int) (out bool) {
			// if |tup| ∈ |rng|, set |out| to false
			tup := val.Tuple(nd.getKey(i))
			out = !rng.BelowStop(tup)
			return
		})
	}
}

// GreaterRange defines a Range of Tuples greater than |start|.
func GreaterRange(start val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: exclusiveBound(start, desc),
		Desc:  desc,
	}
}

// GreaterOrEqualRange defines a Range of Tuples greater than or equal to |start|.
func GreaterOrEqualRange(start val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: inclusiveBound(start, desc),
		Desc:  desc,
	}
}

// LesserRange defines a Range of Tuples less than |stop|.
func LesserRange(stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Stop: exclusiveBound(stop, desc),
		Desc: desc,
	}
}

// LesserOrEqualRange defines a Range of Tuples less than or equal to |stop|.
func LesserOrEqualRange(stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Stop: inclusiveBound(stop, desc),
		Desc: desc,
	}
}

// OpenRange defines a non-inclusive Range of Tuples from |start| to |stop|.
func OpenRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: exclusiveBound(start, desc),
		Stop:  exclusiveBound(stop, desc),
		Desc:  desc,
	}
}

// OpenStartRange defines a half-open Range of Tuples from |start| to |stop|.
func OpenStartRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: exclusiveBound(start, desc),
		Stop:  inclusiveBound(stop, desc),
		Desc:  desc,
	}
}

// OpenStopRange defines a half-open Range of Tuples from |start| to |stop|.
func OpenStopRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: inclusiveBound(start, desc),
		Stop:  exclusiveBound(stop, desc),
		Desc:  desc,
	}
}

// ClosedRange defines an inclusive Range of Tuples from |start| to |stop|.
func ClosedRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: inclusiveBound(start, desc),
		Stop:  inclusiveBound(stop, desc),
		Desc:  desc,
	}
}

func inclusiveBound(tup val.Tuple, desc val.TupleDesc) (cut []RangeCut) {
	cut = make([]RangeCut, len(desc.Types))
	for i := range cut {
		cut[i] = RangeCut{
			Value:     tup.GetField(i),
			Inclusive: true,
		}
	}
	return
}

func exclusiveBound(tup val.Tuple, desc val.TupleDesc) (cut []RangeCut) {
	cut = inclusiveBound(tup, desc)
	cut[len(cut)-1].Inclusive = false
	return
}

func formatRange(r Range) string {
	var sb strings.Builder
	sb.WriteString("( ")

	seenOne := false
	for i, cut := range r.Start {
		if seenOne {
			sb.WriteString(", ")
		}
		seenOne = true

		v := "-∞"
		if cut.Value != nil {
			v = r.Desc.FormatValue(i, cut.Value)
		}

		var op string
		switch {
		case cut.Null:
			op, v = "==", "NULL"
		case cut.Inclusive:
			op = ">="
		default:
			op = ">"
		}
		sb.WriteString(fmt.Sprintf("tuple[%d] %s %s", i, op, v))
	}
	for i, cut := range r.Stop {
		if seenOne {
			sb.WriteString(", ")
		}
		seenOne = true

		v := "∞"
		if cut.Value != nil {
			v = r.Desc.FormatValue(i, cut.Value)
		}

		var op string
		switch {
		case cut.Null:
			op, v = "==", "NULL"
		case cut.Inclusive:
			op = "<="
		default:
			op = "<"
		}
		sb.WriteString(fmt.Sprintf("tuple[%d] %s %s", i, op, v))
	}

	sb.WriteString(" )")
	return sb.String()
}
