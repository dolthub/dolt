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

// Range defines a contiguous range of Tuples.
type Range struct {
	Start, Stop []RangeCut
	Desc        val.TupleDesc
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

func (r Range) AboveStart(t val.Tuple) bool {
	for i, cut := range r.Start {
		if cut.nonBinding() {
			continue
		}

		v := t.GetField(i)

		if cut.Null || v == nil {
			// null values are returned iff |cut.Null|
			return cut.Null && v == nil
		}

		cmp := r.Desc.CompareField(cut.Value, i, t)

		if cut.Inclusive && cmp == 0 {
			continue
		}

		return cmp < 0
	}
	return true
}

func (r Range) BelowStop(t val.Tuple) bool {
	for i, cut := range r.Stop {
		if cut.nonBinding() {
			return true
		}

		v := t.GetField(i)

		if cut.Null || v == nil {
			// null values are returned iff |cut.Null|
			return cut.Null && v == nil
		}

		cmp := r.Desc.CompareField(cut.Value, i, t)

		if cut.Inclusive && cmp == 0 {
			continue
		}

		return cmp > 0
	}
	return true
}

func rangeStartSearchFn(rng Range) searchFn {
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

func rangeStopSearchFn(rng Range) searchFn {
	return func(nd Node) int {
		// todo(andy): inline sort.Search()
		return sort.Search(int(nd.count), func(i int) (out bool) {
			// if |tup| ∈ |rng|, set |out| to false
			tup := val.Tuple(nd.getKey(i))
			out = !rng.BelowStop(tup)
			return
		})
	}
}

// GreaterRange defines a Range of Tuples greater than |lo|.
func GreaterRange(start val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: exclusiveBound(start, desc),
		Desc:  desc,
	}
}

// GreaterOrEqualRange defines a Range of Tuples greater than or equal to |lo|.
func GreaterOrEqualRange(start val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: inclusiveBound(start, desc),
		Desc:  desc,
	}
}

// LesserRange defines a Range of Tuples less than |last|.
func LesserRange(stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Stop: exclusiveBound(stop, desc),
		Desc: desc,
	}
}

// LesserOrEqualRange defines a Range of Tuples less than or equal to |last|.
func LesserOrEqualRange(stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Stop: inclusiveBound(stop, desc),
		Desc: desc,
	}
}

// OpenRange defines a non-inclusive Range of Tuples from |lo| to |last|.
func OpenRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: exclusiveBound(start, desc),
		Stop:  exclusiveBound(stop, desc),
		Desc:  desc,
	}
}

// OpenStartRange defines a half-open Range of Tuples from |lo| to |last|.
func OpenStartRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: exclusiveBound(start, desc),
		Stop:  inclusiveBound(stop, desc),
		Desc:  desc,
	}
}

// OpenStopRange defines a half-open Range of Tuples from |lo| to |last|.
func OpenStopRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: inclusiveBound(start, desc),
		Stop:  exclusiveBound(stop, desc),
		Desc:  desc,
	}
}

// ClosedRange defines an inclusive Range of Tuples from |lo| to |last|.
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
		op := ">"
		if cut.Inclusive {
			op = ">="
		}
		v := r.Desc.FormatValue(i, cut.Value)
		sb.WriteString(fmt.Sprintf("tuple[%d] %s %s", i, op, v))
	}
	for i, cut := range r.Stop {
		if seenOne {
			sb.WriteString(", ")
		}
		seenOne = true
		op := "<"
		if cut.Inclusive {
			op = "<="
		}
		v := r.Desc.FormatValue(i, cut.Value)
		sb.WriteString(fmt.Sprintf("tuple[%d] %s %s", i, op, v))
	}

	sb.WriteString(" )")
	return sb.String()
}
