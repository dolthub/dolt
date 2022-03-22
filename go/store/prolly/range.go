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
	KeyDesc     val.TupleDesc
}

// RangeCut bounds one dimension of a Range.
type RangeCut struct {
	Value     []byte
	Inclusive bool
	Null      bool
}

func (r Range) insideStart(key val.Tuple) bool {
	for i, cut := range r.Start {
		if cut.Value == nil {
			// no bound for this field
			continue
		}

		if cut.Null {
			// value must be null
			if !r.KeyDesc.IsNull(i, key) {
				return false
			}
			continue
		}

		cmp := r.KeyDesc.CompareField(cut.Value, i, key)
		if cmp == 0 {
			return cut.Inclusive
		}
		return cmp < 0
	}
	return true
}

func (r Range) insideStop(key val.Tuple) bool {
	for i, cut := range r.Stop {
		if cut.Value == nil {
			// no bound for this field
			continue
		}

		if cut.Null {
			// value must be null
			if !r.KeyDesc.IsNull(i, key) {
				return false
			}
			continue
		}

		cmp := r.KeyDesc.CompareField(cut.Value, i, key)
		if cmp == 0 {
			return cut.Inclusive
		}
		return cmp > 0
	}
	return true
}

func (r Range) format() string {
	return formatRange(r)
}

// todo(andy): inline sort.Search()
// todo(andy): comment doc
func rangeStartSearchFn(rng Range) searchFn {
	return func(nd Node) int {
		return sort.Search(int(nd.count), func(i int) bool {
			tup := val.Tuple(nd.getKey(i))
			for i, cut := range rng.Start {
				cmp := rng.KeyDesc.CompareField(cut.Value, i, tup)
				if cut.Inclusive {
					return cmp <= 0
				} else {
					return cmp < 0
				}
			}
			return true
		})
	}
}

// todo(andy): inline sort.Search()
// todo(andy): comment doc
func rangeStopSearchFn(rng Range) searchFn {
	return func(nd Node) int {
		return sort.Search(int(nd.count), func(i int) bool {
			tup := val.Tuple(nd.getKey(i))
			for i, cut := range rng.Stop {
				cmp := rng.KeyDesc.CompareField(cut.Value, i, tup)
				if cut.Inclusive {
					return cmp < 0
				} else {
					return cmp <= 0
				}
			}
			return false
		})
	}
}

// GreaterRange defines a Range of Tuples greater than |lo|.
func GreaterRange(start val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start:   openRangeCuts(start, desc),
		KeyDesc: desc,
	}
}

// GreaterOrEqualRange defines a Range of Tuples greater than or equal to |lo|.
func GreaterOrEqualRange(start val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start:   closeRangeCuts(start, desc),
		KeyDesc: desc,
	}
}

// LesserRange defines a Range of Tuples less than |last|.
func LesserRange(stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Stop:    openRangeCuts(stop, desc),
		KeyDesc: desc,
	}
}

// LesserOrEqualRange defines a Range of Tuples less than or equal to |last|.
func LesserOrEqualRange(stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Stop:    closeRangeCuts(stop, desc),
		KeyDesc: desc,
	}
}

// OpenRange defines a non-inclusive Range of Tuples from |lo| to |last|.
func OpenRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start:   openRangeCuts(start, desc),
		Stop:    openRangeCuts(stop, desc),
		KeyDesc: desc,
	}
}

// OpenStartRange defines a half-open Range of Tuples from |lo| to |last|.
func OpenStartRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start:   openRangeCuts(start, desc),
		Stop:    closeRangeCuts(stop, desc),
		KeyDesc: desc,
	}
}

// OpenStopRange defines a half-open Range of Tuples from |lo| to |last|.
func OpenStopRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start:   closeRangeCuts(start, desc),
		Stop:    openRangeCuts(stop, desc),
		KeyDesc: desc,
	}
}

// ClosedRange defines an inclusive Range of Tuples from |lo| to |last|.
func ClosedRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start:   closeRangeCuts(start, desc),
		Stop:    closeRangeCuts(stop, desc),
		KeyDesc: desc,
	}
}

func openRangeCuts(tup val.Tuple, desc val.TupleDesc) (cut []RangeCut) {
	cut = make([]RangeCut, len(desc.Types))
	for i := range cut {
		cut[i] = RangeCut{
			Value:     tup.GetField(i),
			Inclusive: false,
		}
	}
	return
}

func closeRangeCuts(tup val.Tuple, desc val.TupleDesc) (cut []RangeCut) {
	cut = make([]RangeCut, len(desc.Types))
	for i := range cut {
		cut[i] = RangeCut{
			Value:     tup.GetField(i),
			Inclusive: true,
		}
	}
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
		v := r.KeyDesc.FormatValue(i, cut.Value)
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
		v := r.KeyDesc.FormatValue(i, cut.Value)
		sb.WriteString(fmt.Sprintf("tuple[%d] %s %s", i, op, v))
	}

	sb.WriteString(" )")
	return sb.String()
}
