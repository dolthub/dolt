// Copyright 2020 Dolthub, Inc.
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

package lookup

import "sort"

// rangeSlice is a sortable slice of ranges.
type rangeSlice struct {
	ranges []Range
	err    error
}

func (r *rangeSlice) Len() int      { return len(r.ranges) }
func (r *rangeSlice) Swap(i, j int) { r.ranges[i], r.ranges[j] = r.ranges[j], r.ranges[i] }
func (r *rangeSlice) Less(i, j int) bool {
	lc, err := r.ranges[i].LowerBound.Compare(r.ranges[j].LowerBound)
	if err != nil {
		r.err = err
		return false
	}
	if lc < 0 {
		return true
	} else if lc > 0 {
		return false
	}
	uc, err := r.ranges[i].UpperBound.Compare(r.ranges[j].UpperBound)
	if err != nil {
		r.err = err
		return false
	}
	return uc < 0
}

// SimplifyRanges combines all ranges that are connected and returns a new slice.
func SimplifyRanges(rs []Range) ([]Range, error) {
	if len(rs) == 0 {
		return []Range{EmptyRange()}, nil
	}
	sorted := make([]Range, len(rs))
	copy(sorted, rs)
	rSlice := &rangeSlice{ranges: sorted}
	sort.Sort(rSlice)
	if rSlice.err != nil {
		return nil, rSlice.err
	}
	var res []Range
	cur := EmptyRange()
	for _, r := range sorted {
		merged, ok, err := cur.TryUnion(r)
		if err != nil {
			return nil, err
		}
		if ok {
			cur = merged
		} else if !cur.IsEmpty() {
			res = append(res, cur)
			cur = r
		}
	}
	if !cur.IsEmpty() {
		res = append(res, cur)
	}
	return res, nil
}
