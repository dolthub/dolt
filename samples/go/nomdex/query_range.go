// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/attic-labs/noms/go/types"
)

type bound struct {
	value    types.Value
	include  bool
	infinity int8
}

func (b bound) isLessThanOrEqual(o bound) (res bool) {
	return b.equals(o) || b.isLessThan(o)
}

func (b bound) isLessThan(o bound) (res bool) {
	if b.infinity < o.infinity {
		return true
	}

	if b.infinity > o.infinity {
		return false
	}

	if b.infinity == o.infinity && b.infinity != 0 {
		return false
	}

	if b.value.Less(o.value) {
		return true
	}

	if b.value.Equals(o.value) {
		if !b.include && o.include {
			return true
		}
	}
	return false
}

func (b bound) isGreaterThanOrEqual(o bound) (res bool) {
	return !b.isLessThan(o)
}

func (b bound) isGreaterThan(o bound) (res bool) {
	return !b.equals(o) || !b.isLessThan(o)
}

func (b bound) equals(o bound) bool {
	return b.infinity == o.infinity && b.include == o.include &&
		(b.value == nil && o.value == nil || (b.value != nil && o.value != nil && b.value.Equals(o.value)))
}

func (b bound) String() string {
	var s1 string
	if b.value == nil {
		s1 = "<nil>"
	} else {
		buf := bytes.Buffer{}
		types.WriteEncodedValue(&buf, b.value)
		s1 = buf.String()
	}
	return fmt.Sprintf("bound{v: %s, include: %t, infinity: %d}", s1, b.include, b.infinity)
}

func (v bound) minValue(o bound) (res bound) {
	if v.isLessThan(o) {
		return v
	}
	return o
}

func (v bound) maxValue(o bound) (res bound) {
	if v.isLessThan(o) {
		return o
	}
	return v
}

type queryRange struct {
	lower bound
	upper bound
}

func (r queryRange) and(o queryRange) (rangeDescs queryRangeSlice) {
	if !r.intersects(o) {
		return []queryRange{}
	}

	lower := r.lower.maxValue(o.lower)
	upper := r.upper.minValue(o.upper)
	return []queryRange{{lower, upper}}
}

func (r queryRange) or(o queryRange) (rSlice queryRangeSlice) {
	if r.intersects(o) {
		v1 := r.lower.minValue(o.lower)
		v2 := r.upper.maxValue(o.upper)
		return queryRangeSlice{queryRange{v1, v2}}
	}
	rSlice = queryRangeSlice{r, o}
	sort.Sort(rSlice)
	return rSlice
}

func (r queryRange) intersects(o queryRange) (res bool) {
	if r.lower.isGreaterThanOrEqual(o.lower) && r.lower.isLessThanOrEqual(o.upper) {
		return true
	}
	if r.upper.isGreaterThanOrEqual(o.lower) && r.upper.isLessThanOrEqual(o.upper) {
		return true
	}
	if o.lower.isGreaterThanOrEqual(r.lower) && o.lower.isLessThanOrEqual(r.upper) {
		return true
	}
	if o.upper.isGreaterThanOrEqual(r.lower) && o.upper.isLessThanOrEqual(r.upper) {
		return true
	}
	return false
}

func (r queryRange) String() string {
	return fmt.Sprintf("queryRange{lower: %s, upper: %s", r.lower, r.upper)
}

// queryRangeSlice defines the sort.Interface. This implementation sorts queryRanges by
// the lower bound in ascending order.
type queryRangeSlice []queryRange

func (rSlice queryRangeSlice) Len() int {
	return len(rSlice)
}

func (rSlice queryRangeSlice) Swap(i, j int) {
	rSlice[i], rSlice[j] = rSlice[j], rSlice[i]
}

func (rSlice queryRangeSlice) Less(i, j int) bool {
	return !rSlice[i].lower.equals(rSlice[j].lower) && rSlice[i].lower.isLessThanOrEqual(rSlice[j].lower)
}

func (rSlice queryRangeSlice) dbgPrint(w io.Writer) {
	for i, rd := range rSlice {
		if i == 0 {
			fmt.Fprintf(w, "\n#################\n")
		}
		fmt.Fprintf(w, "queryRange %d: %s\n", i, rd)
	}
	if len(rSlice) > 0 {
		fmt.Fprintf(w, "\n")
	}
}
