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

type expr interface {
	ranges() queryRangeSlice
	dbgPrintTree(w io.Writer, level int)
	indexName() string
	iterator(im *indexManager) types.SetIterator
}

// logExpr represents a logical 'and' or 'or' expression between two other expressions.
// e.g. logExpr would represent the and/or expressions in this query:
// (index1 > 0 and index1 < 9) or (index1 > 100 and index < 109)
type logExpr struct {
	op      boolOp
	expr1   expr
	expr2   expr
	idxName string
}

type compExpr struct {
	idxName string
	op      compOp
	v1      types.Value
}

func (le logExpr) indexName() string {
	return le.idxName
}

func (le logExpr) iterator(im *indexManager) types.SetIterator {
	if le.idxName != "" {
		return unionizeIters(iteratorsFromRanges(im.indexes[le.idxName], le.ranges()))
	}

	i1 := le.expr1.iterator(im)
	i2 := le.expr2.iterator(im)
	var iter types.SetIterator
	switch le.op {
	case and:
		if i1 == nil || i2 == nil {
			return nil
		}
		iter = types.NewIntersectionIterator(le.expr1.iterator(im), le.expr2.iterator(im))
	case or:
		if i1 == nil {
			return i2
		}
		if i2 == nil {
			return i1
		}
		iter = types.NewUnionIterator(le.expr1.iterator(im), le.expr2.iterator(im))
	}
	return iter
}

func (le logExpr) ranges() (ranges queryRangeSlice) {
	rslice1 := le.expr1.ranges()
	rslice2 := le.expr2.ranges()
	rslice := queryRangeSlice{}

	switch le.op {
	case and:
		if len(rslice1) == 0 || len(rslice2) == 0 {
			return rslice
		}
		for _, r1 := range rslice1 {
			for _, r2 := range rslice2 {
				rslice = append(rslice, r1.and(r2)...)
			}
		}
		sort.Sort(rslice)
		return rslice
	case or:
		if len(rslice1) == 0 {
			return rslice2
		}
		if len(rslice2) == 0 {
			return rslice1
		}
		for _, r1 := range rslice1 {
			for _, r2 := range rslice2 {
				rslice = append(rslice, r1.or(r2)...)
			}
		}
		sort.Sort(rslice)
		return rslice
	}
	return queryRangeSlice{}
}

func (le logExpr) dbgPrintTree(w io.Writer, level int) {
	fmt.Fprintf(w, "%*s%s\n", 2*level, "", le.op)
	if le.expr1 != nil {
		le.expr1.dbgPrintTree(w, level+1)
	}
	if le.expr2 != nil {
		le.expr2.dbgPrintTree(w, level+1)
	}
}

func (re compExpr) indexName() string {
	return re.idxName
}

func iteratorsFromRange(index types.Map, rd queryRange) []types.SetIterator {
	first := true
	iterators := []types.SetIterator{}
	index.IterFrom(rd.lower.value, func(k, v types.Value) bool {
		if first && rd.lower.value != nil && !rd.lower.include && rd.lower.value.Equals(k) {
			return false
		}
		if rd.upper.value != nil {
			if !rd.upper.include && rd.upper.value.Equals(k) {
				return true
			}
			if rd.upper.value.Less(k) {
				return true
			}
		}
		s := v.(types.Set)
		iterators = append(iterators, s.Iterator())
		return false
	})
	return iterators
}

func iteratorsFromRanges(index types.Map, ranges queryRangeSlice) []types.SetIterator {
	iterators := []types.SetIterator{}
	for _, r := range ranges {
		iterators = append(iterators, iteratorsFromRange(index, r)...)
	}
	return iterators
}

func unionizeIters(iters []types.SetIterator) types.SetIterator {
	if len(iters) == 0 {
		return nil
	}
	if len(iters) <= 1 {
		return iters[0]
	}

	unionIters := []types.SetIterator{}
	var iter0 types.SetIterator
	for i, iter := range iters {
		if i%2 == 0 {
			iter0 = iter
		} else {
			unionIters = append(unionIters, types.NewUnionIterator(iter0, iter))
			iter0 = nil
		}
	}
	if iter0 != nil {
		unionIters = append(unionIters, iter0)
	}
	return unionizeIters(unionIters)
}

func (re compExpr) iterator(im *indexManager) types.SetIterator {
	index := im.indexes[re.idxName]
	iters := iteratorsFromRanges(index, re.ranges())
	return unionizeIters(iters)
}

func (re compExpr) ranges() (ranges queryRangeSlice) {
	var r queryRange
	switch re.op {
	case equals:
		e := bound{value: re.v1, include: true}
		r = queryRange{lower: e, upper: e}
	case gt:
		r = queryRange{lower: bound{re.v1, false, 0}, upper: bound{nil, true, 1}}
	case gte:
		r = queryRange{lower: bound{re.v1, true, 0}, upper: bound{nil, true, 1}}
	case lt:
		r = queryRange{lower: bound{nil, true, -1}, upper: bound{re.v1, false, 0}}
	case lte:
		r = queryRange{lower: bound{nil, true, -1}, upper: bound{re.v1, true, 0}}
	case ne:
		return queryRangeSlice{
			{lower: bound{nil, true, -1}, upper: bound{re.v1, false, 0}},
			{lower: bound{re.v1, false, 0}, upper: bound{nil, true, 1}},
		}
	}
	return queryRangeSlice{r}
}

func (re compExpr) dbgPrintTree(w io.Writer, level int) {
	buf := bytes.Buffer{}
	types.WriteEncodedValue(&buf, re.v1)
	fmt.Fprintf(w, "%*s%s %s %s\n", 2*level, "", re.idxName, re.op, buf.String())
}
