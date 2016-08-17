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
}

// logExpr represents a logical 'and' or 'or' expression between two other expressions. e.g.
type logExpr struct {
	op    boolOp
	expr1 expr
	expr2 expr
}

// compExpr represents a comparison between index values and constants. e.g. model-year-index > 1999
type compExpr struct {
	idxPath string
	op      compOp
	v1      types.Value
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
	}
	return queryRangeSlice{r}
}

func (re compExpr) dbgPrintTree(w io.Writer, level int) {
	buf := bytes.Buffer{}
	types.WriteEncodedValue(&buf, re.v1)
	fmt.Fprintf(w, "%*s%s %s %s\n", 2*level, "", re.idxPath, re.op, buf.String())
}
