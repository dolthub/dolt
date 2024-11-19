// Copyright 2024 Dolthub, Inc.
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

package kvexec

import (
	"errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

func newMergeKvIter(
	leftIter, rightIter prolly.MapIter,
	joiner *prollyToSqlJoiner,
	lrComparer, llComparer func(val.Tuple, val.Tuple, val.Tuple, val.Tuple) int,
	leftNorm, rightNorm coveringNormalizer,
	leftFilter, rightFilter sql.Expression,
	joinFilters []sql.Expression,
	isLeftJoin bool,
	excludeNulls bool,
) (*mergeJoinKvIter, error) {
	//
	//cmp, ok := filters[0].(expression.Comparer)
	//if !ok {
	//	if equality, ok := filters[0].(expression.Equality); ok {
	//		var err error
	//		cmp, err = equality.ToComparer()
	//		if err != nil {
	//			return nil, nil
	//		}
	//	} else {
	//		return nil, nil
	//	}
	//}
	//
	//if len(filters) == 0 {
	//	return nil, sql.ErrNoJoinFilters.New()
	//}
	//
	//var lIdx, rIdx int
	//if l, ok := cmp.Left().(*expression.GetField); ok {
	//	if r, ok := cmp.Right().(*expression.GetField); ok {
	//		// get indices of get fields
	//		lIdx = l.Index()
	//		rIdx = r.Index()
	//	}
	//}
	//
	//if lIdx == rIdx {
	//	return nil, fmt.Errorf("unsupported merge comparison")
	//}
	//if lIdx != 0 || rIdx != joiner.kvSplits[0] {
	//	return nil, fmt.Errorf("unsupported merge comparison")
	//}

	return &mergeJoinKvIter{
		leftIter:     leftIter,
		rightIter:    rightIter,
		joiner:       joiner,
		lrCmp:        lrComparer,
		llCmp:        llComparer,
		leftNorm:     leftNorm,
		rightNorm:    rightNorm,
		joinFilters:  joinFilters,
		leftFilter:   leftFilter,
		rightFilter:  rightFilter,
		isLeftJoin:   isLeftJoin,
		excludeNulls: excludeNulls,
	}, nil
}

type mergeJoinKvIter struct {
	leftIter prolly.MapIter
	leftKey  val.Tuple
	leftVal  val.Tuple

	rightIter prolly.MapIter
	rightKey  val.Tuple
	rightVal  val.Tuple

	lrCmp func(val.Tuple, val.Tuple, val.Tuple, val.Tuple) int
	llCmp func(val.Tuple, val.Tuple, val.Tuple, val.Tuple) int

	leftNorm  coveringNormalizer
	rightNorm coveringNormalizer

	nextRightKey val.Tuple
	nextRightVal val.Tuple

	lookaheadBuf [][]byte
	matchPos     int

	// projections
	joiner *prollyToSqlJoiner

	// todo: we want to build KV-side static expression implementations
	// so that we can execute filters more efficiently
	joinFilters []sql.Expression
	leftFilter  sql.Expression
	rightFilter sql.Expression

	// LEFT_JOIN impl details
	excludeNulls bool
	isLeftJoin   bool
	matchedLeft  bool
}

var _ sql.RowIter = (*mergeJoinKvIter)(nil)

func (l *mergeJoinKvIter) Close(_ *sql.Context) error {
	return nil
}

func (l *mergeJoinKvIter) Next(ctx *sql.Context) (sql.Row, error) {
	var err error
	if l.leftKey == nil {
		l.leftKey, l.leftVal, err = l.leftIter.Next(ctx)
		if err != nil {
			return nil, err
		}
		l.rightKey, l.rightVal, err = l.rightIter.Next(ctx)
		if err != nil {
			return nil, err
		}
	}

	if len(l.lookaheadBuf) > 0 || l.matchPos > 0 {
		goto match
	}

incr:
	// increment state
	switch l.lrCmp(l.leftKey, l.leftVal, l.rightKey, l.rightVal) {
	case -1:
		var oldLeftKey, oldLeftVal val.Tuple
		{
			// left join
			if !l.matchedLeft && l.isLeftJoin {
				oldLeftKey, oldLeftVal = l.leftKey, l.leftVal
			}
			l.matchedLeft = false
		}

		l.leftKey, l.leftVal, err = l.leftIter.Next(ctx)
		if err != nil {
			return nil, err
		}

		if oldLeftKey != nil {
			return l.buildCandidate(ctx, oldLeftKey, oldLeftVal, nil, nil)
		}
	case 0:
		goto matchBuf
	case +1:
		if l.nextRightKey != nil {
			l.rightKey, l.rightVal = l.nextRightKey, l.nextRightVal
			l.nextRightKey = nil
		} else {
			l.rightKey, l.rightVal, err = l.rightIter.Next(ctx)
			if err != nil {
				return nil, err
			}
		}
	}
	if l.leftKey == nil || l.rightKey == nil {
		return nil, io.EOF
	}
	goto incr

matchBuf:
	// fill lookahead buffer with keys from right side
	l.nextRightKey, l.nextRightVal, err = l.rightIter.Next(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// this is OK, but need to skip nil key comparison
			goto match
		}
		return nil, err
	}
	if l.lrCmp(l.leftKey, l.leftVal, l.nextRightKey, l.nextRightVal) == 0 {
		l.lookaheadBuf = append(l.lookaheadBuf, l.nextRightKey, l.nextRightVal)
		goto matchBuf
	}

match:
	// match state
	// lookaheadBuf and rightKey
	if l.matchPos < len(l.lookaheadBuf) {
		candidate, ok, err := l.tryReturn(ctx, l.leftKey, l.leftVal, l.lookaheadBuf[l.matchPos], l.lookaheadBuf[l.matchPos+1])
		if err != nil {
			return nil, err
		}
		l.matchPos += 2
		if !ok {
			goto match
		}
		return candidate, nil
	} else if l.matchPos == len(l.lookaheadBuf) {
		candidate, ok, err := l.tryReturn(ctx, l.leftKey, l.leftVal, l.rightKey, l.rightVal)
		if err != nil {
			return nil, err
		}
		l.matchPos++
		if !ok {
			goto match
		}
		return candidate, nil
	} else {
		// reset
		l.matchPos = 0

		// compare the current to the next left key
		tmpKey, tmpVal := l.leftKey, l.leftVal
		l.leftKey, l.leftVal, err = l.leftIter.Next(ctx)
		if err != nil {
			return nil, err
		}
		cmp := l.llCmp(tmpKey, tmpVal, l.leftKey, l.leftVal)

		if cmp != 0 {
			// if the left key is new, invalidate lookahead buffer and
			// advance the right side
			l.lookaheadBuf = l.lookaheadBuf[:0]
			if l.nextRightKey != nil {
				l.rightKey, l.rightVal = l.nextRightKey, l.nextRightVal
				l.nextRightKey = nil
			} else {
				l.rightKey, l.rightVal, err = l.rightIter.Next(ctx)
				if err != nil {
					return nil, err
				}
			}
		}

		if l.isLeftJoin && !l.matchedLeft {
			// consider left join after appropriate state transitions
			l.matchedLeft = false
			ret, ok, err := l.tryReturn(ctx, tmpKey, tmpVal, nil, nil)
			if err != nil {
				return nil, err
			}
			if ok {
				return ret, nil
			}
		}
		l.matchedLeft = false

		if cmp == 0 {
			// the left keys are equivalent, the right-side lookahead
			// buffer is still valid for the new left key. the only reason
			// we do not do this check earlier is for left-joins.
			goto match
		}
		goto incr
	}
}

func (l *mergeJoinKvIter) tryReturn(ctx *sql.Context, leftKey, leftVal, rightKey, rightVal val.Tuple) (sql.Row, bool, error) {
	candidate, err := l.buildCandidate(ctx, leftKey, leftVal, rightKey, rightVal)
	if err != nil {
		return nil, false, err
	}

	rightKeyNil := l.rightKey == nil

	if l.leftFilter != nil {
		res, err := sql.EvaluateCondition(ctx, l.leftFilter, candidate[:l.joiner.kvSplits[0]])
		if err != nil {
			return nil, false, err
		}
		if !sql.IsTrue(res) {
			return nil, false, nil
		}
	}

	if l.rightFilter != nil {
		res, err := sql.EvaluateCondition(ctx, l.rightFilter, candidate[l.joiner.kvSplits[0]:])
		if err != nil {
			return nil, false, err
		}
		if !sql.IsTrue(res) {
			return nil, false, nil
		}
	}

	// check filters
	for _, f := range l.joinFilters {
		res, err := sql.EvaluateCondition(ctx, f, candidate)
		if err != nil {
			return nil, false, err
		}
		if res == nil && l.excludeNulls {
			// override default left join behavior
			return nil, false, nil
		} else if !sql.IsTrue(res) && !rightKeyNil {
			return nil, false, nil
		}
	}

	l.matchedLeft = true
	return candidate, true, nil
}

func (l *mergeJoinKvIter) buildCandidate(ctx *sql.Context, leftKey, leftVal, rightKey, rightVal val.Tuple) (sql.Row, error) {
	var err error
	if l.leftNorm != nil && leftKey != nil {
		leftKey, leftVal, err = l.leftNorm(leftKey)
		if err != nil {
			return nil, err
		}
	}
	if l.rightNorm != nil && rightKey != nil {
		rightKey, rightVal, err = l.rightNorm(rightKey)
		if err != nil {
			return nil, err
		}
	}
	return l.joiner.buildRow(ctx, leftKey, leftVal, rightKey, rightVal)
}

var defCmp = val.DefaultTupleComparator{}

func mergeComparer(
	filter sql.Expression,
	leftSch, rightSch schema.Schema,
	projections []uint64,
	lKeyDesc, lValDesc, rKeyDesc, rValDesc val.TupleDesc,
) (lrCmp, llCmp func(leftKey, leftVal, rightKey, rightVal val.Tuple) int, ok bool) {
	// first filter expression needs to be evaluated
	// can accept a subset of types -- (cmp GF GF)
	// need to map expression id to key or value position

	cmp, ok := filter.(expression.Comparer)
	if !ok {
		if equality, ok := filter.(expression.Equality); ok {
			var err error
			cmp, err = equality.ToComparer()
			if err != nil {
				return nil, nil, false
			}
		} else {
			return nil, nil, false
		}
	}

	var lIdx, rIdx int
	if l, ok := cmp.Left().(*expression.GetField); ok {
		if r, ok := cmp.Right().(*expression.GetField); ok {
			// get indices of get fields
			lIdx = l.Index()
			rIdx = r.Index()
		}
	}

	if lIdx == rIdx {
		return nil, nil, false
	}

	// |projections| and idx are in terms of output projections,
	// but we need tuple and position in terms of secondary index.
	// Use tags for the mapping.
	lKeyIdx, lKeyOk := leftSch.GetPKCols().StoredIndexByTag(projections[lIdx])
	lValIdx, lValOk := leftSch.GetNonPKCols().StoredIndexByTag(projections[lIdx])
	rKeyIdx, rKeyOk := rightSch.GetPKCols().StoredIndexByTag(projections[rIdx])
	rValIdx, rValOk := rightSch.GetNonPKCols().StoredIndexByTag(projections[rIdx])

	// first field in keyless value is cardinality
	if schema.IsKeyless(leftSch) {
		lValIdx++
	}

	if schema.IsKeyless(rightSch) {
		rValIdx++
	}

	var lTyp val.Type
	var rTyp val.Type
	if lKeyOk {
		lTyp = lKeyDesc.Types[lKeyIdx]
		llCmp = func(leftKey, _, rightKey, _ val.Tuple) int {
			return defCmp.CompareValues(0, leftKey.GetField(lKeyIdx), rightKey.GetField(lKeyIdx), lTyp)
		}
		if rKeyOk {
			rTyp = rKeyDesc.Types[rKeyIdx]
			lrCmp = func(leftKey, _, rightKey, _ val.Tuple) int {
				return defCmp.CompareValues(0, leftKey.GetField(lKeyIdx), rightKey.GetField(rKeyIdx), lTyp)
			}
		} else if rValOk {
			rTyp = rValDesc.Types[rValIdx]
			lrCmp = func(leftKey, _, _, rightVal val.Tuple) int {
				return defCmp.CompareValues(0, leftKey.GetField(lKeyIdx), rightVal.GetField(rValIdx), lTyp)
			}
		} else {
			return nil, nil, false
		}
	} else if lValOk {
		lTyp = lValDesc.Types[lValIdx]
		llCmp = func(_, leftVal, _, rightVal val.Tuple) int {
			return defCmp.CompareValues(0, leftVal.GetField(lValIdx), rightVal.GetField(lValIdx), lTyp)
		}
		if rKeyOk {
			rTyp = rKeyDesc.Types[rKeyIdx]
			lrCmp = func(_, leftVal, rightKey, _ val.Tuple) int {
				return defCmp.CompareValues(0, leftVal.GetField(lValIdx), rightKey.GetField(rKeyIdx), lTyp)
			}
		} else if rValOk {
			rTyp = rValDesc.Types[rValIdx]
			lrCmp = func(_, leftVal, _, rightVal val.Tuple) int {
				return defCmp.CompareValues(0, leftVal.GetField(lValIdx), rightVal.GetField(rValIdx), lTyp)
			}
		} else {
			return nil, nil, false
		}
	} else {
		return nil, nil, false
	}

	if lTyp.Enc != rTyp.Enc {
		return nil, nil, false
	}

	return lrCmp, llCmp, true
}

func schemaIsCovering(sch schema.Schema, projections []uint64) bool {
	cols := sch.GetAllCols()
	if len(projections) > cols.Size() {
		return false
	}
	for _, colTag := range projections {
		if _, ok := cols.TagToIdx[colTag]; !ok {
			return false
		}
	}
	return true
}
