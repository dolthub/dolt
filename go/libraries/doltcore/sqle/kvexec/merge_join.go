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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

func newMergeKvIter(
	leftState, rightState mergeState,
	joiner *prollyToSqlJoiner,
	lrComparer, llComparer func(val.Tuple, val.Tuple, val.Tuple, val.Tuple) int,
	joinFilters []sql.Expression,
	isLeftJoin bool,
	excludeNulls bool,
) (*mergeJoinKvIter, error) {
	return &mergeJoinKvIter{
		leftIter:     leftState.iter,
		rightIter:    rightState.iter,
		joiner:       joiner,
		lrCmp:        lrComparer,
		llCmp:        llComparer,
		leftNorm:     leftState.norm,
		rightNorm:    rightState.norm,
		joinFilters:  joinFilters,
		leftFilter:   leftState.filter,
		rightFilter:  rightState.filter,
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

	// Non-covering secondary index reads are faster separated
	// from the initial mapIter, because we avoid unnecessary
	// primary reads. Note: keyless indexes are less amenable to
	// this optimization because their cardinality is stored in the
	// primary index.
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
	exhaustLeft  bool
}

var _ sql.RowIter = (*mergeJoinKvIter)(nil)

func (l *mergeJoinKvIter) Close(_ *sql.Context) error {
	return nil
}

func (l *mergeJoinKvIter) Next(ctx *sql.Context) (sql.Row, error) {
	var err error
	if l.leftKey == nil {
		if err := l.initialize(ctx); err != nil {
			if errors.Is(err, io.EOF) && l.isLeftJoin {
				return l.exhaustLeftReturn(ctx)
			}
			return nil, err
		}
	}

	if l.exhaustLeft {
		return l.exhaustLeftReturn(ctx)
	}

	if len(l.lookaheadBuf) > 0 || l.matchPos > 0 {
		goto match
	}

compare:
	// compare the left/right keys.
	// if equal continue to match buffer stage
	// if unequal increment one side
	for {
		switch l.lrCmp(l.leftKey, l.leftVal, l.rightKey, l.rightVal) {
		case -1:
			// left side has to consider left join non-matches
			var oldLeftKey, oldLeftVal val.Tuple
			{
				// left join state
				if !l.matchedLeft && l.isLeftJoin {
					oldLeftKey, oldLeftVal = l.leftKey, l.leftVal
				}
				l.matchedLeft = false
			}

			l.leftKey, l.leftVal, err = l.leftIter.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) && oldLeftKey != nil {
					l.exhaustLeft = true
					candidate, ok, err := l.buildResultRow(ctx, oldLeftKey, oldLeftVal, nil, nil)
					if err != nil {
						return nil, err
					}
					if ok {
						return candidate, nil
					}
				}
				return nil, err
			}

			if oldLeftKey != nil {
				candidate, ok, err := l.buildResultRow(ctx, oldLeftKey, oldLeftVal, nil, nil)
				if err != nil {
					return nil, err
				}
				if ok {
					return candidate, nil
				}
			}
		case 0:
			if err := l.fillMatchBuf(ctx); err != nil {
				return nil, err
			}
			goto match
		case +1:
			// right side considers lookahead used for match buffer
			if l.nextRightKey != nil {
				l.rightKey, l.rightVal = l.nextRightKey, l.nextRightVal
				l.nextRightKey = nil
			} else {
				l.rightKey, l.rightVal, err = l.rightIter.Next(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) && l.isLeftJoin {
						return l.exhaustLeftReturn(ctx)
					}
					return nil, err
				}
			}
		}
	}

match:
	// We start this stage with at least one match. We consider the
	// lookahead buffer matches before |l.rightKey| because we can
	// track state with a single |l.matchPos| variable. Matching merge
	// condition does not guarantee the rest of the filters will match.
	for {
		if l.matchPos < len(l.lookaheadBuf) {
			candidate, ok, err := l.buildResultRow(ctx, l.leftKey, l.leftVal, l.lookaheadBuf[l.matchPos], l.lookaheadBuf[l.matchPos+1])
			if err != nil {
				return nil, err
			}
			l.matchPos += 2
			l.matchedLeft = ok
			if ok {
				return candidate, nil
			}
		} else if l.matchPos == len(l.lookaheadBuf) {
			candidate, ok, err := l.buildResultRow(ctx, l.leftKey, l.leftVal, l.rightKey, l.rightVal)
			if err != nil {
				return nil, err
			}
			l.matchedLeft = ok
			l.matchPos++
			if ok {
				return candidate, nil
			}
		} else {
			// We exhausted matches for the current |l.leftKey|.
			// See whether we should flush or reuse the right buffer
			// with the next left key.
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
						if errors.Is(err, io.EOF) && l.isLeftJoin {
							// need to check |tmpKey| for left-join before
							// jumping to exhaustLeft
							l.exhaustLeft = true
						} else {
							return nil, err
						}
					}
				}
			}

			if l.isLeftJoin && !l.matchedLeft {
				// consider left join after appropriate state transitions
				ret, ok, err := l.buildResultRow(ctx, tmpKey, tmpVal, nil, nil)
				if err != nil {
					return nil, err
				}
				if ok {
					return ret, nil
				}
			}
			l.matchedLeft = false

			if cmp == 0 {
				// The left keys are equivalent, the right-side lookahead
				// buffer is still valid for the new left key. The only reason
				// we didn't short-circuit this check earlier is for left-joins.
				goto match
			}
			if l.exhaustLeft {
				return l.exhaustLeftReturn(ctx)
			}
			goto compare
		}
	}
}

// buildResultRow combines a set of key/value tuples into an output row
// and checks it against filter expressions. Return the row, a boolean
// that indicates whether it passed filter checks, and an error.
func (l *mergeJoinKvIter) buildResultRow(ctx *sql.Context, leftKey, leftVal, rightKey, rightVal val.Tuple) (sql.Row, bool, error) {
	candidate, err := l.buildCandidate(ctx, leftKey, leftVal, rightKey, rightVal)
	if err != nil {
		return nil, false, err
	}

	rightKeyNil := rightKey == nil

	if l.leftFilter != nil {
		res, err := sql.EvaluateCondition(ctx, l.leftFilter, candidate.Subslice(0, l.joiner.kvSplits[0]))
		if err != nil {
			return nil, false, err
		}
		if !sql.IsTrue(res) {
			return nil, false, nil
		}
	}

	if l.rightFilter != nil && !rightKeyNil {
		res, err := sql.EvaluateCondition(ctx, l.rightFilter, candidate.Subslice(l.joiner.kvSplits[0], candidate.Len()))
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

	return candidate, true, nil
}

func (l *mergeJoinKvIter) exhaustLeftReturn(ctx *sql.Context) (sql.Row, error) {
	l.exhaustLeft = true
	if l.leftKey == nil {
		return nil, io.EOF
	}
	var err error
	for {
		if l.matchedLeft {
			l.leftKey, l.leftVal, err = l.leftIter.Next(ctx)
			if err != nil {
				return nil, err
			}
		}
		l.matchedLeft = true // simplifies loop
		ret, ok, err := l.buildResultRow(ctx, l.leftKey, l.leftVal, nil, nil)
		if err != nil {
			return nil, err
		}
		if ok {
			return ret, nil
		}
	}
}

// Fill lookahead buffer with all right side keys that match current
// left key. |l.nextRightKey| can be a lookahead key or nil at the
// end of this stage.
func (l *mergeJoinKvIter) fillMatchBuf(ctx *sql.Context) error {
	var err error
	for {
		l.nextRightKey, l.nextRightVal, err = l.rightIter.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// this is OK, but need to skip nil key comparison
				return nil
			}
			return err
		}
		if l.lrCmp(l.leftKey, l.leftVal, l.nextRightKey, l.nextRightVal) == 0 {
			l.lookaheadBuf = append(l.lookaheadBuf, l.nextRightKey, l.nextRightVal)
		} else {
			return nil
		}
	}
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

func (l *mergeJoinKvIter) initialize(ctx *sql.Context) error {
	var err error
	l.leftKey, l.leftVal, err = l.leftIter.Next(ctx)
	if err != nil {
		return err
	}
	l.rightKey, l.rightVal, err = l.rightIter.Next(ctx)
	if err != nil {
		return err
	}
	return nil
}

var defCmp = val.DefaultTupleComparator{}

func mergeComparer(
	filter sql.Expression,
	lState, rState mergeState,
	projections []uint64,
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
	lKeyIdx, lKeyOk := lState.idxSch.GetPKCols().StoredIndexByTag(projections[lIdx])
	lValIdx, lValOk := lState.idxSch.GetNonPKCols().StoredIndexByTag(projections[lIdx])
	rKeyIdx, rKeyOk := rState.idxSch.GetPKCols().StoredIndexByTag(projections[rIdx])
	rValIdx, rValOk := rState.idxSch.GetNonPKCols().StoredIndexByTag(projections[rIdx])

	// first field in keyless value is cardinality
	if schema.IsKeyless(lState.idxSch) {
		lValIdx++
	}

	if schema.IsKeyless(rState.idxSch) {
		rValIdx++
	}

	var lTyp val.Type
	var rTyp val.Type
	if lKeyOk {
		lTyp = lState.idxMap.KeyDesc().Types[lKeyIdx]
		llCmp = func(leftKey, _, rightKey, _ val.Tuple) int {
			return defCmp.CompareValues(0, leftKey.GetField(lKeyIdx), rightKey.GetField(lKeyIdx), lTyp)
		}
		if rKeyOk {
			rTyp = rState.idxMap.KeyDesc().Types[rKeyIdx]
			lrCmp = func(leftKey, _, rightKey, _ val.Tuple) int {
				return defCmp.CompareValues(0, leftKey.GetField(lKeyIdx), rightKey.GetField(rKeyIdx), lTyp)
			}
		} else if rValOk {
			rTyp = rState.idxMap.ValDesc().Types[rValIdx]
			lrCmp = func(leftKey, _, _, rightVal val.Tuple) int {
				return defCmp.CompareValues(0, leftKey.GetField(lKeyIdx), rightVal.GetField(rValIdx), lTyp)
			}
		} else {
			return nil, nil, false
		}
	} else if lValOk {
		lTyp = lState.idxMap.ValDesc().Types[lValIdx]
		llCmp = func(_, leftVal, _, rightVal val.Tuple) int {
			return defCmp.CompareValues(0, leftVal.GetField(lValIdx), rightVal.GetField(lValIdx), lTyp)
		}
		if rKeyOk {
			rTyp = rState.idxMap.KeyDesc().Types[rKeyIdx]
			lrCmp = func(_, leftVal, rightKey, _ val.Tuple) int {
				return defCmp.CompareValues(0, leftVal.GetField(lValIdx), rightKey.GetField(rKeyIdx), lTyp)
			}
		} else if rValOk {
			rTyp = rState.idxMap.ValDesc().Types[rValIdx]
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

// schemaIsCovering returns true if all projection tags are found in the
// source schema. If any tag is not found in the schema, the primary index
// has to be access to complete the |projections| list.
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
