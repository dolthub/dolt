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
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"io"
)

func newMergeKvIter(
	leftIter, rightIter prolly.MapIter,
	leftMap prolly.Map,
	joiner *prollyToSqlJoiner,
	leftFilter, rightFilter, joinFilter sql.Expression,
	isLeftJoin bool,
	excludeNulls bool,
) (*mergeJoinKvIter, error) {
	filters := expression.SplitConjunction(joinFilter)
	cmp, ok := filters[0].(expression.Comparer)
	if !ok {
		if equality, ok := filters[0].(expression.Equality); ok {
			var err error
			cmp, err = equality.ToComparer()
			if err != nil {
				return nil, nil
			}
		} else {
			return nil, nil
		}
	}

	if len(filters) == 0 {
		return nil, sql.ErrNoJoinFilters.New()
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
		return nil, nil
	}

	return &mergeJoinKvIter{
		leftIter:     leftIter,
		rightIter:    rightIter,
		joiner:       joiner,
		cmpDesc:      leftMap.KeyDesc().PrefixDesc(1),
		leftFilter:   leftFilter,
		rightFilter:  rightFilter,
		joinFilters:  filters[1:],
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

	// todo convert comparer to be []byte-amenable callback
	cmpDesc val.TupleDesc

	nextKey val.Tuple
	nextVal val.Tuple

	lookaheadBuf [][]byte
	matchPos     int

	// projections
	joiner *prollyToSqlJoiner

	// todo: we want to build KV-side static expression implementations
	// so that we can execute filters more efficiently
	leftFilter  sql.Expression
	rightFilter sql.Expression
	joinFilters []sql.Expression

	// LEFT_JOIN impl details
	excludeNulls bool
	isLeftJoin   bool
	returnedARow bool
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

	if len(l.lookaheadBuf) > 0 {
		goto match
	}

incr:
	// increment state
	switch l.cmpDesc.Compare(l.leftKey, l.rightKey) {
	case -1:
		l.leftKey, l.leftVal, err = l.leftIter.Next(ctx)
		if err != nil {
			return nil, err
		}
	case 0:
		goto matchBuf
	case +1:
		if l.nextKey != nil {
			l.rightKey, l.rightVal = l.nextKey, l.nextVal
			l.nextKey = nil
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
	l.nextKey, l.nextVal, err = l.rightIter.Next(ctx)
	if err != nil {
		// TODO: EOF here?
		return nil, err
	}
	if l.cmpDesc.Compare(l.leftKey, l.nextKey) == 0 {
		l.lookaheadBuf = append(l.lookaheadBuf, l.nextKey, l.nextVal)
		goto matchBuf
	}

match:
	// match state
	// lookaheadBuf and rightKey
	var candidate sql.Row
	var rightKeyNil bool
	if l.matchPos < len(l.lookaheadBuf) {
		candidate, err = l.joiner.buildRow(ctx, l.leftKey, l.leftVal, l.lookaheadBuf[l.matchPos], l.lookaheadBuf[l.matchPos+1])
		rightKeyNil = l.lookaheadBuf[l.matchPos] == nil
		l.matchPos += 2
	} else if l.matchPos == len(l.lookaheadBuf) {
		candidate, err = l.joiner.buildRow(ctx, l.leftKey, l.leftVal, l.rightKey, l.rightVal)
		rightKeyNil = l.rightKey == nil
		l.matchPos++
	} else {
		// reset
		l.leftKey, l.leftVal, err = l.leftIter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if l.cmpDesc.Compare(l.leftKey, l.rightKey) != 0 {
			l.lookaheadBuf = l.lookaheadBuf[:0]
			l.matchPos = 0
			goto incr
		}
	}
	if err != nil {
		return nil, err
	}

	// check filters
	for _, f := range l.joinFilters {
		res, err := sql.EvaluateCondition(ctx, f, candidate)
		if err != nil {
			return nil, err
		}
		if res == nil && l.excludeNulls {
			// override default left join behavior
			goto match
		} else if !sql.IsTrue(res) && !rightKeyNil {
			goto match
		}
	}
	return candidate, nil
}
