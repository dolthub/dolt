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
	"context"
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func rowIterTableLookupJoin(
	srcIter prolly.MapIter,
	dstIter index.SecondaryLookupIterGen,
	mapping *lookupMapping,
	rowJoiner *prollyToSqlJoiner,
	srcFilter, dstFilter, joinFilter sql.Expression,
	isLeftJoin bool,
	excludeNulls bool,
) (sql.RowIter, error) {
	return newLookupKvIter(srcIter, dstIter, mapping, rowJoiner, srcFilter, dstFilter, joinFilter, isLeftJoin, excludeNulls)
}

type lookupJoinKvIter struct {
	srcIter prolly.MapIter
	srcKey  val.Tuple
	srcVal  val.Tuple

	dstIterGen index.SecondaryLookupIterGen
	dstIter    prolly.MapIter
	dstKey     val.Tuple

	// keyTupleMapper inputs (srcKey, srcVal) to create a dstKey
	keyTupleMapper *lookupMapping

	// projections
	joiner *prollyToSqlJoiner

	// todo: we want to build KV-side static expression implementations
	// so that we can execute filters more efficiently
	srcFilter  sql.Expression
	dstFilter  sql.Expression
	joinFilter sql.Expression

	// LEFT_JOIN impl details
	excludeNulls bool
	isLeftJoin   bool
	returnedARow bool
}

func (l *lookupJoinKvIter) Close(_ *sql.Context) error {
	return nil
}

var _ sql.RowIter = (*lookupJoinKvIter)(nil)

func newLookupKvIter(
	srcIter prolly.MapIter,
	targetIter index.SecondaryLookupIterGen,
	mapping *lookupMapping,
	joiner *prollyToSqlJoiner,
	srcFilter, dstFilter, joinFilter sql.Expression,
	isLeftJoin bool,
	excludeNulls bool,
) (*lookupJoinKvIter, error) {
	if lit, ok := joinFilter.(*expression.Literal); ok {
		if lit.Value() == true {
			joinFilter = nil
		}
	}

	return &lookupJoinKvIter{
		srcIter:        srcIter,
		dstIterGen:     targetIter,
		joiner:         joiner,
		keyTupleMapper: mapping,
		srcFilter:      srcFilter,
		dstFilter:      dstFilter,
		joinFilter:     joinFilter,
		isLeftJoin:     isLeftJoin,
		excludeNulls:   excludeNulls,
	}, nil
}

func (l *lookupJoinKvIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		// (1) initialize secondary iter if does not exist yet
		// (2) read from secondary until EOF
		// (3) concat, convert, filter primary/secondary rows
		var err error
		if l.dstIter == nil {
			// if secondary iterator does not exist:
			//   (1) read the next KV pair from the primary iterator
			//   (2) perform tuple mapping into destination key form
			//   (3) initialize secondary iterator with |dstKey|
			l.returnedARow = false

			l.srcKey, l.srcVal, err = l.srcIter.Next(ctx)
			if err != nil {
				return nil, err
			}
			if l.srcKey == nil {
				return nil, io.EOF
			}

			l.dstKey, err = l.keyTupleMapper.dstKeyTuple(ctx, l.srcKey, l.srcVal)
			if err != nil {
				return nil, err
			}

			l.dstIter, err = l.dstIterGen.New(ctx, l.dstKey)
			if err != nil {
				return nil, err
			}
		}

		dstKey, dstVal, err := l.dstIter.Next(ctx)
		if err != nil && err != io.EOF {
			return nil, err
		}

		if dstKey == nil {
			l.dstIter = nil
			emitLeftJoinNullRow := l.isLeftJoin && !l.returnedARow
			if !emitLeftJoinNullRow {
				continue
			}
		}

		ret, err := l.joiner.buildRow(ctx, l.srcKey, l.srcVal, dstKey, dstVal)
		if err != nil {
			return nil, err
		}

		// side-specific filters are currently hoisted
		if l.srcFilter != nil {
			res, err := sql.EvaluateCondition(ctx, l.srcFilter, ret[:l.joiner.kvSplits[0]])
			if err != nil {
				return nil, err
			}

			if !sql.IsTrue(res) {
				continue
			}

		}
		if l.dstFilter != nil && l.dstKey != nil {
			res, err := sql.EvaluateCondition(ctx, l.dstFilter, ret[l.joiner.kvSplits[0]:])
			if err != nil {
				return nil, err
			}

			if !sql.IsTrue(res) {
				continue
			}
		}
		if l.joinFilter != nil {
			res, err := sql.EvaluateCondition(ctx, l.joinFilter, ret)
			if err != nil {
				return nil, err
			}
			if res == nil && l.excludeNulls {
				// override default left join behavior
				l.dstKey = nil
				continue
			} else if !sql.IsTrue(res) && l.dstKey != nil {
				continue
			}
		}
		l.returnedARow = true
		return ret, nil
	}
}

// lookupMapping is responsible for generating keys for lookups into
// the destination iterator.
type lookupMapping struct {
	split      int
	srcMapping val.OrdinalMapping

	// litTuple are the statically provided literal expressions in the key expression
	litTuple val.Tuple

	litKd    val.TupleDesc
	srcKd    val.TupleDesc
	srcVd    val.TupleDesc
	targetKb *val.TupleBuilder

	ns   tree.NodeStore
	pool pool.BuffPool
}

func newLookupKeyMapping(ctx context.Context, sourceSch schema.Schema, src prolly.Map, tgtKeyDesc val.TupleDesc, keyExprs []sql.Expression) *lookupMapping {
	keyless := schema.IsKeyless(sourceSch)
	// |split| is an index into the schema separating the key and value fields
	var split int
	if keyless {
		// the only key is the hash of the values
		split = 1
	} else {
		split = sourceSch.GetPKCols().Size()
	}

	// schMappings tell us where to look for key fields. A field will either
	// be in the source key tuple (< split), source value tuple (>=split),
	// or in the literal tuple (-1).
	srcMapping := make(val.OrdinalMapping, len(keyExprs))
	var litMappings val.OrdinalMapping
	var litTypes []val.Type
	for i, e := range keyExprs {
		switch e := e.(type) {
		case *expression.GetField:
			// map the schema order index to the physical storage index
			col := sourceSch.GetAllCols().NameToCol[e.Name()]
			if col.IsPartOfPK {
				srcMapping[i] = sourceSch.GetPKCols().TagToIdx[col.Tag]
			} else if keyless {
				// Skip cardinality column
				srcMapping[i] = split + 1 + sourceSch.GetNonPKCols().TagToIdx[col.Tag]
			} else {
				srcMapping[i] = split + sourceSch.GetNonPKCols().TagToIdx[col.Tag]
			}
		case *expression.Literal:
			srcMapping[i] = -1
			litMappings = append(litMappings, i)
			litTypes = append(litTypes, tgtKeyDesc.Types[i])
		}
	}
	litDesc := val.NewTupleDescriptor(litTypes...)
	litTb := val.NewTupleBuilder(litDesc)
	for i, j := range litMappings {
		tree.PutField(ctx, src.NodeStore(), litTb, i, keyExprs[j].(*expression.Literal).Value())
	}

	var litTuple val.Tuple
	if litDesc.Count() > 0 {
		litTuple = litTb.Build(src.Pool())
	}

	return &lookupMapping{
		split:      split,
		srcMapping: srcMapping,
		litTuple:   litTuple,
		litKd:      litDesc,
		srcKd:      src.KeyDesc(),
		srcVd:      src.ValDesc(),
		targetKb:   val.NewTupleBuilder(tgtKeyDesc),
		ns:         src.NodeStore(),
		pool:       src.Pool(),
	}
}

// valid returns whether the source and destination key types
// are type compatible
func (m *lookupMapping) valid() bool {
	var litIdx int
	for to := range m.srcMapping {
		from := m.srcMapping.MapOrdinal(to)
		var desc val.TupleDesc
		if from == -1 {
			desc = m.litKd
			// literal offsets increment sequentially
			from = litIdx
			litIdx++
		} else if from < m.split {
			desc = m.srcKd
		} else {
			// value tuple, adjust offset
			desc = m.srcVd
			from = from - m.split
		}
		if desc.Types[from].Enc != m.targetKb.Desc.Types[to].Enc {
			return false
		}
	}
	return true
}

func (m *lookupMapping) dstKeyTuple(ctx context.Context, srcKey, srcVal val.Tuple) (val.Tuple, error) {
	var litIdx int
	for to := range m.srcMapping {
		from := m.srcMapping.MapOrdinal(to)
		var tup val.Tuple
		var desc val.TupleDesc
		if from == -1 {
			tup = m.litTuple
			desc = m.litKd
			// literal offsets increment sequentially
			from = litIdx
			litIdx++
		} else if from < m.split {
			desc = m.srcKd
			tup = srcKey
		} else {
			// value tuple, adjust offset
			tup = srcVal
			desc = m.srcVd
			from = from - m.split
		}

		if desc.Types[from].Enc == m.targetKb.Desc.Types[to].Enc {
			m.targetKb.PutRaw(to, desc.GetField(from, tup))
		} else {
			// TODO support GMS-side type conversions
			return nil, fmt.Errorf("invalid key type conversions should be rejected by lookupMapping.valid()")
		}
	}

	idxKey := m.targetKb.BuildPermissive(m.pool)
	return idxKey, nil
}
