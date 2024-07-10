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

package rowexec

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/pool"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// internal iterators are strictly |prolly.MapIter|

// todo read a batch of keys from source map, sort before performing lookup

// todo batched version runs a fixed set of workers on kv ranges in parallel
// need filters and projections in this version for it to be worth it

func rowIterTableLookupJoin(
	ctx *sql.Context,
	srcIter prolly.MapIter,
	dstIter index.SecondaryLookupIter,
	srcMap prolly.Map,
	srcSch, dstSch schema.Schema,
	srcProj, dstProj []uint64,
	keyExprs []sql.Expression,
	srcFilter, dstFilter, joinFilter sql.Expression,
	isLeftJoin bool,
) (sql.RowIter, error) {
	split := len(srcProj)

	projections := append(srcProj, dstProj...)

	rowJoiner := newRowJoiner(srcSch, dstIter.Schema(), dstIter.NodeStore(), split, projections)

	return newLookupKvIter(ctx, srcIter, dstIter, srcMap, keyExprs, srcSch, rowJoiner, srcFilter, dstFilter, joinFilter, isLeftJoin)
}

type lookupJoinKvIter struct {
	// srcIter is left relation
	srcIter prolly.MapIter
	srcKey  val.Tuple
	srcVal  val.Tuple

	dstIter index.SecondaryLookupIter
	dstKey  val.Tuple

	dstKb *val.TupleBuilder

	// keyTupleMapper inputs (srcKey, srcVal) to create a dstKey
	keyTupleMapper *lookupMapping

	// projections
	joiner *prollyToSqlJoiner

	// TODO convert sql.Expression to static prolly expressions
	// that can be pushed.
	srcFilter  sql.Expression
	dstFilter  sql.Expression
	joinFilter sql.Expression
	isLeftJoin bool
}

func (l *lookupJoinKvIter) Close(_ *sql.Context) error {
	return nil
}

var _ sql.RowIter = (*lookupJoinKvIter)(nil)

func newLookupKvIter(
	ctx context.Context,
	srcIter prolly.MapIter,
	targetIter index.SecondaryLookupIter,
	source prolly.Map,
	keyExprs []sql.Expression,
	sourceSch schema.Schema,
	joiner *prollyToSqlJoiner,
	srcFilter, dstFilter, joinFilter sql.Expression,
	isLeftJoin bool,
) (*lookupJoinKvIter, error) {
	if lit, ok := joinFilter.(*expression.Literal); ok {
		if lit.Value() == true {
			joinFilter = nil
		}
	}

	keyLookupMapper := newLookupKeyMapping(ctx, sourceSch, source, targetIter.InputKeyDesc(), keyExprs)

	return &lookupJoinKvIter{
		srcIter:        srcIter,
		dstIter:        targetIter,
		joiner:         joiner,
		keyTupleMapper: keyLookupMapper,
		srcFilter:      srcFilter,
		dstFilter:      dstFilter,
		joinFilter:     joinFilter,
		isLeftJoin:     isLeftJoin,
	}, nil
}

// lookupMapping is responsible for generating keys for lookups into
// the destination iterator.
type lookupMapping struct {
	split      int
	srcMapping val.OrdinalMapping

	// litTuple are the statically provided literal expressions in the key expression
	litTuple    val.Tuple
	litMappings val.OrdinalMapping

	litKd    val.TupleDesc
	srcKd    val.TupleDesc
	srcVd    val.TupleDesc
	targetKb *val.TupleBuilder

	ns   tree.NodeStore
	pool pool.BuffPool
}

func newLookupKeyMapping(ctx context.Context, sourceSch schema.Schema, src prolly.Map, tgtKeyDesc val.TupleDesc, keyExprs []sql.Expression) *lookupMapping {
	keyless := schema.IsKeyless(sourceSch)
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
			j := e.Index()
			col := sourceSch.GetAllCols().GetColumns()[j]
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
		split:       split,
		srcMapping:  srcMapping,
		litTuple:    litTuple,
		litMappings: litMappings,
		litKd:       litDesc,
		srcKd:       src.KeyDesc(),
		srcVd:       src.ValDesc(),
		targetKb:    val.NewTupleBuilder(tgtKeyDesc),
		ns:          src.NodeStore(),
		pool:        src.Pool(),
	}
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
			value, err := tree.GetField(ctx, desc, from, tup, m.ns)
			if err != nil {
				return nil, err
			}

			err = tree.PutField(ctx, m.ns, m.targetKb, to, value)
			if err != nil {
				return nil, err
			}
		}
	}

	idxKey := m.targetKb.BuildPermissive(m.pool)
	return idxKey, nil
}

func (l *lookupJoinKvIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		var err error
		var newIter bool
		if l.dstKey == nil {
			newIter = true
			// get row from |src|
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

			if err := l.dstIter.New(ctx, l.dstKey); err != nil {
				return nil, err
			}
		}

		dstKey, dstVal, ok, err := l.dstIter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			l.dstKey = nil
			if !(newIter && l.isLeftJoin) {
				continue
			}
		}

		ret, err := l.joiner.buildRow(ctx, l.srcKey, l.srcVal, dstKey, dstVal)
		if err != nil {
			return nil, err
		}

		// side-specific filters are currently hoisted

		if l.srcFilter != nil {
			res, err := sql.EvaluateCondition(ctx, l.srcFilter, ret[:l.joiner.srcSplit])
			if err != nil {
				return nil, err
			}

			if !sql.IsTrue(res) {
				continue
			}

		}
		if l.dstFilter != nil {
			res, err := sql.EvaluateCondition(ctx, l.dstFilter, ret[l.joiner.srcSplit:])
			if err != nil {
				return nil, err
			}

			if !sql.IsTrue(res) {
				if !(newIter && l.isLeftJoin) {
					continue
				}
			}
		}
		if l.joinFilter != nil {
			res, err := sql.EvaluateCondition(ctx, l.joinFilter, ret)
			if err != nil {
				return nil, err
			}

			if !sql.IsTrue(res) {
				if !(newIter && l.isLeftJoin) {
					continue
				}
			}
		}
		return ret, nil
	}
}
