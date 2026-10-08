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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// lookupJoinSource supplies lookup keys and assembles result rows. Keeping the
// source representation here lets SQL rows and storage tuples share the same
// lookup, filtering, and outer-join state machine.
type lookupJoinSource interface {
	// nextLookupKey advances to the next source row and encodes its destination
	// lookup key, retaining the source row for buildRow. The boolean is false
	// when no destination row can match. Returns io.EOF when the source is exhausted.
	nextLookupKey(*sql.Context) (val.Tuple, bool, error)

	// buildRow combines the current source row with the destination key and value
	// tuples in a new SQL row. Nil destination tuples produce a null-extended row.
	buildRow(*sql.Context, val.Tuple, val.Tuple) (sql.Row, error)

	// Close releases the source iterator's resources.
	Close(*sql.Context) error
}

type lookupJoinKvIter struct {
	src        lookupJoinSource
	srcLen     int
	dstIter    prolly.MapIter
	dstIterGen index.SecondaryLookupIterGen

	// TODO: we want to build KV-side static expression implementations
	// so that we can execute filters more efficiently
	srcFilter  sql.Expression
	dstFilter  sql.Expression
	joinFilter sql.Expression

	// LEFT_JOIN impl details
	isLeftJoin   bool
	excludeNulls bool
	returnedARow bool
}

var _ sql.RowIter = (*lookupJoinKvIter)(nil)

func (l *lookupJoinKvIter) Close(ctx *sql.Context) error {
	l.dstIter = nil
	return l.src.Close(ctx)
}

func newLookupKvIter(
	srcIter prolly.MapIter,
	targetIter index.SecondaryLookupIterGen,
	mapping *lookupMapping,
	joiner *prollyToSqlJoiner,
	srcFilter, dstFilter, joinFilter sql.Expression,
	isLeftJoin bool,
	excludeNulls bool,
) (*lookupJoinKvIter, error) {
	return &lookupJoinKvIter{
		src: &kvLookupJoinSource{
			iter:    srcIter,
			mapping: mapping,
			joiner:  joiner,
		},
		srcLen:       joiner.kvSplits[0],
		dstIterGen:   targetIter,
		srcFilter:    srcFilter,
		dstFilter:    dstFilter,
		joinFilter:   joinFilter,
		isLeftJoin:   isLeftJoin,
		excludeNulls: excludeNulls,
	}, nil
}

func (l *lookupJoinKvIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		// (1) initialize secondary iter if does not exist yet
		// (2) read from secondary until EOF
		// (3) concat, convert, filter primary/secondary rows
		if l.dstIter == nil {
			// if secondary iterator does not exist:
			//   (1) read the next source row or KV pair
			//   (2) map it into destination key form
			//   (3) initialize secondary iterator with that key
			l.returnedARow = false
			key, canMatch, err := l.src.nextLookupKey(ctx)
			if err != nil {
				return nil, err
			}

			// Skip the lookup when no right row can match this key.
			if canMatch {
				l.dstIter, err = l.dstIterGen.New(ctx, key)
				if err != nil {
					return nil, err
				}
			}
		}

		var dstKey, dstVal val.Tuple
		if l.dstIter != nil {
			var err error
			dstKey, dstVal, err = l.dstIter.Next(ctx)
			if err != nil && err != io.EOF {
				return nil, err
			}
		}

		if dstKey == nil {
			l.dstIter = nil
			if !l.isLeftJoin || l.returnedARow {
				continue
			}
		}

		ret, err := l.src.buildRow(ctx, dstKey, dstVal)
		if err != nil {
			return nil, err
		}

		// side-specific filters are currently hoisted
		if l.srcFilter != nil {
			res, err := sql.EvaluateCondition(ctx, l.srcFilter, ret[:l.srcLen])
			if err != nil {
				return nil, err
			}

			if !sql.IsTrue(res) {
				continue
			}
		}

		// A right-side filter must not reject a null-extended outer row.
		if l.dstFilter != nil && dstKey != nil {
			res, err := sql.EvaluateCondition(ctx, l.dstFilter, ret[l.srcLen:])
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
				continue
			}

			if !sql.IsTrue(res) && dstKey != nil {
				continue
			}
		}

		l.returnedARow = true
		return ret, nil
	}
}

type kvLookupJoinSource struct {
	iter prolly.MapIter
	// mapping inputs (key, value) to create a destination key
	mapping *lookupMapping
	// projections
	joiner *prollyToSqlJoiner
	key    val.Tuple
	value  val.Tuple
}

func (s *kvLookupJoinSource) nextLookupKey(ctx *sql.Context) (val.Tuple, bool, error) {
	var err error
	s.key, s.value, err = s.iter.Next(ctx)
	if err != nil {
		return nil, false, err
	}

	if s.key == nil {
		return nil, false, io.EOF
	}

	key, err := s.mapping.dstKeyTuple(ctx, s.key, s.value)
	return key, true, err
}

func (s *kvLookupJoinSource) buildRow(ctx *sql.Context, key, value val.Tuple) (sql.Row, error) {
	return s.joiner.buildRow(ctx, s.key, s.value, key, value)
}

func (s *kvLookupJoinSource) Close(*sql.Context) error {
	return nil
}

// lookupMapping is responsible for generating keys for lookups into
// the destination iterator.
type lookupMapping struct {
	ns         tree.NodeStore
	pool       pool.BuffPool
	targetKb   *val.TupleBuilder
	litKd      *val.TupleDesc
	srcKd      *val.TupleDesc
	srcVd      *val.TupleDesc
	srcMapping val.OrdinalMapping
	// litTuple are the statically provided literal expressions in the key expression
	litTuple   val.Tuple
	split      int
	keyExprs   []sql.Expression
	idxColTyps []sql.ColumnExpressionType
}

func newLookupKeyMapping(
	ctx *sql.Context,
	sourceSch schema.Schema,
	tgtKeyDesc *val.TupleDesc,
	keyExprs []sql.Expression,
	typs []sql.ColumnExpressionType,
	ns tree.NodeStore,
) (*lookupMapping, error) {
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
	tda := val.TupleDescriptorArgs{ValueStore: ns}

	for i, e := range keyExprs {
		switch e := e.(type) {
		case *expression.GetField:
			// map the schema order index to the physical storage index
			col, ok := sourceSch.GetAllCols().LowerNameToCol[strings.ToLower(e.Name())]
			if !ok {
				return nil, fmt.Errorf("failed to build lookup mapping, column missing from schema: %s", e.Name())
			}
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
			tgtTyp := tgtKeyDesc.Types[i]
			litTypes = append(litTypes, tgtTyp)
			tda.Handlers = append(tda.Handlers, tgtKeyDesc.Handlers[i])
		}
	}

	litDesc := val.NewTupleDescriptorWithArgs(tda, litTypes...)
	litTb := val.NewTupleBuilder(litDesc, ns)
	for i, j := range litMappings {
		colTyp := typs[j]
		value, inRange, err := convertLiteralKeyValue(ctx, colTyp, keyExprs[j].(*expression.Literal))
		if err != nil && !sql.ErrTruncatedIncorrect.Is(err) {
			return nil, err
		}
		if inRange != sql.InRange {
			return nil, nil
		}

		if err := tree.PutField(ctx, ns, litTb, i, value); err != nil {
			return nil, err
		}
	}

	var litTuple val.Tuple
	var err error
	if litDesc.Count() > 0 {
		litTuple, err = litTb.Build(ctx, ns.Pool())
		if err != nil {
			return nil, err
		}
	}

	return &lookupMapping{
		split:      split,
		srcMapping: srcMapping,
		litTuple:   litTuple,
		litKd:      litDesc,
		srcKd:      sourceSch.GetKeyDescriptor(ns),
		srcVd:      sourceSch.GetValueDescriptor(ns),
		targetKb:   val.NewTupleBuilder(tgtKeyDesc, ns),
		ns:         ns,
		pool:       ns.Pool(),
		keyExprs:   keyExprs,
		idxColTyps: typs,
	}, nil
}

// convertLiteralKeyValue converts a literal expression value to the appropriate type for the reference column
// in a key lookup
func convertLiteralKeyValue(ctx *sql.Context, colTyp sql.ColumnExpressionType, literal *expression.Literal) (any, sql.ConvertInRange, error) {
	srcType := literal.Type(ctx)
	destType := colTyp.Type

	// For extended types, use the rich type conversion methods
	if srcEt, ok := srcType.(sql.ExtendedType); ok {
		if destEt, ok := destType.(sql.ExtendedType); ok {
			return destEt.ConvertToType(ctx, srcEt, literal.Value(), 'a')
		}
	}
	return destType.Convert(ctx, literal.Value())
}

// valid returns whether the source and destination key types
// are type compatible
func (m *lookupMapping) valid(ctx *sql.Context) bool {
	if m == nil {
		return false
	}
	var litIdx int
	for to := range m.srcMapping {
		from := m.srcMapping.MapOrdinal(to)
		var desc *val.TupleDesc
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

		// The extended encoding types don't provide us enough information to know if the types are actually
		// byte-compatible for these lookups, so we need to dig deeper.
		switch desc.Types[from].Enc {
		case val.ExtendedAddrEnc, val.ExtendedEnc, val.ExtendedAdaptiveEnc:
			toTyp := m.idxColTyps[to].Type
			fromTyp := m.keyExprs[to].Type(ctx)
			// this is more conservative than it needs to be, we want to assert these values are byte-compatible
			if toTyp != fromTyp {
				return false
			}
		}
	}
	return true
}

func (m *lookupMapping) dstKeyTuple(ctx context.Context, srcKey, srcVal val.Tuple) (val.Tuple, error) {
	var litIdx int
	for to := range m.srcMapping {
		from := m.srcMapping.MapOrdinal(to)
		var tup val.Tuple
		var desc *val.TupleDesc
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

	return m.targetKb.BuildPermissive(ctx, m.pool)
}
