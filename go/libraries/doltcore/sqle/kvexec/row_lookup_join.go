// Copyright 2026 Dolthub, Inc.
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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// newRowLookupKvIter returns a lookup join iterator for joins whose right side
// is a Dolt index lookup but whose left side is not a KV source: a CTE, a
// derived table, an aggregation, or another join. GMS executes that shape by
// building a fresh row iterator over the right side for every left row, which
// for a single row index lookup costs an index scan builder, a sql.Range to
// prolly.Range conversion, a partition iterator and a table row iterator per
// row. Here the left side stays rows while the right side is read as KV pairs,
// so only the lookup key is rebuilt per row.
//
// Returns a nil iterator and nil error when the shape is unsupported, which
// sends the join back to GMS.
func (b *Builder) newRowLookupKvIter(
	ctx *sql.Context,
	n *plan.JoinNode,
	parentRow sql.Row,
	ita *plan.IndexedTableAccess,
	dstIterGen index.SecondaryLookupIterGen,
	dstTags []uint64,
	dstFilter sql.Expression,
) (sql.RowIter, error) {
	if b.fallback == nil {
		// without a fallback builder we have no way to execute the left side
		return nil, nil
	}
	if n.ScopeLen != 0 {
		// the left row would carry outer scope columns we do not account for
		return nil, nil
	}

	idx := ita.Index()
	if idx == nil || idx.IsSpatial() || idx.IsFullText() || idx.IsVector() {
		// these indexes are not keyed by the join columns
		return nil, nil
	}

	keyExprs := ita.Expressions()
	if len(keyExprs) == 0 {
		// a static lookup, nothing to rebuild per row
		return nil, nil
	}

	srcLen := len(n.Left().Schema(ctx))
	dstLen := len(n.Right().Schema(ctx))
	if srcLen == 0 || len(dstTags) != dstLen {
		// the joiner projections have to line up with the join schema
		return nil, nil
	}

	keyTypes := idx.ColumnExpressionTypes(ctx)
	keyDesc := dstIterGen.InputKeyDesc()
	if len(keyTypes) < len(keyExprs) || keyDesc.Count() < len(keyExprs) {
		return nil, nil
	}
	for i, e := range keyExprs {
		if !lookupKeyEncodingSupported(keyDesc.Types[i].Enc) {
			return nil, nil
		}
		if gf, ok := e.(*expression.GetField); ok && (gf.Index() < 0 || gf.Index() >= srcLen) {
			// key columns have to resolve inside the left row
			return nil, nil
		}
	}

	joinFilter := n.Filter
	if lit, ok := joinFilter.(*expression.Literal); ok && lit.Value() == true {
		joinFilter = nil
	}

	ns := dstIterGen.NodeStore()
	srcIter, err := b.fallback.Build(ctx, n.Left(), parentRow)
	if err != nil {
		return nil, err
	}

	return &rowLookupJoinKvIter{
		srcIter:    srcIter,
		srcLen:     srcLen,
		dstIterGen: dstIterGen,
		keyTupleMapper: &rowLookupMapping{
			ns:       ns,
			pool:     ns.Pool(),
			targetKb: val.NewTupleBuilder(keyDesc, ns),
			keyExprs: keyExprs,
			keyTypes: keyTypes,
			nullSafe: ita.NullMask(),
		},
		joiner:     newRowJoiner(ctx, []schema.Schema{dstIterGen.Schema()}, nil, dstTags, ns),
		fullRow:    make(sql.Row, srcLen+dstLen),
		dstFilter:  dstFilter,
		joinFilter: joinFilter,
		isLeftJoin: n.Op.IsLeftOuter(),
	}, nil
}

// rowLookupJoinKvIter joins a SQL row source on the left to KV index lookups on
// the right.
type rowLookupJoinKvIter struct {
	// TODO: we want to build KV-side static expression implementations
	// so that we can execute filters more efficiently
	srcIter sql.RowIter
	// srcLen is the width of the left node schema
	srcLen int

	dstIter    prolly.MapIter
	dstIterGen index.SecondaryLookupIterGen

	// keyTupleMapper encodes a dstKey from the left row
	keyTupleMapper *rowLookupMapping

	// joiner decodes the KV pairs read from the right side
	joiner *prollyToSqlJoiner

	dstFilter  sql.Expression
	joinFilter sql.Expression

	// fullRow is the current left row followed by the current right row. It is
	// reused across iterations and copied into every returned row.
	fullRow sql.Row

	// LEFT_JOIN impl details
	isLeftJoin   bool
	returnedARow bool
}

var _ sql.RowIter = (*rowLookupJoinKvIter)(nil)

func (l *rowLookupJoinKvIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		// (1) initialize secondary iter if does not exist yet
		// (2) read from secondary until EOF
		// (3) convert and filter the secondary row, then concat
		if l.dstIter == nil {
			// if secondary iterator does not exist:
			//   (1) read the next row from the left iterator
			//   (2) encode the lookup key from its values
			//   (3) initialize secondary iterator with that key
			l.returnedARow = false

			srcRow, err := l.srcIter.Next(ctx)
			if err != nil {
				return nil, err
			}
			// the left iter begins with rows from the outer scope; strip those away
			copy(l.fullRow, srcRow[len(srcRow)-l.srcLen:])
			l.nullifyDst()

			dstKey, canMatch, err := l.keyTupleMapper.dstKeyTuple(ctx, l.fullRow)
			if err != nil {
				return nil, err
			}
			if !canMatch {
				// no right row can match this key, so skip the lookup
				if l.isLeftJoin {
					return l.resultRow(), nil
				}
				continue
			}

			l.dstIter, err = l.dstIterGen.New(ctx, dstKey)
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
			if !l.isLeftJoin || l.returnedARow {
				continue
			}
			l.nullifyDst()
		} else if err := l.joiner.buildRowInto(ctx, l.fullRow[l.srcLen:], dstKey, dstVal); err != nil {
			return nil, err
		}

		// side-specific filters are currently hoisted
		if l.dstFilter != nil && dstKey != nil {
			res, err := sql.EvaluateCondition(ctx, l.dstFilter, l.fullRow[l.srcLen:])
			if err != nil {
				return nil, err
			}
			if !sql.IsTrue(res) {
				continue
			}
		}
		if l.joinFilter != nil {
			res, err := sql.EvaluateCondition(ctx, l.joinFilter, l.fullRow)
			if err != nil {
				return nil, err
			}
			if !sql.IsTrue(res) && dstKey != nil {
				continue
			}
		}
		l.returnedARow = true
		return l.resultRow(), nil
	}
}

// nullifyDst clears the right half of |fullRow| so a row left over from an
// earlier match cannot leak into a null extended row.
func (l *rowLookupJoinKvIter) nullifyDst() {
	clear(l.fullRow[l.srcLen:])
}

func (l *rowLookupJoinKvIter) resultRow() sql.Row {
	ret := make(sql.Row, len(l.fullRow))
	copy(ret, l.fullRow)
	return ret
}

func (l *rowLookupJoinKvIter) Close(ctx *sql.Context) error {
	l.dstIter = nil
	if l.srcIter == nil {
		return nil
	}
	srcIter := l.srcIter
	l.srcIter = nil
	return srcIter.Close(ctx)
}

// rowLookupMapping is responsible for generating keys for lookups into the
// destination iterator from SQL rows. It is the row source analogue of
// lookupMapping, which copies key fields between storage tuples.
type rowLookupMapping struct {
	ns       tree.NodeStore
	pool     pool.BuffPool
	targetKb *val.TupleBuilder
	keyExprs []sql.Expression
	keyTypes []sql.ColumnExpressionType
	// nullSafe marks the key columns compared with the null safe equality
	// operator, which is the only way a NULL key matches anything
	nullSafe []bool
}

// dstKeyTuple encodes the destination lookup key for |row|. The boolean return
// is false when no destination row can match the key, either because a key
// value is NULL under a regular equality comparison or because it falls outside
// the range of its index column.
func (m *rowLookupMapping) dstKeyTuple(ctx *sql.Context, row sql.Row) (val.Tuple, bool, error) {
	for i, e := range m.keyExprs {
		v, err := e.Eval(ctx, row)
		if err != nil {
			m.targetKb.Recycle()
			return nil, false, err
		}
		if v == nil {
			if i >= len(m.nullSafe) || !m.nullSafe[i] {
				m.targetKb.Recycle()
				return nil, false, nil
			}
			// a null safe comparison matches NULL, and an unwritten field
			// encodes as NULL
			continue
		}
		v, inRange, err := convertLookupKeyValue(ctx, m.keyTypes[i].Type, v)
		if err != nil {
			m.targetKb.Recycle()
			return nil, false, err
		}
		if inRange != sql.InRange {
			m.targetKb.Recycle()
			return nil, false, nil
		}
		if err = tree.PutField(ctx, m.ns, m.targetKb, i, v); err != nil {
			m.targetKb.Recycle()
			return nil, false, err
		}
	}
	// the key can be a prefix of the index, BuildPermissive leaves the
	// remaining fields NULL
	tup, err := m.targetKb.BuildPermissive(ctx, m.pool)
	if err != nil {
		return nil, false, err
	}
	return tup, true, nil
}

// convertLookupKeyValue converts a key value to the type of the index column it
// is compared against. It mirrors the conversion GMS performs when building an
// index lookup from a row, so both execution paths agree on which rows a key
// matches.
func convertLookupKeyValue(ctx *sql.Context, colTyp sql.Type, v interface{}) (interface{}, sql.ConvertInRange, error) {
	v, inRange, err := colTyp.Convert(ctx, v)
	if err != nil && sql.ErrTruncatedIncorrect.Is(err) {
		// for this purpose, truncation errors are acceptable and we only look
		// at the in-range status
		err = nil
	}
	return v, inRange, err
}

// lookupKeyEncodingSupported returns whether a SQL value converted to an index
// column type can be written into a key field with |enc|. The encodings left
// out either address content stored outside the tuple or accept a narrower Go
// type than type conversion produces, so joins on those columns stay on the
// GMS path.
func lookupKeyEncodingSupported(enc val.Encoding) bool {
	switch enc {
	case val.Int8Enc, val.Uint8Enc, val.Int16Enc, val.Uint16Enc,
		val.Int32Enc, val.Uint32Enc, val.Int64Enc, val.Uint64Enc,
		val.Float32Enc, val.Float64Enc, val.Bit64Enc, val.DecimalEnc,
		val.YearEnc, val.DateEnc, val.TimeEnc, val.DatetimeEnc,
		val.EnumEnc, val.SetEnc, val.StringEnc, val.ByteStringEnc:
		return true
	default:
		return false
	}
}
