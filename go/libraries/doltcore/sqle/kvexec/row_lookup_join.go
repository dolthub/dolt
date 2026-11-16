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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// newRowLookupKvIter returns a lookup join iterator for joins whose right side is a Dolt index lookup but whose
// left side is some GMS-native plan shape (e.g. a subquery, a CTE, etc.)
// A nil result signals to the builder to fall back to the vanilla row_exec path
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
		return nil, nil
	}

	if n.ScopeLen != 0 {
		// we don't have the logic here to account for outer scope field index adjustments
		return nil, nil
	}

	idx := ita.Index()
	if idx == nil || idx.IsSpatial() || idx.IsFullText() || idx.IsVector() {
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
		return nil, nil
	}

	keyTypes := idx.ColumnExpressionTypes(ctx)
	keyDesc := dstIterGen.InputKeyDesc()
	if len(keyTypes) < len(keyExprs) || keyDesc.Count() < len(keyExprs) {
		return nil, nil
	}
	comparisons := make([]sql.Expression, len(keyExprs))
	for i, e := range keyExprs {
		// unsupported key encodings return nil, which signals the row_exec builder to use the vanilla building path
		if !lookupKeyEncodingSupported(keyDesc.Types[i].Enc) {
			return nil, nil
		}

		if gf, ok := e.(*expression.GetField); ok && (gf.Index() < 0 || gf.Index() >= srcLen) {
			return nil, nil
		}

		_, srcExtended := e.Type(ctx).(sql.ExtendedType)
		_, dstExtended := keyTypes[i].Type.(sql.ExtendedType)
		if srcExtended != dstExtended {
			return nil, nil
		}

		if !srcExtended && !e.Type(ctx).Equals(keyTypes[i].Type) {
			comparisons[i] = expression.NewEquals(
				expression.NewGetField(0, e.Type(ctx), "source", true),
				expression.NewGetField(1, keyTypes[i].Type, "key", true),
			)
		}
	}

	ns := dstIterGen.NodeStore()
	srcIter, err := b.fallback.Build(ctx, n.Left(), parentRow)
	if err != nil {
		return nil, err
	}

	return &lookupJoinKvIter{
		src: &rowLookupJoinSource{
			iter:   srcIter,
			srcLen: srcLen,
			mapping: &rowLookupMapping{
				ns:            ns,
				pool:          ns.Pool(),
				targetKb:      val.NewTupleBuilder(keyDesc, ns),
				keyExprs:      keyExprs,
				keyTypes:      keyTypes,
				nullSafe:      ita.NullMask(),
				comparisons:   comparisons,
				comparisonRow: make(sql.Row, 2),
			},
			joiner: newRowJoiner(ctx, []schema.Schema{dstIterGen.Schema()}, nil, dstTags, ns),
		},
		srcLen:       srcLen,
		dstIterGen:   dstIterGen,
		dstFilter:    dstFilter,
		joinFilter:   n.Filter,
		isLeftJoin:   n.Op.IsLeftOuter(),
		excludeNulls: n.Op.IsExcludeNulls(),
	}, nil
}

// rowLookupJoinSource keeps the left side as SQL rows and decodes only the
// right side's storage tuples.
type rowLookupJoinSource struct {
	iter sql.RowIter
	// srcLen is the width of the left node schema
	srcLen int
	row    sql.Row
	// mapping is responsible for generating lookup keys from the left row
	mapping *rowLookupMapping
	// joiner decodes the KV pairs read from the right side
	joiner *prollyToSqlJoiner
}

func (s *rowLookupJoinSource) nextLookupKey(ctx *sql.Context) (val.Tuple, bool, error) {
	row, err := s.iter.Next(ctx)
	if err != nil {
		return nil, false, err
	}

	// the left iter begins with rows from the outer scope; strip those away
	s.row = row[len(row)-s.srcLen:]
	return s.mapping.dstKeyTuple(ctx, s.row)
}

func (s *rowLookupJoinSource) buildRow(ctx *sql.Context, key, value val.Tuple) (sql.Row, error) {
	row := make(sql.Row, s.srcLen+s.joiner.outCnt)
	copy(row, s.row)
	if key != nil {
		if err := s.joiner.buildRowInto(ctx, row[s.srcLen:], key, value); err != nil {
			return nil, err
		}
	}

	return row, nil
}

func (s *rowLookupJoinSource) Close(ctx *sql.Context) error {
	if s.iter == nil {
		return nil
	}

	iter := s.iter
	s.iter = nil
	return iter.Close(ctx)
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
	nullSafe      []bool
	comparisons   []sql.Expression
	comparisonRow sql.Row
}

// dstKeyTuple encodes the destination lookup key for |row|. The boolean return is false when no destination row
// can match the key, either because a key value is NULL under a regular equality comparison or because it falls outside
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
		original := v
		v, inRange, err := convertLookupKeyValue(ctx, e.Type(ctx), m.keyTypes[i].Type, v)
		if inRange != sql.InRange {
			m.targetKb.Recycle()
			return nil, false, nil
		}

		if err != nil {
			m.targetKb.Recycle()
			return nil, false, err
		}

		// Index matching may remove the equality from the join filter, so a
		// rounded or truncated lookup key must still satisfy the comparison.
		if cmp := m.comparisons[i]; cmp != nil {
			m.comparisonRow[0], m.comparisonRow[1] = original, v
			result, err := cmp.Eval(ctx, m.comparisonRow)
			if err != nil || !sql.IsTrue(result) {
				m.targetKb.Recycle()
				return nil, false, err
			}
		} else if src, ok := e.Type(ctx).(sql.ExtendedType); ok && !src.Equals(m.keyTypes[i].Type) {
			equal, err := extendedLookupKeyMatches(ctx, src, m.keyTypes[i].Type.(sql.ExtendedType), original, v)
			if err != nil || !equal {
				m.targetKb.Recycle()
				return nil, false, err
			}
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

// extendedLookupKeyMatches compares in the source representation, since extended
// types can use incompatible Go values (for example, decimals and integers).
func extendedLookupKeyMatches(ctx *sql.Context, src, dst sql.ExtendedType, original, converted interface{}) (bool, error) {
	if types.IsFloat(dst) && !types.IsFloat(src) {
		// Comparisons against approximate values widen integers to floating point.
		return true, nil
	}

	restored, status, err := src.ConvertToType(ctx, dst, converted, 'a')
	if err != nil || status != sql.InRange {
		return false, err
	}

	cmp, err := src.Compare(ctx, original, restored)
	return cmp == 0, err
}

// convertLookupKeyValue converts a SQL value from the source type to the destination type, returning the
// converted value, whether it is in range, and any error encountered.
func convertLookupKeyValue(ctx *sql.Context, srcTyp, colTyp sql.Type, v interface{}) (interface{}, sql.ConvertInRange, error) {
	if src, ok := srcTyp.(sql.ExtendedType); ok {
		if dst, ok := colTyp.(sql.ExtendedType); ok {
			return dst.ConvertToType(ctx, src, v, 'a')
		}
	}

	// ENUM and SET types are represented in memory as integer ordinals for their label values. Values are
	// equivalent if they have the same label, but the ordinals may differ between source and destination types.
	// Therefore, we convert them to their string representation first, such that a value of e.g. `green` compares
	// the same on both sides of the join, even if the source and destination types have different label orders.
	if types.IsEnum(srcTyp) || types.IsSet(srcTyp) {
		var err error
		v, _, err = types.ConvertToCollatedString(ctx, v, srcTyp)
		if err != nil {
			return nil, sql.InRange, err
		}
	}

	v, inRange, err := colTyp.Convert(ctx, v)
	if types.ErrLengthBeyondLimit.Is(err) || types.ErrConvertToDecimalLimit.Is(err) {
		return nil, sql.Overflow, nil
	}

	if err != nil && sql.ErrTruncatedIncorrect.Is(err) {
		// A truncated value can still be used as a lookup key if it matches the destination type
		err = nil
	}

	return v, inRange, err
}

// lookupKeyEncodingSupported returns whether a SQL value converted to an index
// column type can be written into a key field with |enc|.
func lookupKeyEncodingSupported(enc val.Encoding) bool {
	switch enc {
	case val.Int8Enc, val.Uint8Enc, val.Int16Enc, val.Uint16Enc,
		val.Int32Enc, val.Uint32Enc, val.Int64Enc, val.Uint64Enc,
		val.Float32Enc, val.Float64Enc, val.Bit64Enc, val.DecimalEnc,
		val.YearEnc, val.DateEnc, val.TimeEnc, val.DatetimeEnc,
		val.EnumEnc, val.SetEnc, val.StringEnc, val.ByteStringEnc,
		val.StringAdaptiveEnc, val.BytesAdaptiveEnc, val.JsonAdaptiveEnc,
		val.ExtendedEnc, val.ExtendedAddrEnc, val.ExtendedAdaptiveEnc:
		return true
	default:
		return false
	}
}
