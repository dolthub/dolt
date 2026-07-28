// Copyright 2022 Dolthub, Inc.
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

package writer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func getPrimaryProllyWriter(ctx context.Context, t *doltdb.Table, schState *dsess.WriterState) (prollyIndexWriter, error) {
	idx, err := t.GetRowDataWithDescriptors(ctx, schState.PkKeyDesc, schState.PkValDesc)
	if err != nil {
		return prollyIndexWriter{}, err
	}

	m, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return prollyIndexWriter{}, err
	}

	keyDesc, valDesc := m.Descriptors()

	adaptiveEncodingMaxRowSize := schState.DoltSchema.GetTargetRowSize()

	return prollyIndexWriter{
		mut:    m.Mutate(),
		keyBld: val.NewTupleBuilder(keyDesc, m.NodeStore()).WithMaxRowSize(adaptiveEncodingMaxRowSize),
		keyMap: schState.PriIndex.KeyMapping,
		valBld: val.NewTupleBuilder(valDesc, m.NodeStore()).WithMaxRowSize(adaptiveEncodingMaxRowSize),
		valMap: schState.PriIndex.ValMapping,
		key:    make(sql.Row, keyDesc.Count()),
	}, nil
}

func getPrimaryKeylessProllyWriter(ctx context.Context, t *doltdb.Table, schState *dsess.WriterState) (prollyKeylessWriter, error) {
	idx, err := t.GetRowData(ctx)
	if err != nil {
		return prollyKeylessWriter{}, err
	}

	m, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return prollyKeylessWriter{}, err
	}

	keyDesc, valDesc := m.Descriptors()

	targetRowSize := schState.DoltSchema.GetTargetRowSize()

	return prollyKeylessWriter{
		mut:    m.Mutate(),
		keyBld: val.NewTupleBuilder(keyDesc, m.NodeStore()).WithMaxRowSize(targetRowSize),
		valBld: val.NewTupleBuilder(valDesc, m.NodeStore()).WithMaxRowSize(targetRowSize),
		valMap: schState.PriIndex.ValMapping,
	}, nil
}

type indexWriter interface {
	Name() string
	Map(ctx context.Context) (prolly.MapInterface, error)
	ValidateKeyViolations(ctx context.Context, sqlRow sql.Row) error
	Insert(ctx context.Context, sqlRow sql.Row) error
	Delete(ctx context.Context, sqlRow sql.Row) error
	Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) error
	Commit(ctx context.Context) error
	Discard(ctx context.Context) error
	HasEdits(ctx context.Context) bool
	IterRange(ctx context.Context, rng prolly.Range) (prolly.MapIter, error)
	VisitGCRoots(ctx context.Context, roots func(hash.Hash) bool) error
}

type primaryIndexErrBuilder interface {
	errForSecondaryUniqueKeyError(ctx context.Context, err secondaryUniqueKeyError) error
}

type prollyIndexWriter struct {
	mut *prolly.MutableMap

	keyBld *val.TupleBuilder
	keyMap val.OrdinalMapping

	valBld *val.TupleBuilder
	valMap val.OrdinalMapping

	// buffer to reduce memory allocations
	key sql.Row
}

var _ indexWriter = prollyIndexWriter{}
var _ primaryIndexErrBuilder = prollyIndexWriter{}

func (m prollyIndexWriter) Name() string {
	// primary indexes don't have a name
	return ""
}

func (m prollyIndexWriter) Map(ctx context.Context) (prolly.MapInterface, error) {
	return m.mut.Map(ctx)
}

func (m prollyIndexWriter) keyFromRow(ctx context.Context, sqlRow sql.Row) (val.Tuple, error) {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := tree.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, sqlRow[from]); err != nil {
			return nil, err
		}
	}
	return m.keyBld.BuildPermissive(ctx, sharePool)
}

func (m prollyIndexWriter) ValidateKeyViolations(ctx context.Context, sqlRow sql.Row) error {
	k, err := m.keyFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}

	ok, err := m.mut.Has(ctx, k)
	if err != nil {
		return err
	} else if ok {
		for to := range m.keyMap {
			from := m.keyMap.MapOrdinal(to)
			m.key[to] = sqlRow[from]
		}
		keyStr := FormatKeyForUniqKeyErr(ctx, k, m.keyBld.Desc, m.key)
		return m.uniqueKeyError(ctx, keyStr, k, true)
	}
	return nil
}

func (m prollyIndexWriter) Insert(ctx context.Context, sqlRow sql.Row) error {
	k, err := m.keyFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}

	for to := range m.valMap {
		from := m.valMap.MapOrdinal(to)
		if err := tree.PutField(ctx, m.mut.NodeStore(), m.valBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	v, err := m.valBld.Build(ctx, sharePool)
	if err != nil {
		return err
	}

	return m.mut.Put(ctx, k, v)
}

func (m prollyIndexWriter) Delete(ctx context.Context, sqlRow sql.Row) error {
	k, err := m.keyFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}

	return m.mut.Delete(ctx, k)
}

func (m prollyIndexWriter) Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) error {
	oldKey, err := m.keyFromRow(ctx, oldRow)
	if err != nil {
		return err
	}

	// If the old row is empty, there is nothing to delete.
	// This can happen when updating a row in a conflict table if the row did not exist on one branch.
	if oldKey.Count() != 0 {
		// todo(andy): we can skip building, deleting |oldKey|
		//  if we know the key fields are unchanged
		if err := m.mut.Delete(ctx, oldKey); err != nil {
			return err
		}
	}

	newKey, err := m.keyFromRow(ctx, newRow)
	if err != nil {
		return err
	}

	ok, err := m.mut.Has(ctx, newKey)
	if err != nil {
		return err
	} else if ok {
		for to := range m.keyMap {
			from := m.keyMap.MapOrdinal(to)
			m.key[to] = newRow[from]
		}
		keyStr := FormatKeyForUniqKeyErr(ctx, newKey, m.keyBld.Desc, m.key)
		return m.uniqueKeyError(ctx, keyStr, newKey, true)
	}

	for to := range m.valMap {
		from := m.valMap.MapOrdinal(to)
		if err = tree.PutField(ctx, m.mut.NodeStore(), m.valBld, to, newRow[from]); err != nil {
			return err
		}
	}
	v, err := m.valBld.Build(ctx, sharePool)
	if err != nil {
		return err
	}

	return m.mut.Put(ctx, newKey, v)
}

func (m prollyIndexWriter) Commit(ctx context.Context) error {
	return m.mut.Checkpoint(ctx)
}

func (m prollyIndexWriter) VisitGCRoots(ctx context.Context, roots func(hash.Hash) bool) error {
	return m.mut.VisitGCRoots(ctx, roots)
}

func (m prollyIndexWriter) Discard(ctx context.Context) error {
	m.mut.Revert(ctx)
	return nil
}

func (m prollyIndexWriter) HasEdits(ctx context.Context) bool {
	return m.mut.HasEdits()
}

func (m prollyIndexWriter) IterRange(ctx context.Context, rng prolly.Range) (prolly.MapIter, error) {
	return m.mut.IterRange(ctx, rng)
}

func (m prollyIndexWriter) errForSecondaryUniqueKeyError(ctx context.Context, err secondaryUniqueKeyError) error {
	return m.uniqueKeyError(ctx, err.keyStr, err.existingKey, false)
}

// uniqueKeyError builds a sql.UniqueKeyError. It fetches the existing row using
// |key| and passes it as the |existing| row.
func (m prollyIndexWriter) uniqueKeyError(ctx context.Context, keyStr string, key val.Tuple, isPk bool) error {
	existing := make(sql.Row, len(m.keyMap)+len(m.valMap))

	_ = m.mut.Get(ctx, key, func(key, value val.Tuple) (err error) {
		kd := m.keyBld.Desc
		for from := range m.keyMap {
			to := m.keyMap.MapOrdinal(from)
			if existing[to], err = tree.GetField(ctx, kd, from, key, m.mut.NodeStore()); err != nil {
				return err
			}
		}

		vd := m.valBld.Desc
		for from := range m.valMap {
			to := m.valMap.MapOrdinal(from)
			if existing[to], err = tree.GetField(ctx, vd, from, value, m.mut.NodeStore()); err != nil {
				return err
			}
		}
		return
	})

	return sql.NewUniqueKeyErr(keyStr, isPk, existing)
}

type prollySecondaryIndexWriter struct {
	mut prolly.MutableMapInterface
	// pkBld builds key tuples for primary key index
	pkBld *val.TupleBuilder
	// keyBld builds key tuples for the secondary index
	keyBld *val.TupleBuilder

	name          string
	prefixLengths []uint16

	// pkMap is a mapping from secondary index keys to
	// primary key clustered index keys
	pkMap val.OrdinalMapping
	// keyMap is a mapping from sql.Row fields to
	// key fields of this secondary index
	keyMap val.OrdinalMapping
	// buffer to reduce memory allocations
	key sql.Row
	// number of indexed cols
	idxCols int
	unique  bool
	// predicate is set for partial indexes; rows not matching are excluded.
	predicate sql.Expression
	// virtualExprs is parallel to keyMap; non-nil entries are generating expressions for virtual
	// (unstored) generated columns that appear in this index's key. Nil overall if the index has
	// no virtual key parts (the common/fast-path case).
	virtualExprs []sql.Expression
}

// keyPartFromRow returns the value for key part |to|, given |sqlRow|. For most columns this is
// simply sqlRow[from]. For virtual generated columns, the value is computed by evaluating the
// generating expression against |sqlRow|.
func (m prollySecondaryIndexWriter) keyPartFromRow(ctx context.Context, to int, sqlRow sql.Row) (interface{}, error) {
	if m.virtualExprs != nil {
		if expr := m.virtualExprs[to]; expr != nil {
			sqlCtx, ok := ctx.(*sql.Context)
			if !ok {
				return nil, fmt.Errorf("expected *sql.Context for virtual column expression evaluation")
			}
			return expr.Eval(sqlCtx, sqlRow)
		}
	}
	from := m.keyMap.MapOrdinal(to)
	return sqlRow[from], nil
}

// matchesPredicate returns true if the row satisfies this index's predicate (or if the index has no predicate).
// Rows that do not match must be skipped for all index write operations.
func (m prollySecondaryIndexWriter) matchesPredicate(ctx context.Context, sqlRow sql.Row) (bool, error) {
	if m.predicate == nil {
		return true, nil
	}
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		return false, fmt.Errorf("expected *sql.Context for partial index predicate evaluation")
	}
	result, err := m.predicate.Eval(sqlCtx, sqlRow)
	return result.(bool), err
}

var _ indexWriter = prollySecondaryIndexWriter{}
var _ UniqueKeyChangeReporter = prollySecondaryIndexWriter{}

func (m prollySecondaryIndexWriter) Name() string {
	return m.name
}

func (m prollySecondaryIndexWriter) Map(ctx context.Context) (prolly.MapInterface, error) {
	return m.mut.MapInterface(ctx)
}

func (m prollySecondaryIndexWriter) ValidateKeyViolations(ctx context.Context, sqlRow sql.Row) error {
	matches, err := m.matchesPredicate(ctx, sqlRow)
	if err != nil {
		return err
	}
	if !matches {
		return nil
	}
	if m.unique {
		if err := m.checkForUniqueKeyErr(ctx, sqlRow); err != nil {
			return err
		}
	}
	return nil
}

// trimKeyPart will trim entry into the sql.Row depending on the prefixLengths
func (m prollySecondaryIndexWriter) trimKeyPart(ctx context.Context, to int, keyPart interface{}) (interface{}, error) {
	var prefixLength uint16
	if len(m.prefixLengths) > to {
		prefixLength = m.prefixLengths[to]
	}
	return val.TrimValueToPrefixLength(ctx, keyPart, prefixLength)
}

func (m prollySecondaryIndexWriter) keyFromRow(ctx context.Context, sqlRow sql.Row) (val.Tuple, error) {
	for to := range m.keyMap {
		v, err := m.keyPartFromRow(ctx, to, sqlRow)
		if err != nil {
			return nil, err
		}
		keyPart, _ := m.trimKeyPart(ctx, to, v)
		if err := tree.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, keyPart); err != nil {
			return nil, err
		}
	}
	return m.keyBld.Build(ctx, sharePool)
}

func (m prollySecondaryIndexWriter) VisitGCRoots(ctx context.Context, roots func(hash.Hash) bool) error {
	return m.mut.VisitGCRoots(ctx, roots)
}

func (m prollySecondaryIndexWriter) Insert(ctx context.Context, sqlRow sql.Row) error {
	matches, err := m.matchesPredicate(ctx, sqlRow)
	if err != nil {
		return err
	}
	if !matches {
		return nil
	}
	k, err := m.keyFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}
	return m.mut.Put(ctx, k, val.EmptyTuple)
}

func (m prollySecondaryIndexWriter) checkForUniqueKeyErr(ctx context.Context, sqlRow sql.Row) error {
	ns := m.mut.NodeStore()
	for to := range m.keyMap[:m.idxCols] {
		v, err := m.keyPartFromRow(ctx, to, sqlRow)
		if err != nil {
			return err
		}
		if v == nil {
			// NULL is incomparable and cannot
			// trigger a UNIQUE KEY violation
			m.keyBld.Recycle()
			return nil
		}
		keyPart, _ := m.trimKeyPart(ctx, to, v)
		if err := tree.PutField(ctx, ns, m.keyBld, to, keyPart); err != nil {
			return err
		}
	}

	// build a val.Tuple containing only fields for the unique column prefix
	key := m.keyBld.BuildPrefix(ns.Pool(), m.idxCols)
	desc := m.keyBld.Desc.PrefixDesc(m.idxCols)
	rng, err := prolly.PrefixRange(ctx, key, desc)
	if err != nil {
		return err
	}
	iter, err := m.mut.IterRange(ctx, rng)
	if err != nil {
		return err
	}

	idxKey, _, err := iter.Next(ctx)
	if err == io.EOF {
		return nil // no violation
	} else if err != nil {
		return err
	}

	// |prefix| collides with an existing key
	idxDesc := m.keyBld.Desc
	for to := range m.pkMap {
		from := m.pkMap.MapOrdinal(to)
		m.pkBld.PutRaw(to, idxDesc.GetField(from, idxKey))
	}
	existingPK, err := m.pkBld.Build(ctx, sharePool)
	if err != nil {
		return err
	}

	for to := range m.keyMap[:m.idxCols] {
		v, err := m.keyPartFromRow(ctx, to, sqlRow)
		if err != nil {
			return err
		}
		m.key[to], _ = m.trimKeyPart(ctx, to, v)
	}
	return secondaryUniqueKeyError{
		keyStr:      FormatKeyForUniqKeyErr(ctx, key, desc, m.key),
		existingKey: existingPK,
	}
}

func (m prollySecondaryIndexWriter) Delete(ctx context.Context, sqlRow sql.Row) error {
	matches, err := m.matchesPredicate(ctx, sqlRow)
	if err != nil {
		return err
	}
	if !matches {
		return nil
	}
	k, err := m.keyFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}
	return m.mut.Delete(ctx, k)
}

func isNoopUpdate(oldRow, newRow sql.Row, keyMap val.OrdinalMapping) bool {
	for to := range keyMap {
		from := keyMap.MapOrdinal(to)
		oldVal, newVal := oldRow[from], newRow[from]
		// []byte must use bytes.Equal; != panics on interface{} values holding slices.
		if oldBytes, ok := oldVal.([]byte); ok {
			newBytes, ok := newVal.([]byte)
			if !ok || !bytes.Equal(oldBytes, newBytes) {
				return false
			}
		} else if oldVal != newVal {
			return false
		}
	}
	return true
}

func (m prollySecondaryIndexWriter) Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) error {
	oldMatches, err := m.matchesPredicate(ctx, oldRow)
	if err != nil {
		return err
	}
	newMatches, err := m.matchesPredicate(ctx, newRow)
	if err != nil {
		return err
	}

	if !oldMatches && !newMatches {
		return nil
	}

	// If no indexed columns are modified and predicate status hasn't changed, no need to update.
	// isNoopUpdate compares the row's raw slots directly, which isn't safe for a virtual key part:
	// a stale/unpopulated slot could look identical between oldRow and newRow even though the real
	// underlying columns (and so the virtual column's computed value) changed.
	if oldMatches && newMatches && m.virtualExprs == nil && isNoopUpdate(oldRow, newRow, m.keyMap) {
		return nil
	}

	if oldMatches {
		oldKey, err := m.keyFromRow(ctx, oldRow)
		if err != nil {
			return err
		}
		if err := m.mut.Delete(ctx, oldKey); err != nil {
			return err
		}
	}

	if newMatches {
		if m.unique {
			if err := m.checkForUniqueKeyErr(ctx, newRow); err != nil {
				return err
			}
		}
		newKey, err := m.keyFromRow(ctx, newRow)
		if err != nil {
			return err
		}
		return m.mut.Put(ctx, newKey, val.EmptyTuple)
	}

	return nil
}

// UpdateChangesUniqueKey implements UniqueKeyChangeReporter for this index.
func (m prollySecondaryIndexWriter) UpdateChangesUniqueKey(oldRow sql.Row, newRow sql.Row) bool {
	if m.virtualExprs != nil {
		return m.unique
	}
	return m.unique && (m.predicate != nil || !isNoopUpdate(oldRow, newRow, m.keyMap[:m.idxCols]))
}

func (m prollySecondaryIndexWriter) Commit(ctx context.Context) error {
	return m.mut.Checkpoint(ctx)
}

func (m prollySecondaryIndexWriter) Discard(ctx context.Context) error {
	m.mut.Revert(ctx)
	return nil
}

func (m prollySecondaryIndexWriter) HasEdits(ctx context.Context) bool {
	return m.mut.HasEdits()
}

func (m prollySecondaryIndexWriter) IterRange(ctx context.Context, rng prolly.Range) (prolly.MapIter, error) {
	return m.mut.IterRange(ctx, rng)
}

// FormatKeyForUniqKeyErr formats the given tuple |key| using |d|. The resulting
// string is suitable for use in a sql.UniqueKeyError
func FormatKeyForUniqKeyErr(ctx context.Context, key val.Tuple, d *val.TupleDesc, sqlRow sql.Row) string {
	var sb strings.Builder
	sb.WriteString("[")
	seenOne := false
	for i := range d.Types {
		if seenOne {
			sb.WriteString(",")
		}
		seenOne = true
		switch d.Types[i].Enc {
		// address encodings should be printed as strings
		case val.BytesAddrEnc, val.StringAddrEnc:
			sb.WriteString(fmt.Sprintf("%s", sqlRow[i]))
		default:
			sb.WriteString(d.FormatValue(ctx, i, key.GetField(i)))
		}
	}
	sb.WriteString("]")
	return sb.String()
}
