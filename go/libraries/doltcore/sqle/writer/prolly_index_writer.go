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
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
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

	return prollyIndexWriter{
		mut:    m.Mutate(),
		keyBld: val.NewTupleBuilder(keyDesc, m.NodeStore()),
		keyMap: schState.PriIndex.KeyMapping,
		valBld: val.NewTupleBuilder(valDesc, m.NodeStore()),
		valMap: schState.PriIndex.ValMapping,
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

	return prollyKeylessWriter{
		mut:    m.Mutate(),
		keyBld: val.NewTupleBuilder(keyDesc, m.NodeStore()),
		valBld: val.NewTupleBuilder(valDesc, m.NodeStore()),
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
	return m.keyBld.BuildPermissive(sharePool)
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
		remappedSqlRow := make(sql.Row, len(sqlRow))
		for to := range m.keyMap {
			from := m.keyMap.MapOrdinal(to)
			remappedSqlRow[to] = sqlRow[from]
		}
		keyStr := FormatKeyForUniqKeyErr(ctx, k, m.keyBld.Desc, remappedSqlRow)
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
	v, err := m.valBld.Build(sharePool)
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
		remappedSqlRow := make(sql.Row, len(newRow))
		for to := range m.keyMap {
			from := m.keyMap.MapOrdinal(to)
			remappedSqlRow[to] = newRow[from]
		}
		keyStr := FormatKeyForUniqKeyErr(ctx, newKey, m.keyBld.Desc, remappedSqlRow)
		return m.uniqueKeyError(ctx, keyStr, newKey, true)
	}

	for to := range m.valMap {
		from := m.valMap.MapOrdinal(to)
		if err = tree.PutField(ctx, m.mut.NodeStore(), m.valBld, to, newRow[from]); err != nil {
			return err
		}
	}
	v, err := m.valBld.Build(sharePool)
	if err != nil {
		return err
	}

	return m.mut.Put(ctx, newKey, v)
}

func (m prollyIndexWriter) Commit(ctx context.Context) error {
	return m.mut.Checkpoint(ctx)
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
	name          string
	mut           prolly.MutableMapInterface
	unique        bool
	prefixLengths []uint16

	// number of indexed cols
	idxCols int

	// keyMap is a mapping from sql.Row fields to
	// key fields of this secondary index
	keyMap val.OrdinalMapping
	// keyBld builds key tuples for the secondary index
	keyBld *val.TupleBuilder

	// pkMap is a mapping from secondary index keys to
	// primary key clustered index keys
	pkMap val.OrdinalMapping
	// pkBld builds key tuples for primary key index
	pkBld *val.TupleBuilder
}

var _ indexWriter = prollySecondaryIndexWriter{}

func (m prollySecondaryIndexWriter) Name() string {
	return m.name
}

func (m prollySecondaryIndexWriter) Map(ctx context.Context) (prolly.MapInterface, error) {
	return m.mut.MapInterface(ctx)
}

func (m prollySecondaryIndexWriter) ValidateKeyViolations(ctx context.Context, sqlRow sql.Row) error {
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
		from := m.keyMap.MapOrdinal(to)
		keyPart, _ := m.trimKeyPart(ctx, to, sqlRow[from])
		if err := tree.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, keyPart); err != nil {
			return nil, err
		}
	}
	return m.keyBld.Build(sharePool)
}

func (m prollySecondaryIndexWriter) Insert(ctx context.Context, sqlRow sql.Row) error {
	k, err := m.keyFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}
	return m.mut.Put(ctx, k, val.EmptyTuple)
}

func (m prollySecondaryIndexWriter) checkForUniqueKeyErr(ctx context.Context, sqlRow sql.Row) error {
	ns := m.mut.NodeStore()
	for to := range m.keyMap[:m.idxCols] {
		from := m.keyMap.MapOrdinal(to)
		if sqlRow[from] == nil {
			// NULL is incomparable and cannot
			// trigger a UNIQUE KEY violation
			m.keyBld.Recycle()
			return nil
		}
		keyPart, _ := m.trimKeyPart(ctx, to, sqlRow[from])
		if err := tree.PutField(ctx, ns, m.keyBld, to, keyPart); err != nil {
			return err
		}
	}

	// build a val.Tuple containing only fields for the unique column prefix
	key := m.keyBld.BuildPrefix(ns.Pool(), m.idxCols)
	desc := m.keyBld.Desc.PrefixDesc(m.idxCols)
	rng := prolly.PrefixRange(ctx, key, desc)
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
	existingPK, err := m.pkBld.Build(sharePool)
	if err != nil {
		return err
	}

	remappedSqlRow := make(sql.Row, m.idxCols)
	for to := range m.keyMap[:m.idxCols] {
		from := m.keyMap.MapOrdinal(to)
		remappedSqlRow[to], _ = m.trimKeyPart(ctx, to, sqlRow[from])
	}
	return secondaryUniqueKeyError{
		keyStr:      FormatKeyForUniqKeyErr(ctx, key, desc, remappedSqlRow),
		existingKey: existingPK,
	}
}

func (m prollySecondaryIndexWriter) Delete(ctx context.Context, sqlRow sql.Row) error {
	k, err := m.keyFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}
	return m.mut.Delete(ctx, k)
}

func (m prollySecondaryIndexWriter) Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) error {
	oldKey, err := m.keyFromRow(ctx, oldRow)
	if err != nil {
		return err
	}

	// todo(andy): we can skip building, deleting |oldKey|
	//  if we know the key fields are unchanged
	if err := m.mut.Delete(ctx, oldKey); err != nil {
		return err
	}

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
func FormatKeyForUniqKeyErr(ctx context.Context, key val.Tuple, d val.TupleDesc, sqlRow sql.Row) string {
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
