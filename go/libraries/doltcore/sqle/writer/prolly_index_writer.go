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
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

func getPrimaryProllyWriter(ctx context.Context, t *doltdb.Table, sqlSch sql.Schema, sch schema.Schema) (prollyIndexWriter, error) {
	idx, err := t.GetRowData(ctx)
	if err != nil {
		return prollyIndexWriter{}, err
	}

	m := durable.ProllyMapFromIndex(idx)

	keyDesc, valDesc := m.Descriptors()
	keyMap, valMap := ordinalMappingsFromSchema(sqlSch, sch)

	return prollyIndexWriter{
		mut:    m.Mutate(),
		keyBld: val.NewTupleBuilder(keyDesc),
		keyMap: keyMap,
		valBld: val.NewTupleBuilder(valDesc),
		valMap: valMap,
	}, nil
}

func getPrimaryKeylessProllyWriter(ctx context.Context, t *doltdb.Table, sqlSch sql.Schema, sch schema.Schema) (prollyKeylessWriter, error) {
	idx, err := t.GetRowData(ctx)
	if err != nil {
		return prollyKeylessWriter{}, err
	}

	m := durable.ProllyMapFromIndex(idx)

	keyDesc, valDesc := m.Descriptors()
	_, valMap := ordinalMappingsFromSchema(sqlSch, sch)

	return prollyKeylessWriter{
		mut:    m.Mutate(),
		keyBld: val.NewTupleBuilder(keyDesc),
		valBld: val.NewTupleBuilder(valDesc),
		valMap: valMap,
	}, nil
}

type indexWriter interface {
	Name() string
	Map(ctx context.Context) (prolly.Map, error)
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
	mut prolly.MutableMap

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

func (m prollyIndexWriter) Map(ctx context.Context) (prolly.Map, error) {
	return m.mut.Map(ctx)
}

func (m prollyIndexWriter) Insert(ctx context.Context, sqlRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	k := m.keyBld.Build(sharePool)

	ok, err := m.mut.Has(ctx, k)
	if err != nil {
		return err
	} else if ok {
		keyStr := FormatKeyForUniqKeyErr(k, m.keyBld.Desc)
		return m.uniqueKeyError(ctx, keyStr, k, true)
	}

	for to := range m.valMap {
		from := m.valMap.MapOrdinal(to)
		if err = index.PutField(ctx, m.mut.NodeStore(), m.valBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	v := m.valBld.Build(sharePool)

	return m.mut.Put(ctx, k, v)
}

func (m prollyIndexWriter) Delete(ctx context.Context, sqlRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	k := m.keyBld.Build(sharePool)

	return m.mut.Delete(ctx, k)
}

func (m prollyIndexWriter) Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, oldRow[from]); err != nil {
			return err
		}
	}
	oldKey := m.keyBld.Build(sharePool)

	// todo(andy): we can skip building, deleting |oldKey|
	//  if we know the key fields are unchanged
	if err := m.mut.Delete(ctx, oldKey); err != nil {
		return err
	}

	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, newRow[from]); err != nil {
			return err
		}
	}
	newKey := m.keyBld.Build(sharePool)

	ok, err := m.mut.Has(ctx, newKey)
	if err != nil {
		return err
	} else if ok {
		keyStr := FormatKeyForUniqKeyErr(newKey, m.keyBld.Desc)
		return m.uniqueKeyError(ctx, keyStr, newKey, true)
	}

	for to := range m.valMap {
		from := m.valMap.MapOrdinal(to)
		if err = index.PutField(ctx, m.mut.NodeStore(), m.valBld, to, newRow[from]); err != nil {
			return err
		}
	}
	v := m.valBld.Build(sharePool)

	return m.mut.Put(ctx, newKey, v)
}

func (m prollyIndexWriter) Commit(ctx context.Context) error {
	return m.mut.ApplyPending(ctx)
}

func (m prollyIndexWriter) Discard(ctx context.Context) error {
	m.mut.DiscardPending(ctx)
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
			if existing[to], err = index.GetField(ctx, kd, from, key, m.mut.NodeStore()); err != nil {
				return err
			}
		}

		vd := m.valBld.Desc
		for from := range m.valMap {
			to := m.valMap.MapOrdinal(from)
			if existing[to], err = index.GetField(ctx, vd, from, value, m.mut.NodeStore()); err != nil {
				return err
			}
		}
		return
	})

	return sql.NewUniqueKeyErr(keyStr, isPk, existing)
}

type prollySecondaryIndexWriter struct {
	name   string
	mut    prolly.MutableMap
	unique bool

	keyBld    *val.TupleBuilder
	prefixBld *val.TupleBuilder
	suffixBld *val.TupleBuilder
	keyMap    val.OrdinalMapping
}

var _ indexWriter = prollySecondaryIndexWriter{}

func (m prollySecondaryIndexWriter) Name() string {
	return m.name
}

func (m prollySecondaryIndexWriter) Map(ctx context.Context) (prolly.Map, error) {
	return m.mut.Map(ctx)
}

func (m prollySecondaryIndexWriter) Insert(ctx context.Context, sqlRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, sqlRow[from]); err != nil {
			return err
		}
		if to < m.prefixBld.Desc.Count() {
			if err := index.PutField(ctx, m.mut.NodeStore(), m.prefixBld, to, sqlRow[from]); err != nil {
				return err
			}
		}
	}
	k := m.keyBld.Build(sharePool)

	if m.unique {
		prefixKey := m.prefixBld.Build(sharePool)
		err := m.checkForUniqueKeyErr(ctx, prefixKey)
		if err != nil {
			return err
		}
	} else {
		m.prefixBld.Recycle()
	}

	return m.mut.Put(ctx, k, val.EmptyTuple)
}

func (m prollySecondaryIndexWriter) checkForUniqueKeyErr(ctx context.Context, prefixKey val.Tuple) error {
	for i := 0; i < prefixKey.Count(); i++ {
		if prefixKey.FieldIsNull(i) {
			return nil
		}
	}
	rng := prolly.PrefixRange(prefixKey, m.prefixBld.Desc)
	itr, err := m.mut.IterRange(ctx, rng)
	if err != nil {
		return err
	}
	existingK, _, err := itr.Next(ctx)
	if err != nil && err != io.EOF {
		return err
	}
	if err == nil {
		for i := m.prefixBld.Desc.Count(); i < existingK.Count(); i++ {
			j := i - m.prefixBld.Desc.Count()
			m.suffixBld.PutRaw(j, existingK.GetField(i))
		}
		suffixK := m.suffixBld.Build(sharePool)
		keyStr := FormatKeyForUniqKeyErr(prefixKey, m.prefixBld.Desc)
		return secondaryUniqueKeyError{keyStr: keyStr, existingKey: suffixK}
	}
	return nil
}

func (m prollySecondaryIndexWriter) Delete(ctx context.Context, sqlRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	k := m.keyBld.Build(sharePool)
	return m.mut.Delete(ctx, k)
}

func (m prollySecondaryIndexWriter) Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, oldRow[from]); err != nil {
			return err
		}
	}
	oldKey := m.keyBld.Build(sharePool)

	// todo(andy): we can skip building, deleting |oldKey|
	//  if we know the key fields are unchanged
	if err := m.mut.Delete(ctx, oldKey); err != nil {
		return err
	}

	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, m.mut.NodeStore(), m.keyBld, to, newRow[from]); err != nil {
			return err
		}
		if to < m.prefixBld.Desc.Count() {
			if err := index.PutField(ctx, m.mut.NodeStore(), m.prefixBld, to, newRow[from]); err != nil {
				return err
			}
		}
	}
	newKey := m.keyBld.Build(sharePool)

	if m.unique {
		prefixKey := m.prefixBld.Build(sharePool)
		err := m.checkForUniqueKeyErr(ctx, prefixKey)
		if err != nil {
			return err
		}
	} else {
		m.prefixBld.Recycle()
	}

	return m.mut.Put(ctx, newKey, val.EmptyTuple)
}

func (m prollySecondaryIndexWriter) Commit(ctx context.Context) error {
	return m.mut.ApplyPending(ctx)
}

func (m prollySecondaryIndexWriter) Discard(ctx context.Context) error {
	m.mut.DiscardPending(ctx)
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
func FormatKeyForUniqKeyErr(key val.Tuple, d val.TupleDesc) string {
	var sb strings.Builder
	sb.WriteString("[")
	seenOne := false
	for i := range d.Types {
		if seenOne {
			sb.WriteString(",")
		}
		seenOne = true
		sb.WriteString(d.FormatValue(i, key.GetField(i)))
	}
	sb.WriteString("]")
	return sb.String()
}
