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

func getKeylessProllyWriter(ctx context.Context, t *doltdb.Table, sqlSch sql.Schema, sch schema.Schema) (indexWriter, error) {
	idx, err := t.GetRowData(ctx)
	if err != nil {
		return prollyIndexWriter{}, err
	}

	m := durable.ProllyMapFromIndex(idx)

	_, valDesc := m.Descriptors()
	_, valMap := ordinalMappingsFromSchema(sqlSch, sch)

	return prollyKeylessWriter{
		mut:    m.Mutate(),
		valBld: val.NewTupleBuilder(valDesc),
		valMap: valMap,
	}, nil
}

type indexWriter interface {
	Map(ctx context.Context) (prolly.Map, error)
	Insert(ctx context.Context, sqlRow sql.Row) error
	Delete(ctx context.Context, sqlRow sql.Row) error
	Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) error
	Commit(ctx context.Context) error
	Discard(ctx context.Context) error
	HasEdits(ctx context.Context) bool
	UniqueKeyError(ctx context.Context, sqlRow sql.Row) error
}

type prollyIndexWriter struct {
	name string
	mut  prolly.MutableMap

	keyBld *val.TupleBuilder
	keyMap val.OrdinalMapping

	valBld *val.TupleBuilder
	valMap val.OrdinalMapping
}

var _ indexWriter = prollyIndexWriter{}

func (m prollyIndexWriter) Map(ctx context.Context) (prolly.Map, error) {
	return m.mut.Map(ctx)
}

func (m prollyIndexWriter) Insert(ctx context.Context, sqlRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(m.keyBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	k := m.keyBld.Build(sharePool)

	ok, err := m.mut.Has(ctx, k)
	if err != nil {
		return err
	} else if ok {
		// All secondary writers have a name, while the primary does not. If this is a secondary writer, then we can
		// bypass the keyError() call as it will be done in the calling primary writer.
		if m.name == "" {
			return m.keyError(ctx, k, true)
		} else {
			return sql.ErrUniqueKeyViolation.New()
		}
	}

	for to := range m.valMap {
		from := m.valMap.MapOrdinal(to)
		if err = index.PutField(m.valBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	v := m.valBld.Build(sharePool)

	return m.mut.Put(ctx, k, v)
}

func (m prollyIndexWriter) Delete(ctx context.Context, sqlRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(m.keyBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	k := m.keyBld.Build(sharePool)

	return m.mut.Delete(ctx, k)
}

func (m prollyIndexWriter) Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(m.keyBld, to, oldRow[from]); err != nil {
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
		if err := index.PutField(m.keyBld, to, newRow[from]); err != nil {
			return err
		}
	}
	newKey := m.keyBld.Build(sharePool)

	ok, err := m.mut.Has(ctx, newKey)
	if err != nil {
		return err
	} else if ok {
		// All secondary writers have a name, while the primary does not. If this is a secondary writer, then we can
		// bypass the keyError() call as it will be done in the calling primary writer.
		if m.name == "" {
			return m.keyError(ctx, newKey, true)
		} else {
			return sql.ErrUniqueKeyViolation.New()
		}
	}

	for to := range m.valMap {
		from := m.valMap.MapOrdinal(to)
		if err = index.PutField(m.valBld, to, newRow[from]); err != nil {
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

func (m prollyIndexWriter) UniqueKeyError(ctx context.Context, sqlRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		if err := index.PutField(m.keyBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	k := m.keyBld.Build(sharePool)
	return m.keyError(ctx, k, false)
}

func (m prollyIndexWriter) keyError(ctx context.Context, key val.Tuple, isPk bool) error {
	dupe := make(sql.Row, len(m.keyMap)+len(m.valMap))

	_ = m.mut.Get(ctx, key, func(key, value val.Tuple) (err error) {
		kd := m.keyBld.Desc
		for from := range m.keyMap {
			to := m.keyMap.MapOrdinal(from)
			if dupe[to], err = index.GetField(kd, from, key); err != nil {
				return err
			}
		}

		vd := m.valBld.Desc
		for from := range m.valMap {
			to := m.valMap.MapOrdinal(from)
			if dupe[to], err = index.GetField(vd, from, value); err != nil {
				return err
			}
		}
		return
	})

	s := m.keyBld.Desc.Format(key)

	return sql.NewUniqueKeyErr(s, isPk, dupe)
}

type prollyKeylessWriter struct {
	name string
	mut  prolly.MutableMap

	valBld *val.TupleBuilder
	valMap val.OrdinalMapping
}

var _ indexWriter = prollyKeylessWriter{}

func (k prollyKeylessWriter) Map(ctx context.Context) (prolly.Map, error) {
	return k.mut.Map(ctx)
}

func (k prollyKeylessWriter) Insert(ctx context.Context, sqlRow sql.Row) error {
	hashId, value, err := k.tuplesFromRow(sqlRow)
	if err != nil {
		return err
	}

	err = k.mut.Get(ctx, hashId, func(k, v val.Tuple) (err error) {
		if k != nil {
			value = v
		}
		return
	})
	if err != nil {
		return err
	}

	// increment cardinality
	val.ModifyKeylessCardinality(value, int64(1))

	return k.mut.Put(ctx, hashId, value)
}

func (k prollyKeylessWriter) Delete(ctx context.Context, sqlRow sql.Row) error {
	hashId, _, err := k.tuplesFromRow(sqlRow)
	if err != nil {
		return err
	}

	var value val.Tuple
	err = k.mut.Get(ctx, hashId, func(k, v val.Tuple) (err error) {
		if k != nil {
			value = v
		}
		return
	})
	if err != nil {
		return err
	}

	if value == nil {
		return nil // non-existent row
	}

	// decrement cardinality
	after := val.ModifyKeylessCardinality(value, int64(-1))
	if after > 0 {
		return k.mut.Put(ctx, hashId, value)
	} else {
		return k.mut.Delete(ctx, hashId)
	}
}

func (k prollyKeylessWriter) Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	if err = k.Delete(ctx, oldRow); err != nil {
		return err
	}
	if err = k.Insert(ctx, newRow); err != nil {
		return err
	}
	return
}

func (k prollyKeylessWriter) Commit(ctx context.Context) error {
	return k.mut.ApplyPending(ctx)
}

func (k prollyKeylessWriter) Discard(ctx context.Context) error {
	k.mut.DiscardPending(ctx)
	return nil
}

func (k prollyKeylessWriter) HasEdits(ctx context.Context) bool {
	return k.mut.HasEdits()
}

func (k prollyKeylessWriter) UniqueKeyError(ctx context.Context, sqlRow sql.Row) error {
	//TODO: figure out what should go here
	return fmt.Errorf("keyless does not yet know how to handle unique key errors")
}

func (k prollyKeylessWriter) tuplesFromRow(sqlRow sql.Row) (hashId, value val.Tuple, err error) {
	// initialize cardinality to 0
	if err = index.PutField(k.valBld, 0, uint64(0)); err != nil {
		return nil, nil, err
	}

	for to := range k.valMap {
		from := k.valMap.MapOrdinal(to)
		if err = index.PutField(k.valBld, to+1, sqlRow[from]); err != nil {
			return nil, nil, err
		}
	}

	value = k.valBld.Build(sharePool)
	hashId = val.HashTupleFromValue(sharePool, value)
	return
}
