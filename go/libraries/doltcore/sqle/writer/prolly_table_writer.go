// Copyright 2019 Dolthub, Inc.
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
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyTableWriter struct {
	tableName string
	dbName    string

	primary   prollyIndexWriter
	secondary []prollyIndexWriter

	tbl       *doltdb.Table
	sch       schema.Schema
	aiCol     schema.Column
	aiTracker globalstate.AutoIncrementTracker

	sess    WriteSession
	setter  SessionRootSetter
	batched bool
}

var _ TableWriter = &prollyTableWriter{}

// Insert implements TableWriter.
func (w *prollyTableWriter) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	if schema.IsKeyless(w.sch) {
		return errors.New("operation unsupported")
	}
	for _, wr := range w.secondary {
		if err := wr.Insert(ctx, sqlRow); err != nil {
			return err
		}
	}
	if err := w.primary.Insert(ctx, sqlRow); err != nil {
		return err
	}
	return nil
}

// Delete implements TableWriter.
func (w *prollyTableWriter) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	if schema.IsKeyless(w.sch) {
		return errors.New("operation unsupported")
	}
	for _, wr := range w.secondary {
		if err := wr.Delete(ctx, sqlRow); err != nil {
			return err
		}
	}
	if err := w.primary.Delete(ctx, sqlRow); err != nil {
		return err
	}
	return nil
}

// Update implements TableWriter.
func (w *prollyTableWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	if schema.IsKeyless(w.sch) {
		return errors.New("operation unsupported")
	}
	for _, wr := range w.secondary {
		if err := wr.Update(ctx, oldRow, newRow); err != nil {
			return err
		}
	}
	if err := w.primary.Update(ctx, oldRow, newRow); err != nil {
		return err
	}
	return nil
}

// NextAutoIncrementValue implements TableWriter.
func (w *prollyTableWriter) NextAutoIncrementValue(potentialVal, tableVal interface{}) (interface{}, error) {
	return w.aiTracker.Next(w.tableName, potentialVal, tableVal)
}

// SetAutoIncrementValue implements TableWriter.
func (w *prollyTableWriter) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
	nomsVal, err := w.aiCol.TypeInfo.ConvertValueToNomsValue(ctx, w.tbl.ValueReadWriter(), val)
	if err != nil {
		return err
	}

	w.tbl, err = w.tbl.SetAutoIncrementValue(ctx, nomsVal)
	if err != nil {
		return err
	}

	w.aiTracker.Reset(w.tableName, val)

	return w.flush(ctx)
}

// Close implements Closer
func (w *prollyTableWriter) Close(ctx *sql.Context) error {
	// If we're running in batched mode, don't flush the edits until explicitly told to do so
	if w.batched {
		return nil
	}

	return w.flush(ctx)
}

// StatementBegin implements TableWriter.
func (w *prollyTableWriter) StatementBegin(ctx *sql.Context) {
	// todo(andy)
	return
}

// DiscardChanges implements TableWriter.
func (w *prollyTableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	// todo(andy)
	return nil
}

// StatementComplete implements TableWriter.
func (w *prollyTableWriter) StatementComplete(ctx *sql.Context) error {
	// todo(andy)
	return nil
}

func (w *prollyTableWriter) table(ctx context.Context) (t *doltdb.Table, err error) {
	// flush primary row storage
	m, err := w.primary.Map(ctx)
	if err != nil {
		return nil, err
	}

	t, err = w.tbl.UpdateRows(ctx, durable.IndexFromProllyMap(m))
	if err != nil {
		return nil, err
	}

	// flush secondary index storage
	s, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	for _, wr := range w.secondary {
		m, err := wr.mut.Map(ctx)
		if err != nil {
			return nil, err
		}
		idx := durable.IndexFromProllyMap(m)

		s, err = s.PutIndex(ctx, wr.name, idx)
		if err != nil {
			return nil, err
		}
	}

	t, err = t.SetIndexSet(ctx, s)
	if err != nil {
		return nil, err
	}

	if w.aiCol.AutoIncrement {
		seq, err := w.aiTracker.Next(w.tableName, nil, nil)
		if err != nil {
			return nil, err
		}
		vrw := w.tbl.ValueReadWriter()

		v, err := w.aiCol.TypeInfo.ConvertValueToNomsValue(ctx, vrw, seq)
		if err != nil {
			return nil, err
		}

		t, err = t.SetAutoIncrementValue(ctx, v)
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}

func (w *prollyTableWriter) flush(ctx *sql.Context) error {
	newRoot, err := w.sess.Flush(ctx)
	if err != nil {
		return err
	}

	return w.setter(ctx, w.dbName, newRoot)
}

type prollyIndexWriter struct {
	name string
	mut  prolly.MutableMap

	keyBld *val.TupleBuilder
	keyMap val.OrdinalMapping

	valBld *val.TupleBuilder
	valMap val.OrdinalMapping
}

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

func getSecondaryProllyIndexWriters(ctx context.Context, t *doltdb.Table, sqlSch sql.Schema, sch schema.Schema) ([]prollyIndexWriter, error) {
	s, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	definitions := sch.Indexes().AllIndexes()
	writers := make([]prollyIndexWriter, len(definitions))

	for i, def := range definitions {
		idxRows, err := s.GetIndex(ctx, sch, def.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idxRows)

		keyMap, valMap := ordinalMappingsFromSchema(sqlSch, def.Schema())
		keyDesc, valDesc := m.Descriptors()

		writers[i] = prollyIndexWriter{
			name:   def.Name(),
			mut:    m.Mutate(),
			keyBld: val.NewTupleBuilder(keyDesc),
			keyMap: keyMap,
			valBld: val.NewTupleBuilder(valDesc),
			valMap: valMap,
		}
	}

	return writers, nil
}

var sharePool = pool.NewBuffPool()

func (m prollyIndexWriter) Map(ctx context.Context) (prolly.Map, error) {
	return m.mut.Map(ctx)
}

func (m prollyIndexWriter) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		index.PutField(m.keyBld, to, sqlRow[from])
	}
	k := m.keyBld.Build(sharePool)

	ok, err := m.mut.Has(ctx, k)
	if err != nil {
		return err
	} else if ok {
		return m.primaryKeyError(ctx, k)
	}

	for to := range m.valMap {
		from := m.valMap.MapOrdinal(to)
		index.PutField(m.valBld, to, sqlRow[from])
	}
	v := m.valBld.Build(sharePool)

	return m.mut.Put(ctx, k, v)
}

func (m prollyIndexWriter) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		index.PutField(m.keyBld, to, sqlRow[from])
	}
	k := m.keyBld.Build(sharePool)

	return m.mut.Delete(ctx, k)
}

func (m prollyIndexWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		index.PutField(m.keyBld, to, oldRow[from])
	}
	oldKey := m.keyBld.Build(sharePool)

	// todo(andy): we can skip building, deleting |oldKey|
	//  if we know the key fields are unchanged
	if err := m.mut.Delete(ctx, oldKey); err != nil {
		return err
	}

	for to := range m.keyMap {
		from := m.keyMap.MapOrdinal(to)
		index.PutField(m.keyBld, to, newRow[from])
	}
	newKey := m.keyBld.Build(sharePool)

	ok, err := m.mut.Has(ctx, newKey)
	if err != nil {
		return err
	} else if ok {
		return m.primaryKeyError(ctx, newKey)
	}

	for to := range m.valMap {
		from := m.valMap.MapOrdinal(to)
		index.PutField(m.valBld, to, newRow[from])
	}
	v := m.valBld.Build(sharePool)

	return m.mut.Put(ctx, newKey, v)
}

func (m prollyIndexWriter) primaryKeyError(ctx context.Context, key val.Tuple) error {
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

	return sql.NewUniqueKeyErr(s, true, dupe)
}

func ordinalMappingsFromSchema(from sql.Schema, to schema.Schema) (km, vm val.OrdinalMapping) {
	km = makeOrdinalMapping(from, to.GetPKCols())
	vm = makeOrdinalMapping(from, to.GetNonPKCols())
	return
}

func makeOrdinalMapping(from sql.Schema, to *schema.ColCollection) (m val.OrdinalMapping) {
	m = make(val.OrdinalMapping, len(to.GetColumns()))
	for i := range m {
		name := to.GetAtIndex(i).Name
		for j, col := range from {
			if col.Name == name {
				m[i] = j
			}
		}
	}
	return
}
