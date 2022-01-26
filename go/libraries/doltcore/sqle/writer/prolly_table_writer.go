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
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyWriter struct {
	tableName string
	dbName    string

	sch schema.Schema
	mut prollyIndexWriter

	tbl       *doltdb.Table
	aiCol     schema.Column
	aiTracker globalstate.AutoIncrementTracker

	sess    WriteSession
	setter  SessionRootSetter
	batched bool
}

var _ TableWriter = &prollyWriter{}

// Insert implements TableWriter.
func (w *prollyWriter) Insert(ctx *sql.Context, sqlRow sql.Row) (err error) {
	if schema.IsKeyless(w.sch) {
		return errors.New("operation unsupported")
	}
	return w.mut.Insert(ctx, sqlRow)
}

// Delete implements TableWriter.
func (w *prollyWriter) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	if schema.IsKeyless(w.sch) {
		return errors.New("operation unsupported")
	}
	return w.mut.Delete(ctx, sqlRow)
}

// Update implements TableWriter.
func (w *prollyWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	if schema.IsKeyless(w.sch) {
		return errors.New("operation unsupported")
	}
	return w.mut.Update(ctx, oldRow, newRow)
}

// NextAutoIncrementValue implements TableWriter.
func (w *prollyWriter) NextAutoIncrementValue(potentialVal, tableVal interface{}) (interface{}, error) {
	return w.aiTracker.Next(w.tableName, potentialVal, tableVal)
}

// SetAutoIncrementValue implements TableWriter.
func (w *prollyWriter) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
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
func (w *prollyWriter) Close(ctx *sql.Context) error {
	// If we're running in batched mode, don't flush the edits until explicitly told to do so
	if w.batched {
		return nil
	}

	return w.flush(ctx)
}

// StatementBegin implements TableWriter.
func (w *prollyWriter) StatementBegin(ctx *sql.Context) {
	// todo(andy)
	return
}

// DiscardChanges implements TableWriter.
func (w *prollyWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	// todo(andy)
	return nil
}

// StatementComplete implements TableWriter.
func (w *prollyWriter) StatementComplete(ctx *sql.Context) error {
	// todo(andy)
	return nil
}

func (w *prollyWriter) table(ctx context.Context) (t *doltdb.Table, err error) {
	m, err := w.mut.Map(ctx)
	if err != nil {
		return nil, err
	}

	t, err = w.tbl.UpdateRows(ctx, durable.IndexFromProllyMap(m))
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

func (w *prollyWriter) flush(ctx *sql.Context) error {
	newRoot, err := w.sess.Flush(ctx)
	if err != nil {
		return err
	}

	return w.setter(ctx, w.dbName, newRoot)
}

type prollyIndexWriter struct {
	mut prolly.MutableMap

	keyBld *val.TupleBuilder
	keyMap colMapping

	valBld *val.TupleBuilder
	valMap colMapping
}

func newProllyIndexWriter(m prolly.Map, sqlSch sql.Schema, sch schema.Schema) prollyIndexWriter {
	keyDesc, valDesc := m.Descriptors()
	keyMap, valMap := colMappingsFromSchema(sqlSch, sch)

	return prollyIndexWriter{
		mut:    m.Mutate(),
		keyBld: val.NewTupleBuilder(keyDesc),
		keyMap: keyMap,
		valBld: val.NewTupleBuilder(valDesc),
		valMap: valMap,
	}
}

var sharePool = pool.NewBuffPool()

func (m prollyIndexWriter) Map(ctx context.Context) (prolly.Map, error) {
	return m.mut.Map(ctx)
}

func (m prollyIndexWriter) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	for to, from := range m.keyMap {
		m.keyBld.PutField(to, sqlRow[from])
	}
	k := m.keyBld.Build(sharePool)

	ok, err := m.mut.Has(ctx, k)
	if err != nil {
		return err
	} else if ok {
		return m.primaryKeyError(ctx, k)
	}

	for to, from := range m.valMap {
		m.valBld.PutField(to, sqlRow[from])
	}
	v := m.valBld.Build(sharePool)

	return m.mut.Put(ctx, k, v)
}

func (m prollyIndexWriter) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	for to, from := range m.keyMap {
		m.keyBld.PutField(to, sqlRow[from])
	}
	k := m.keyBld.Build(sharePool)

	return m.mut.Delete(ctx, k)
}

func (m prollyIndexWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	for to, from := range m.keyMap {
		m.keyBld.PutField(to, oldRow[from])
	}
	oldKey := m.keyBld.Build(sharePool)

	// todo(andy): we can skip building, deleting |oldKey|
	//  if we know the key fields are unchanged
	if err := m.mut.Delete(ctx, oldKey); err != nil {
		return err
	}

	for to, from := range m.keyMap {
		m.keyBld.PutField(to, newRow[from])
	}
	newKey := m.keyBld.Build(sharePool)

	ok, err := m.mut.Has(ctx, newKey)
	if err != nil {
		return err
	} else if ok {
		return m.primaryKeyError(ctx, newKey)
	}

	for to, from := range m.valMap {
		m.valBld.PutField(to, newRow[from])
	}
	v := m.valBld.Build(sharePool)

	return m.mut.Put(ctx, newKey, v)
}

func (m prollyIndexWriter) primaryKeyError(ctx context.Context, key val.Tuple) error {
	existing := make(sql.Row, len(m.keyMap)+len(m.valMap))

	_ = m.mut.Get(ctx, key, func(key, value val.Tuple) (err error) {
		kd := m.keyBld.Desc
		for from, to := range m.keyMap {
			existing[to] = kd.GetField(from, key)
		}

		vd := m.valBld.Desc
		for from, to := range m.valMap {
			existing[to] = vd.GetField(from, value)
		}
		return
	})

	s := m.keyBld.Desc.Format(key)

	return sql.NewUniqueKeyErr(s, true, existing)
}

type colMapping []int

func colMappingsFromSchema(from sql.Schema, to schema.Schema) (km, vm colMapping) {
	km = makeColMapping(from, to.GetPKCols())
	vm = makeColMapping(from, to.GetNonPKCols())
	return
}

func makeColMapping(from sql.Schema, to *schema.ColCollection) (m colMapping) {
	m = make(colMapping, len(to.GetColumns()))
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
