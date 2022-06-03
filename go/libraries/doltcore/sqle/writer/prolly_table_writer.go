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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// todo(andy): get from NodeStore
var sharePool = pool.NewBuffPool()

type prollyTableWriter struct {
	tableName string
	dbName    string

	primary   indexWriter
	secondary map[string]indexWriter

	tbl    *doltdb.Table
	sch    schema.Schema
	sqlSch sql.Schema

	aiCol     schema.Column
	aiTracker globalstate.AutoIncrementTracker

	flusher WriteSessionFlusher
	setter  SessionRootSetter
	batched bool
}

var _ TableWriter = &prollyTableWriter{}

func getSecondaryProllyIndexWriters(ctx context.Context, t *doltdb.Table, sqlSch sql.Schema, sch schema.Schema) (map[string]indexWriter, error) {
	s, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	definitions := sch.Indexes().AllIndexes()
	writers := make(map[string]indexWriter)

	for _, def := range definitions {
		defName := def.Name()
		idxRows, err := s.GetIndex(ctx, sch, defName)
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idxRows)

		keyMap, valMap := ordinalMappingsFromSchema(sqlSch, def.Schema())
		keyDesc, valDesc := m.Descriptors()

		writers[defName] = prollyIndexWriter{
			name:   defName,
			mut:    m.Mutate(),
			keyBld: val.NewTupleBuilder(keyDesc),
			keyMap: keyMap,
			valBld: val.NewTupleBuilder(valDesc),
			valMap: valMap,
		}
	}

	return writers, nil
}

func getSecondaryKeylessProllyWriters(ctx context.Context, t *doltdb.Table, sqlSch sql.Schema, sch schema.Schema, primary prollyKeylessWriter) (map[string]indexWriter, error) {
	s, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	definitions := sch.Indexes().AllIndexes()
	writers := make(map[string]indexWriter)

	for _, def := range definitions {
		defName := def.Name()
		idxRows, err := s.GetIndex(ctx, sch, defName)
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idxRows)
		m = prolly.ConvertToKeylessIndex(m)

		keyMap, valMap := ordinalMappingsFromSchema(sqlSch, def.Schema())
		keyDesc, valDesc := m.Descriptors()

		writers[defName] = prollyKeylessSecondaryWriter{
			name:    defName,
			mut:     m.Mutate(),
			primary: primary,
			unique:  def.IsUnique(),
			keyBld:  val.NewTupleBuilder(keyDesc),
			keyMap:  keyMap,
			valBld:  val.NewTupleBuilder(valDesc),
			valMap:  valMap,
		}
	}

	return writers, nil
}

// Insert implements TableWriter.
func (w *prollyTableWriter) Insert(ctx *sql.Context, sqlRow sql.Row) (err error) {
	if sqlRow, err = index.NormalizeRow(w.sqlSch, sqlRow); err != nil {
		return err
	}

	if err := w.primary.Insert(ctx, sqlRow); err != nil {
		return err
	}
	for _, wr := range w.secondary {
		if err := wr.Insert(ctx, sqlRow); err != nil {
			if sql.ErrUniqueKeyViolation.Is(err) {
				return w.primary.UniqueKeyError(ctx, sqlRow)
			}
			return err
		}
	}
	return nil
}

// Delete implements TableWriter.
func (w *prollyTableWriter) Delete(ctx *sql.Context, sqlRow sql.Row) (err error) {
	if sqlRow, err = index.NormalizeRow(w.sqlSch, sqlRow); err != nil {
		return err
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
	if oldRow, err = index.NormalizeRow(w.sqlSch, oldRow); err != nil {
		return err
	}
	if newRow, err = index.NormalizeRow(w.sqlSch, newRow); err != nil {
		return err
	}

	for _, wr := range w.secondary {
		if err := wr.Update(ctx, oldRow, newRow); err != nil {
			if sql.ErrUniqueKeyViolation.Is(err) {
				return w.primary.UniqueKeyError(ctx, newRow)
			}
			return err
		}
	}
	if err := w.primary.Update(ctx, oldRow, newRow); err != nil {
		return err
	}
	return nil
}

// GetNextAutoIncrementValue implements TableWriter.
func (w *prollyTableWriter) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	return w.aiTracker.Next(w.tableName, insertVal)
}

// SetAutoIncrementValue implements TableWriter.
func (w *prollyTableWriter) SetAutoIncrementValue(ctx *sql.Context, val uint64) error {
	seq, err := globalstate.CoerceAutoIncrementValue(val)
	if err != nil {
		return err
	}

	// todo(andy) set here or in flush?
	w.tbl, err = w.tbl.SetAutoIncrementValue(ctx, seq)
	if err != nil {
		return err
	}
	w.aiTracker.Set(w.tableName, seq)

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
	return
}

// DiscardChanges implements TableWriter.
func (w *prollyTableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	err := w.primary.Discard(ctx)
	for _, secondary := range w.secondary {
		sErr := secondary.Discard(ctx)
		if sErr != nil && err == nil {
			err = sErr
		}
	}
	return err
}

// StatementComplete implements TableWriter.
func (w *prollyTableWriter) StatementComplete(ctx *sql.Context) error {
	err := w.primary.Commit(ctx)
	for _, secondary := range w.secondary {
		sErr := secondary.Commit(ctx)
		if sErr != nil && err == nil {
			err = sErr
		}
	}
	return err
}

// WithIndexLookup implements TableWriter.
func (w *prollyTableWriter) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	idx := index.IndexFromIndexLookup(lookup)
	return prollyFkIndexer{
		writer: w,
		index:  idx,
		pRange: index.ProllyRangesFromIndexLookup(lookup)[0],
	}
}

// Reset puts the writer into a fresh state, updating the schema and index writers according to the newly given table.
func (w *prollyTableWriter) Reset(ctx context.Context, sess *prollyWriteSession, tbl *doltdb.Table, sch schema.Schema) error {
	sqlSch, err := sqlutil.FromDoltSchema(w.tableName, sch)
	if err != nil {
		return err
	}
	aiCol := autoIncrementColFromSchema(sch)
	var newPrimary indexWriter

	var newSecondaries map[string]indexWriter
	if schema.IsKeyless(sch) {
		newPrimary, err = getPrimaryKeylessProllyWriter(ctx, tbl, sqlSch.Schema, sch)
		if err != nil {
			return err
		}
		newSecondaries, err = getSecondaryKeylessProllyWriters(ctx, tbl, sqlSch.Schema, sch, newPrimary.(prollyKeylessWriter))
		if err != nil {
			return err
		}
	} else {
		newPrimary, err = getPrimaryProllyWriter(ctx, tbl, sqlSch.Schema, sch)
		if err != nil {
			return err
		}
		newSecondaries, err = getSecondaryProllyIndexWriters(ctx, tbl, sqlSch.Schema, sch)
		if err != nil {
			return err
		}
	}

	w.tbl = tbl
	w.sch = sch
	w.sqlSch = sqlSch.Schema
	w.primary = newPrimary
	w.secondary = newSecondaries
	w.aiCol = aiCol
	w.flusher = sess

	return nil
}

func (w *prollyTableWriter) table(ctx context.Context) (t *doltdb.Table, err error) {
	// flush primary row storage
	pm, err := w.primary.Map(ctx)
	if err != nil {
		return nil, err
	}

	t, err = w.tbl.UpdateRows(ctx, durable.IndexFromProllyMap(pm))
	if err != nil {
		return nil, err
	}

	// flush secondary index storage
	s, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	for _, wrSecondary := range w.secondary {
		sm, err := wrSecondary.Map(ctx)
		if err != nil {
			return nil, err
		}
		idx := durable.IndexFromProllyMap(sm)

		s, err = s.PutIndex(ctx, wrSecondary.Name(), idx)
		if err != nil {
			return nil, err
		}
	}

	t, err = t.SetIndexSet(ctx, s)
	if err != nil {
		return nil, err
	}

	if w.aiCol.AutoIncrement {
		seq := w.aiTracker.Current(w.tableName)
		t, err = t.SetAutoIncrementValue(ctx, seq)
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}

func (w *prollyTableWriter) flush(ctx *sql.Context) error {
	ws, err := w.flusher.Flush(ctx)
	if err != nil {
		return err
	}
	return w.setter(ctx, w.dbName, ws.WorkingRoot())
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
