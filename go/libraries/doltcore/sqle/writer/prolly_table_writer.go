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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

// todo(andy): get from NodeStore
var sharePool = pool.NewBuffPool()

type prollyTableWriter struct {
	tableName doltdb.TableName
	dbName    string

	primary   indexWriter
	secondary map[string]indexWriter

	tbl    *doltdb.Table
	sch    schema.Schema
	sqlSch sql.Schema

	aiCol                  schema.Column
	aiTracker              globalstate.AutoIncrementTracker
	nextAutoIncrementValue map[string]uint64
	setAutoIncrement       bool

	flusher dsess.WriteSessionFlusher
	setter  dsess.SessionRootSetter

	targetStaging bool

	errEncountered error
}

var _ dsess.TableWriter = &prollyTableWriter{}
var _ AutoIncrementGetter = &prollyTableWriter{}

func getSecondaryProllyIndexWriters(ctx context.Context, t *doltdb.Table, schState *dsess.WriterState) (map[string]indexWriter, error) {
	s, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	// check session cache based on schema hash argument
	// we want to get or save the
	writers := make(map[string]indexWriter)

	for _, def := range schState.SecIndexes {
		if def.IsFullText {
			continue
		}
		defName := def.Name
		idxRows, err := s.GetIndex(ctx, schState.DoltSchema, def.Schema, defName)
		if err != nil {
			return nil, err
		}
		idxMap := durable.MapFromIndex(idxRows)

		keyDesc, _ := idxMap.Descriptors()

		// mapping from secondary index key to primary key
		writers[defName] = prollySecondaryIndexWriter{
			name:          defName,
			mut:           idxMap.MutateInterface(),
			unique:        def.IsUnique,
			prefixLengths: def.PrefixLengths,
			idxCols:       def.Count,
			keyMap:        def.KeyMapping,
			keyBld:        val.NewTupleBuilder(keyDesc, idxMap.NodeStore()),
			pkMap:         def.PkMapping,
			pkBld:         val.NewTupleBuilder(schState.PkKeyDesc, idxMap.NodeStore()),
		}
	}

	return writers, nil
}

func getSecondaryKeylessProllyWriters(ctx context.Context, t *doltdb.Table, schState *dsess.WriterState, primary prollyKeylessWriter) (map[string]indexWriter, error) {
	s, err := t.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	writers := make(map[string]indexWriter)

	for _, def := range schState.SecIndexes {
		if def.IsFullText {
			continue
		}
		defName := def.Name
		idxRows, err := s.GetIndex(ctx, schState.DoltSchema, def.Schema, defName)
		if err != nil {
			return nil, err
		}
		m, err := durable.ProllyMapFromIndex(idxRows)
		if err != nil {
			return nil, err
		}

		keyDesc, _ := m.Descriptors()

		writers[defName] = prollyKeylessSecondaryWriter{
			name:          defName,
			mut:           m.Mutate(),
			primary:       primary,
			unique:        def.IsUnique,
			spatial:       def.IsSpatial,
			prefixLengths: def.PrefixLengths,
			keyBld:        val.NewTupleBuilder(keyDesc, m.NodeStore()),
			prefixBld:     val.NewTupleBuilder(keyDesc.PrefixDesc(def.Count), m.NodeStore()),
			hashBld:       val.NewTupleBuilder(val.NewTupleDescriptor(val.Type{Enc: val.Hash128Enc}), m.NodeStore()),
			keyMap:        def.KeyMapping,
		}
	}

	return writers, nil
}

// Insert implements TableWriter.
func (w *prollyTableWriter) Insert(ctx *sql.Context, sqlRow sql.Row) (err error) {
	if err = w.primary.ValidateKeyViolations(ctx, sqlRow); err != nil {
		return err
	}
	for _, wr := range w.secondary {
		if err = wr.ValidateKeyViolations(ctx, sqlRow); err != nil {
			if uke, ok := err.(secondaryUniqueKeyError); ok {
				return w.primary.(primaryIndexErrBuilder).errForSecondaryUniqueKeyError(ctx, uke)
			}
		}
	}
	for _, wr := range w.secondary {
		if err = wr.Insert(ctx, sqlRow); err != nil {
			if uke, ok := err.(secondaryUniqueKeyError); ok {
				return w.primary.(primaryIndexErrBuilder).errForSecondaryUniqueKeyError(ctx, uke)
			}
			return err
		}
	}
	if err = w.primary.Insert(ctx, sqlRow); err != nil {
		return err
	}

	w.setAutoIncrement = true

	// TODO: need schema name in ai tracker
	w.aiTracker.Next(ctx, w.tableName.Name, sqlRow)
	return nil
}

// Delete implements TableWriter.
func (w *prollyTableWriter) Delete(ctx *sql.Context, sqlRow sql.Row) (err error) {
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
	for _, wr := range w.secondary {
		if err := wr.Update(ctx, oldRow, newRow); err != nil {
			if uke, ok := err.(secondaryUniqueKeyError); ok {
				return w.primary.(primaryIndexErrBuilder).errForSecondaryUniqueKeyError(ctx, uke)
			}
			return err
		}
	}
	if err := w.primary.Update(ctx, oldRow, newRow); err != nil {
		return err
	}

	w.setAutoIncrement = true
	return nil
}

// GetNextAutoIncrementValue implements TableWriter.
func (w *prollyTableWriter) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	return w.aiTracker.Next(ctx, w.tableName.Name, insertVal)
}

// SetAutoIncrementValue implements AutoIncrementSetter.
func (w *prollyTableWriter) SetAutoIncrementValue(ctx *sql.Context, val uint64) error {
	seq, err := w.aiTracker.CoerceAutoIncrementValue(ctx, val)
	if err != nil {
		return err
	}

	w.nextAutoIncrementValue = make(map[string]uint64)
	w.nextAutoIncrementValue[w.tableName.Name] = seq

	// The work above is persisted in flush
	return w.flush(ctx)
}

func (w *prollyTableWriter) AcquireAutoIncrementLock(ctx *sql.Context) (func(), error) {
	return w.aiTracker.AcquireTableLock(ctx, w.tableName.Name)
}

// Close implements Closer
func (w *prollyTableWriter) Close(ctx *sql.Context) error {
	// We discard data changes in DiscardChanges, but this doesn't include schema changes, which we don't want to flush
	if w.errEncountered == nil {
		return w.flush(ctx)
	}
	return nil
}

// StatementBegin implements TableWriter.
func (w *prollyTableWriter) StatementBegin(ctx *sql.Context) {
	// Table writers are reused in a session, which means we need to reset the error state resulting from previous
	// errors on every new statement.
	w.errEncountered = nil
	return
}

// DiscardChanges implements TableWriter.
func (w *prollyTableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	if _, ignored := errorEncountered.(sql.IgnorableError); !ignored {
		w.errEncountered = errorEncountered
	}
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

// GetIndexes implements sql.IndexAddressableTable.
func (w *prollyTableWriter) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	indexes := ctx.GetIndexRegistry().IndexesByTable(w.dbName, w.tableName.Name)
	ret := make([]sql.Index, len(indexes))
	for i := range indexes {
		ret[i] = indexes[i]
	}
	return ret, nil
}

func (w *prollyTableWriter) PreciseMatch() bool {
	return true
}

// IndexedAccess implements sql.IndexAddressableTable.
func (w *prollyTableWriter) IndexedAccess(_ *sql.Context, i sql.IndexLookup) sql.IndexedTable {
	idx := index.DoltIndexFromSqlIndex(i.Index)
	return &prollyFkIndexer{
		writer: w,
		index:  idx,
	}
}

// Reset puts the writer into a fresh state, updating the schema and index writers according to the newly given table.
func (w *prollyTableWriter) Reset(ctx *sql.Context, sess *prollyWriteSession, tbl *doltdb.Table, sch schema.Schema) error {
	schState, err := writerSchema(ctx, tbl, w.tableName.Name, w.dbName)
	if err != nil {
		return err
	}

	var newPrimary indexWriter

	var newSecondaries map[string]indexWriter
	if schema.IsKeyless(sch) {
		newPrimary, err = getPrimaryKeylessProllyWriter(ctx, tbl, schState)
		if err != nil {
			return err
		}
		newSecondaries, err = getSecondaryKeylessProllyWriters(ctx, tbl, schState, newPrimary.(prollyKeylessWriter))
		if err != nil {
			return err
		}
	} else {
		newPrimary, err = getPrimaryProllyWriter(ctx, tbl, schState)
		if err != nil {
			return err
		}
		newSecondaries, err = getSecondaryProllyIndexWriters(ctx, tbl, schState)
		if err != nil {
			return err
		}
	}

	w.tbl = tbl
	w.sch = sch
	w.sqlSch = schState.PkSchema.Schema
	w.primary = newPrimary
	w.secondary = newSecondaries
	w.aiCol = schState.AutoIncCol
	w.flusher = sess

	return nil
}

func (w *prollyTableWriter) table(ctx context.Context) (t *doltdb.Table, err error) {
	// flush primary row storage
	pm, err := w.primary.Map(ctx)
	if err != nil {
		return nil, err
	}

	t, err = w.tbl.UpdateRows(ctx, durable.IndexFromMapInterface(pm))
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
		idx := durable.IndexFromMapInterface(sm)

		s, err = s.PutIndex(ctx, wrSecondary.Name(), idx)
		if err != nil {
			return nil, err
		}
	}

	t, err = t.SetIndexSet(ctx, s)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (w *prollyTableWriter) flush(ctx *sql.Context) error {
	ws, err := w.flusher.FlushWithAutoIncrementOverrides(ctx, w.setAutoIncrement, w.nextAutoIncrementValue)
	if err != nil {
		return err
	}

	if w.targetStaging {
		return w.setter(ctx, w.dbName, ws.StagedRoot())
	} else {
		return w.setter(ctx, w.dbName, ws.WorkingRoot())
	}
}
