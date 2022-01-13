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
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// prollyWriter is a wrapper for *doltdb.SessionedTableEditor that complies with the SQL interface.
//
// The prollyWriter has two levels of batching: one supported at the SQL engine layer where a single UPDATE, DELETE or
// INSERT statement will touch many rows, and we want to avoid unnecessary intermediate writes; and one at the dolt
// layer as a "batch mode" in DoltDatabase. In the latter mode, it's possible to mix inserts, updates and deletes in any
// order. In general, this is unsafe and will produce incorrect results in many cases. The editor makes reasonable
// attempts to produce correct results when interleaving insert and delete statements, but this is almost entirely to
// support REPLACE statements, which are implemented as a DELETE followed by an INSERT. In general, not flushing the
// editor after every SQL statement is incorrect and will return incorrect results. The single reliable exception is an
// unbroken chain of INSERT statements, where we have taken pains to batch writes to speed things up.
type prollyWriter struct {
	tableName string
	dbName    string
	sch       schema.Schema
	mut       mutableProllyIndex

	tbl        *doltdb.Table
	autoIncCol schema.Column
	aiTracker  globalstate.AutoIncrementTracker

	sess    WriteSession
	setter  SessionRootSetter
	batched bool
}

var _ TableWriter = &prollyWriter{}

func (w *prollyWriter) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	if schema.IsKeyless(w.sch) {
		return errors.New("operation unsupported")
	}
	return w.mut.Insert(ctx, sqlRow)
}

func (w *prollyWriter) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	if schema.IsKeyless(w.sch) {
		return errors.New("operation unsupported")
	}
	return w.mut.Delete(ctx, sqlRow)
}

func (w *prollyWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	if schema.IsKeyless(w.sch) {
		return errors.New("operation unsupported")
	}
	return w.mut.Update(ctx, oldRow, newRow)
}

func (w *prollyWriter) NextAutoIncrementValue(potentialVal, tableVal interface{}) (interface{}, error) {
	return w.aiTracker.Next(w.tableName, potentialVal, tableVal)
}

func (w *prollyWriter) GetAutoIncrementValue() (interface{}, error) {
	v, err := w.tbl.GetAutoIncrementValue(context.Background())
	if err != nil {
		return nil, err
	}
	return w.autoIncCol.TypeInfo.ConvertNomsValueToValue(v)
}

func (w *prollyWriter) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
	panic("unimplemented")
}

// Close implements Closer
func (w *prollyWriter) Close(ctx *sql.Context) error {
	// If we're running in batched mode, don'tbl flush the edits until explicitly told to do so
	if w.batched {
		return nil
	}

	return w.flush(ctx)
}

// StatementBegin implements the interface sql.TableEditor.
func (w *prollyWriter) StatementBegin(ctx *sql.Context) {
	// todo(andy)
	return
}

// DiscardChanges implements the interface sql.TableEditor.
func (w *prollyWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	// todo(andy)
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (w *prollyWriter) StatementComplete(ctx *sql.Context) error {
	// todo(andy)
	return nil
}

func (w *prollyWriter) table(ctx context.Context) (*doltdb.Table, error) {
	m, err := w.mut.Map(ctx)
	if err != nil {
		return nil, err
	}
	return w.tbl.UpdateRows(ctx, durable.IndexFromProllyMap(m))
}

func (w *prollyWriter) flush(ctx *sql.Context) error {
	newRoot, err := w.sess.Flush(ctx)
	if err != nil {
		return err
	}

	return w.setter(ctx, w.dbName, newRoot)
}

type mutableProllyIndex struct {
	mut prolly.MutableMap

	keyBld *val.TupleBuilder
	keyMap colMapping

	valBld *val.TupleBuilder
	valMap colMapping
}

func makeMutableProllyIndex(m prolly.Map, sqlSch sql.Schema, sch schema.Schema) mutableProllyIndex {
	keyDesc, valDesc := m.Descriptors()
	keyMap, valMap := colMappingsFromSchema(sqlSch, sch)

	return mutableProllyIndex{
		mut:    m.Mutate(),
		keyBld: val.NewTupleBuilder(keyDesc),
		keyMap: keyMap,
		valBld: val.NewTupleBuilder(valDesc),
		valMap: valMap,
	}
}

var sharePool = pool.NewBuffPool()

func (m mutableProllyIndex) Map(ctx context.Context) (prolly.Map, error) {
	return m.mut.Map(ctx)
}

func (m mutableProllyIndex) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	// todo(andy) need to check key?

	for to, from := range m.keyMap {
		m.keyBld.PutField(to, sqlRow[from])
	}
	k := m.keyBld.Build(sharePool)

	for to, from := range m.valMap {
		m.valBld.PutField(to, sqlRow[from])
	}
	v := m.valBld.Build(sharePool)

	return m.mut.Put(ctx, k, v)
}

func (m mutableProllyIndex) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	for to, from := range m.keyMap {
		m.keyBld.PutField(to, sqlRow[from])
	}
	k := m.keyBld.Build(sharePool)

	return m.mut.Delete(ctx, k)
}

func (m mutableProllyIndex) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	// todo(andy) need to delete?
	// todo(andy) need to check key?

	for to, from := range m.keyMap {
		m.keyBld.PutField(to, newRow[from])
	}
	k := m.keyBld.Build(sharePool)

	for to, from := range m.valMap {
		m.valBld.PutField(to, newRow[from])
	}
	v := m.valBld.Build(sharePool)

	return m.mut.Put(ctx, k, v)
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

func debugPrintIndexes(r *doltdb.RootValue) string {
	if r == nil {
		return ""
	}

	ctx := context.Background()
	sb := strings.Builder{}

	_ = r.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		sb.WriteString("table: ")
		sb.WriteString(name)
		sb.WriteRune('\n')

		pk, _ := table.GetRowData(ctx)
		if pk.Count() > 0 {
			sb.WriteRune('\t')
			sb.WriteString(fmt.Sprintf("primary (%d): ", pk.Count()))
			sb.WriteString(printIndexRowsInline(ctx, pk))
			sb.WriteRune('\n')
		}

		return false, nil
	})

	return sb.String()
}

func printIndexRowsInline(ctx context.Context, m durable.Index) string {
	pm := durable.ProllyMapFromIndex(m)
	kd, vd := pm.Descriptors()

	sb := strings.Builder{}
	iter, _ := pm.IterAll(ctx)
	for {
		k, v, err := iter.Next(ctx)
		if err == io.EOF {
			return sb.String()
		}
		if err != nil {
			panic(err)
		}

		sb.WriteString("\t")
		sb.WriteString(kd.Format(k))
		sb.WriteString(" ")
		sb.WriteString(vd.Format(v))
		sb.WriteString("\n")
	}
}
