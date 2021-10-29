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

package mvdata

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var _ DataLocation = ProllyDataLocation{}

// ProllyDataLocation is a dolt table that that can be imported from or exported to.
type ProllyDataLocation struct {
	// Name the name of a table
	Name string
}

// String returns a string representation of the rows location.
func (pl ProllyDataLocation) String() string {
	return DoltDB.ReadableStr() + ":" + pl.Name
}

// Exists returns true if the DataLocation already exists
func (pl ProllyDataLocation) Exists(ctx context.Context, root *doltdb.RootValue, _ filesys.ReadableFS) (bool, error) {
	return root.HasTable(ctx, pl.Name)
}

// NewReader creates a TableReadCloser for the DataLocation
func (pl ProllyDataLocation) NewReader(ctx context.Context, root *doltdb.RootValue, _ filesys.ReadableFS, _ interface{}) (rdCl table.TableReadCloser, sorted bool, err error) {
	panic("unimplemented")
}

// NewCreatingWriter will create a TableWriteCloser for a DataLocation that will create a new table, or overwrite
// an existing table.
func (pl ProllyDataLocation) NewCreatingWriter(ctx context.Context, _ DataMoverOptions, _ *env.DoltEnv, root *doltdb.RootValue, _ bool, outSch schema.Schema, statsCB noms.StatsCB, opts editor.Options) (table.TableWriteCloser, error) {
	root, err := root.CreateEmptyTable(ctx, pl.Name, outSch)
	if err != nil {
		return nil, err
	}

	tbl, ok, err := root.GetTable(ctx, pl.Name)
	if !ok {
		panic("couldn't find table")
	}
	if err != nil {
		return nil, err
	}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	keyDesc, valDesc := rows.TupleDescriptors()

	nrw := prolly.NewNodeStore(prolly.ChunkStoreFromVRW(root.VRW()))
	chunker := prolly.EmptyTreeChunkerFromMap(ctx, rows)

	return &prollyWriteCloser{
		name: pl.Name,
		sch:  outSch,
		kb:   val.NewTupleBuilder(keyDesc),
		vb:   val.NewTupleBuilder(valDesc),
		ch:   chunker,
		nrw:  nrw,
		root: root,
	}, nil
}

// NewUpdatingWriter will create a TableWriteCloser for a DataLocation that will update and append rows based on
// their primary key.
func (pl ProllyDataLocation) NewUpdatingWriter(ctx context.Context, _ DataMoverOptions, dEnv *env.DoltEnv, root *doltdb.RootValue, _ bool, wrSch schema.Schema, statsCB noms.StatsCB, rdTags []uint64, opts editor.Options) (table.TableWriteCloser, error) {
	panic("unimplemented")
}

// NewReplacingWriter will create a TableWriteCloser for a DataLocation that will overwrite an existing table while
// preserving schema
func (pl ProllyDataLocation) NewReplacingWriter(ctx context.Context, _ DataMoverOptions, _ *env.DoltEnv, _ *doltdb.RootValue, _ bool, _ schema.Schema, _ noms.StatsCB, _ editor.Options) (table.TableWriteCloser, error) {
	panic("unimplemented")
}

type prollyWriteCloser struct {
	name   string
	sch    schema.Schema
	kb, vb *val.TupleBuilder

	ch     *prolly.TreeChunker
	nrw    prolly.NodeReadWriter
	root   *doltdb.RootValue
}

var _ DataMoverCloser = (*prollyWriteCloser)(nil)

func (pw *prollyWriteCloser) Flush(ctx context.Context) (*doltdb.RootValue, error) {
	node, err := pw.ch.Done(ctx)
	if err != nil {
		return nil, err
	}

	m := prolly.NewMap(node, pw.nrw, pw.kb.Desc, pw.vb.Desc)

	tbl, ok, err := pw.root.GetTable(ctx, pw.name)
	if err != nil {
		return nil, err
	}
	if !ok {
		panic("couldn't find table")
	}

	tbl, err = tbl.UpdateRows(ctx, m)
	if err != nil {
		return nil, err
	}

	return pw.root.PutTable(ctx, pw.name, tbl)
}

// GetSchema implements TableWriteCloser
func (pw *prollyWriteCloser) GetSchema() schema.Schema {
	return pw.sch
}

// WriteRow implements TableWriteCloser
func (pw *prollyWriteCloser) WriteRow(ctx context.Context, r row.Row) (err error) {
	key, value := pw.tuplesFromRow(r)
	_, err = pw.ch.Append(ctx, key, value)
	return
}

// Close implements TableWriteCloser
func (pw *prollyWriteCloser) Close(ctx context.Context) error {
	return nil
}

func (pw *prollyWriteCloser) tuplesFromRow(r row.Row) (key, value val.Tuple) {
	idx := 0
	_ = pw.sch.GetPKCols().Iter(func(tag uint64, _ schema.Column) (stop bool, err error) {
		v, ok := r.GetColVal(tag)
		if ok {
			writeValue(pw.kb, idx, v)
		}
		idx++
		return
	})

	idx = 0
	_ = pw.sch.GetNonPKCols().Iter(func(tag uint64, _ schema.Column) (stop bool, err error) {
		v, ok := r.GetColVal(tag)
		if ok {
			writeValue(pw.vb, idx, v)
		}
		idx++
		return
	})

	key = pw.kb.Tuple(shared)
	value = pw.vb.Tuple(shared)
	return
}

var shared = pool.NewBuffPool()

func writeValue(builder *val.TupleBuilder, idx int, v types.Value) {
	switch tv := v.(type) {
	case types.Bool:
		builder.PutBool(idx, bool(tv))
	case types.Int:
		builder.PutInt64(idx, int64(8))
	case types.Uint:
		builder.PutUint64(idx, uint64(8))
	case types.Float:
		builder.PutFloat64(idx, float64(8))
	case types.String:
		builder.PutString(idx, string(tv))
	default:
		panic("unknown value type")
	}
}
