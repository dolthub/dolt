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
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
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
func (pl ProllyDataLocation) NewCreatingWriter(ctx context.Context, _ DataMoverOptions, root *doltdb.RootValue, _ bool, outSch schema.Schema, statsCB noms.StatsCB, opts editor.Options, _ io.WriteCloser) (table.TableWriteCloser, error) {
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
	keyDesc, valDesc := rows.Descriptors()

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
func (pl ProllyDataLocation) NewUpdatingWriter(ctx context.Context, _ DataMoverOptions, root *doltdb.RootValue, _ bool, wrSch schema.Schema, statsCB noms.StatsCB, rdTags []uint64, opts editor.Options) (table.TableWriteCloser, error) {
	panic("unimplemented")
}

// NewReplacingWriter will create a TableWriteCloser for a DataLocation that will overwrite an existing table while
// preserving schema
func (pl ProllyDataLocation) NewReplacingWriter(ctx context.Context, _ DataMoverOptions, _ *doltdb.RootValue, _ bool, _ schema.Schema, _ noms.StatsCB, _ editor.Options) (table.TableWriteCloser, error) {
	panic("unimplemented")
}

type prollyWriteCloser struct {
	name   string
	sch    schema.Schema
	kb, vb *val.TupleBuilder

	ch   *prolly.TreeChunker
	nrw  prolly.NodeStore
	root *doltdb.RootValue
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

	k, v := pw.kb.Desc.Format(key), pw.vb.Desc.Format(value)
	fmt.Println(k, v)

	_, err = pw.ch.Append(ctx, []byte(key), []byte(value))
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

	key = pw.kb.Build(shared)
	value = pw.vb.Build(shared)
	return
}

var shared = pool.NewBuffPool()

func writeValue(builder *val.TupleBuilder, idx int, v types.Value) {
	enc := builder.Desc.Types[idx].Enc
	switch enc {
	case val.Int8Enc:
		builder.PutInt8(idx, int8(v.(types.Int)))
	case val.Uint8Enc:
		builder.PutUint8(idx, uint8(v.(types.Uint)))
	case val.Int16Enc:
		builder.PutInt16(idx, int16(v.(types.Int)))
	case val.Uint16Enc:
		builder.PutUint16(idx, uint16(v.(types.Uint)))
	case val.Int24Enc:
		panic("24 bit")
	case val.Uint24Enc:
		panic("24 bit")
	case val.Int32Enc:
		builder.PutInt32(idx, int32(v.(types.Int)))
	case val.Uint32Enc:
		builder.PutUint32(idx, uint32(v.(types.Uint)))
	case val.Int64Enc:
		builder.PutInt64(idx, int64(v.(types.Int)))
	case val.Uint64Enc:
		builder.PutUint64(idx, uint64(v.(types.Uint)))
	case val.Float32Enc:
		builder.PutFloat32(idx, float32(v.(types.Float)))
	case val.Float64Enc:
		builder.PutFloat64(idx, float64(v.(types.Float)))
	case val.BytesEnc:
		builder.PutBytes(idx, []byte(v.(types.InlineBlob)))
	case val.StringEnc:
		builder.PutString(idx, string(v.(types.String)))
	default:
		panic("unknown encoding")
	}
}
