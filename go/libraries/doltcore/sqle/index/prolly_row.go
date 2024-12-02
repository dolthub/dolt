// Copyright 2024 Dolthub, Inc.
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

package index

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
)

func NewProllyRow(key, val val.Tuple, kd, vd val.TupleDesc, ords []int, ns tree.NodeStore) *ProllyRow {
	// TODO pool and reuse
	return &ProllyRow{key: key, value: val, kd: kd, vd: vd, ords: ords, ns: ns}
}

// two options
// store in row order -- save space, conversion already performed
// or store tuples and be able to reconstruct row order -- save time, might not need to materialize
type ProllyRow struct {
	key, value val.Tuple
	kd, vd     val.TupleDesc
	ords       []int // placement access
	mat        sql.UntypedSqlRow
	decoded    sql.FastIntSet
	dirty      sql.FastIntSet
	ns         tree.NodeStore
}

var _ sql.Row = (*ProllyRow)(nil)
var _ sql.BytesRow = (*ProllyRow)(nil)

func (r *ProllyRow) GetBytes(i int, typ sql.Type) ([]byte, error) {
	if i > len(r.ords) {
		return nil, fmt.Errorf("invalid index for value: %d:%T", i, typ)
	}
	if r.dirty.Contains(i + 1) {
		// todo convert from SQL to bytes
		return nil, fmt.Errorf("todo add dirty field val->bytes 	conversion")
	}

	pos := r.ords[i]

	// TODO: type normalization
	// if descriptor and input type are a mismatch need conversion

	// TODO: virtual columns
	// position might be filled by expression

	// TODO: recycle these objects?

	if pos < r.kd.Count() {
		ret := r.kd.GetField(pos, r.key)
		return ret, nil
	}

	pos -= r.kd.Count()
	ret := r.vd.GetField(pos, r.value)
	return ret, nil
}

func (r *ProllyRow) decode(ctx context.Context, i int) {
	if r.mat == nil {
		r.mat = make(sql.UntypedSqlRow, r.Len())
	}
	var val interface{}
	pos := r.ords[i]
	if pos < r.kd.Count() {
		val, _ = tree.GetField(ctx, r.kd, pos, r.key, r.ns)
	} else {
		val, _ = tree.GetField(ctx, r.vd, pos-r.kd.Count(), r.value, r.ns)
	}
	r.mat[i] = val
	r.decoded.Add(i + 1)
}

func (r *ProllyRow) GetValue(i int) interface{} {
	if !r.decoded.Contains(i + 1) {
		r.decode(context.Background(), i)
	}
	return r.mat[i]
}

func (r *ProllyRow) SetValue(i int, v interface{}) {
	if r.mat == nil {
		r.mat = make(sql.UntypedSqlRow, r.Len())
	}
	r.mat[i] = v
	r.dirty.Add(i + 1)
}

func (r *ProllyRow) SetBytes(i int, v []byte) {
	//TODO implement me
	panic("implement me")
}

func (r *ProllyRow) GetType(i int) {
	//TODO implement me
	panic("implement me")
}

func (r *ProllyRow) Values() []interface{} {
	for i := range r.ords {
		if !r.decoded.Contains(i + 1) {
			r.decode(context.Background(), i)
		}
	}
	return r.mat
}

func (r *ProllyRow) Copy() sql.Row {
	if r.dirty.Len() > 0 {
		return sql.UntypedSqlRow(r.Values())
	}
	return &ProllyRow{key: r.key, value: r.value, kd: r.kd, vd: r.vd, ords: r.ords}
}

func (r *ProllyRow) Len() int {
	return len(r.ords)
}

func (r *ProllyRow) Subslice(i, j int) sql.Row {
	_ = r.Values()
	return r.mat[i:j]
}

func (r *ProllyRow) Append(row sql.Row) sql.Row {
	return sql.NewUntypedRow(append(r.Values(), row.Values()...))
}

func (r *ProllyRow) Equals(row sql.Row, schema sql.Schema) (bool, error) {
	//TODO implement me
	panic("implement me")
}
