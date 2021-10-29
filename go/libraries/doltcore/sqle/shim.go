// Copyright 2021 Dolthub, Inc.
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

package sqle

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type sqlRowIter struct {
	ctx     context.Context
	iter    prolly.MapIter
	keyDesc val.TupleDesc
	valDesc val.TupleDesc
}

var _ sql.RowIter = sqlRowIter{}

func newKeyedRowIter(ctx context.Context, tbl *doltdb.Table, projectedCols []string, partition *doltTablePartition) (sql.RowIter, error) {
	rows := partition.rowData
	rng := prolly.IndexRange{
		Low:  partition.start,
		High: partition.end - 1,
	}
	if partition.end == NoUpperBound {
		rng.Low, rng.High = 0, rows.Count()-1
	}

	iter, err := rows.IterIndexRange(ctx, rng)
	if err != nil {
		return nil, err
	}

	kd, vd := rows.TupleDescriptors()

	if len(projectedCols) != (kd.Count() + vd.Count()) {
		panic("projection")
	}

	return sqlRowIter{
		ctx:     ctx,
		iter:    iter,
		keyDesc: kd,
		valDesc: vd,
	}, nil
}

func (it sqlRowIter) Next() (sql.Row, error) {
	key, value, err := it.iter.Next(it.ctx)
	if err != nil {
		return nil, err
	}
	kc := key.Count()

	k, v := it.keyDesc.Format(key), it.valDesc.Format(value)
	fmt.Println(k, v)

	row := make(sql.Row, kc+value.Count())
	for i := range row {
		if i < key.Count() {
			row[i] = it.keyDesc.GetField(i, key)
		} else {
			row[i] = it.valDesc.GetField(i-kc, value)
		}
	}

	return row, nil
}

func (it sqlRowIter) Close(ctx *sql.Context) error {
	return nil
}
