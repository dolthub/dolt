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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyFkIndexer struct {
	writer *prollyTableWriter
	index  index.DoltIndex
	pRange prolly.Range
}

var _ sql.Table = prollyFkIndexer{}

// Name implements the interface sql.Table.
func (n prollyFkIndexer) Name() string {
	return n.writer.tableName
}

// String implements the interface sql.Table.
func (n prollyFkIndexer) String() string {
	return n.writer.tableName
}

// Schema implements the interface sql.Table.
func (n prollyFkIndexer) Schema() sql.Schema {
	return n.writer.sqlSch
}

// Partitions implements the interface sql.Table.
func (n prollyFkIndexer) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(fkDummyPartition{}), nil
}

// PartitionRows implements the interface sql.Table.
func (n prollyFkIndexer) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	var idxWriter prollyIndexWriter
	for _, secondaryWriter := range n.writer.secondary {
		if secondaryWriter.name == n.index.ID() {
			idxWriter = secondaryWriter
			break
		}
	}
	if idxWriter.name == "" {
		return nil, fmt.Errorf("unable to find writer for index `%s`", n.index.ID())
	}
	rangeIter, err := idxWriter.mut.IterRange(ctx, n.pRange)
	if err != nil {
		return nil, err
	}

	idxToPkMap := make(map[int]int)
	pkColToOrdinal := make(map[int]int)
	for i, ord := range n.writer.sch.GetPkOrdinals() {
		pkColToOrdinal[ord] = i
	}
	for idxPos, idxCol := range n.index.IndexSchema().GetAllCols().GetColumns() {
		if tblIdx, ok := n.writer.sch.GetPKCols().TagToIdx[idxCol.Tag]; ok {
			idxToPkMap[idxPos] = pkColToOrdinal[tblIdx]
		}
	}

	if primary, ok := n.writer.primary.(prollyIndexWriter); ok {
		return &prollyFkPkRowIter{
			rangeIter:  rangeIter,
			idxToPkMap: idxToPkMap,
			primary:    primary,
		}, nil
	} else {
		return &prollyFkKeylessRowIter{
			rangeIter: rangeIter,
			primary:   n.writer.primary.(prollyKeylessWriter),
		}, nil
	}
}

// prollyFkPkRowIter returns rows requested by a foreign key reference. For use on tables with primary keys.
type prollyFkPkRowIter struct {
	rangeIter  prolly.MapIter
	idxToPkMap map[int]int
	primary    prollyIndexWriter
}

var _ sql.RowIter = prollyFkPkRowIter{}

// Next implements the interface sql.RowIter.
func (iter prollyFkPkRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	k, _, err := iter.rangeIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	if k == nil {
		return nil, io.EOF
	}

	pkBld := iter.primary.keyBld
	for idxPos, pkPos := range iter.idxToPkMap {
		pkBld.PutRaw(pkPos, k.GetField(idxPos))
	}
	pkTup := pkBld.BuildPermissive(sharePool)

	nextRow := make(sql.Row, len(iter.primary.keyMap)+len(iter.primary.valMap))
	err = iter.primary.mut.Get(ctx, pkTup, func(tblKey, tblVal val.Tuple) error {
		for from := range iter.primary.keyMap {
			to := iter.primary.keyMap.MapOrdinal(from)
			if nextRow[to], err = index.GetField(iter.primary.keyBld.Desc, from, tblKey); err != nil {
				return err
			}
		}
		for from := range iter.primary.valMap {
			to := iter.primary.valMap.MapOrdinal(from)
			if nextRow[to], err = index.GetField(iter.primary.valBld.Desc, from, tblVal); err != nil {
				return err
			}
		}
		return nil
	})
	return nextRow, err
}

// Close implements the interface sql.RowIter.
func (iter prollyFkPkRowIter) Close(ctx *sql.Context) error {
	return nil
}

// prollyFkKeylessRowIter returns rows requested by a foreign key reference. For use on keyless tables.
type prollyFkKeylessRowIter struct {
	rangeIter prolly.MapIter
	primary   prollyKeylessWriter
}

var _ sql.RowIter = prollyFkKeylessRowIter{}

// Next implements the interface sql.RowIter.
func (iter prollyFkKeylessRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	k, _, err := iter.rangeIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	if k == nil {
		return nil, io.EOF
	}

	nextRow := make(sql.Row, len(iter.primary.valMap))
	err = iter.primary.mut.Get(ctx, k, func(tblKey, tblVal val.Tuple) error {
		for from := range iter.primary.valMap {
			to := iter.primary.valMap.MapOrdinal(from)
			if nextRow[to], err = index.GetField(iter.primary.valBld.Desc, from+1, tblVal); err != nil {
				return err
			}
		}
		return nil
	})
	return nextRow, err
}

// Close implements the interface sql.RowIter.
func (iter prollyFkKeylessRowIter) Close(ctx *sql.Context) error {
	return nil
}
