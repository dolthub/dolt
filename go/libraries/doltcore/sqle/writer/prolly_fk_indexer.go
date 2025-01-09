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
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyFkIndexer struct {
	writer   *prollyTableWriter
	index    index.DoltIndex
	pRange   prolly.Range
	refCheck bool
}

var _ sql.Table = (*prollyFkIndexer)(nil)
var _ sql.IndexedTable = (*prollyFkIndexer)(nil)
var _ sql.ReferenceChecker = (*prollyFkIndexer)(nil)

// Name implements the interface sql.Table.
func (n *prollyFkIndexer) Name() string {
	return n.writer.tableName.Name
}

// String implements the interface sql.Table.
func (n *prollyFkIndexer) String() string {
	return n.writer.tableName.Name
}

// Schema implements the interface sql.Table.
func (n *prollyFkIndexer) Schema() sql.Schema {
	return n.writer.sqlSch
}

func (n *prollyFkIndexer) SetReferenceCheck() error {
	n.refCheck = true
	return nil
}

// Collation implements the interface sql.Table.
func (n *prollyFkIndexer) Collation() sql.CollationID {
	return sql.CollationID(n.writer.sch.GetCollation())
}

func (n *prollyFkIndexer) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	ranges, err := index.ProllyRangesFromIndexLookup(ctx, lookup)
	if err != nil {
		return nil, err
	}
	n.pRange = ranges[0]
	return sql.PartitionsToPartitionIter(fkDummyPartition{}), nil
}

// Partitions implements the interface sql.Table.
func (n *prollyFkIndexer) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(fkDummyPartition{}), nil
}

// PartitionRows implements the interface sql.Table.
func (n *prollyFkIndexer) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	var idxWriter indexWriter
	var ok bool
	if n.index.IsPrimaryKey() {
		idxWriter = n.writer.primary
	} else if idxWriter, ok = n.writer.secondary[n.index.ID()]; !ok {
		return nil, fmt.Errorf("unable to find writer for index `%s`", n.index.ID())
	}

	pkToIdxMap := make(val.OrdinalMapping, n.writer.sch.GetPKCols().Size())
	for j, idxCol := range n.index.IndexSchema().GetPKCols().GetColumns() {
		if i, ok := n.writer.sch.GetPKCols().TagToIdx[idxCol.Tag]; ok {
			pkToIdxMap[i] = j
		}
	}
	rangeIter, err := idxWriter.IterRange(ctx, n.pRange)
	if err != nil {
		return nil, err
	}
	if primary, ok := n.writer.primary.(prollyIndexWriter); ok {
		return &prollyFkPkRowIter{
			rangeIter:  rangeIter,
			pkToIdxMap: pkToIdxMap,
			primary:    primary,
			sqlSch:     n.writer.sqlSch,
			refCheck:   n.refCheck,
		}, nil
	} else {
		return &prollyFkKeylessRowIter{
			rangeIter: rangeIter,
			primary:   n.writer.primary.(prollyKeylessWriter),
			sqlSch:    n.writer.sqlSch,
		}, nil
	}
}

// prollyFkPkRowIter returns rows of the parent table requested by a foreign key reference. For use on tables with primary keys.
type prollyFkPkRowIter struct {
	rangeIter  prolly.MapIter
	pkToIdxMap val.OrdinalMapping
	primary    prollyIndexWriter
	sqlSch     sql.Schema
	refCheck   bool
}

var _ sql.RowIter = prollyFkPkRowIter{}

// Next implements the interface sql.RowIter.
func (iter prollyFkPkRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		// |rangeIter| iterates on the foreign key index of the parent table
		k, _, err := iter.rangeIter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if k == nil {
			return nil, io.EOF
		}

		pkBld := iter.primary.keyBld
		for pkPos, idxPos := range iter.pkToIdxMap {
			pkBld.PutRaw(pkPos, k.GetField(idxPos))
		}
		pkTup := pkBld.BuildPermissive(sharePool)

		var tblKey, tblVal val.Tuple
		err = iter.primary.mut.Get(ctx, pkTup, func(k, v val.Tuple) error {
			tblKey, tblVal = k, v
			return nil
		})
		if err != nil {
			return nil, err
		}
		if tblKey == nil {
			continue // referential integrity broken
		}

		if iter.refCheck {
			// no need to deserialize
			return nil, nil
		}

		nextRow := make(sql.UntypedSqlRow, len(iter.primary.keyMap)+len(iter.primary.valMap))
		for from := range iter.primary.keyMap {
			to := iter.primary.keyMap.MapOrdinal(from)
			if nextRow[to], err = tree.GetField(ctx, iter.primary.keyBld.Desc, from, tblKey, iter.primary.mut.NodeStore()); err != nil {
				return nil, err
			}
		}
		for from := range iter.primary.valMap {
			to := iter.primary.valMap.MapOrdinal(from)
			if nextRow[to], err = tree.GetField(ctx, iter.primary.valBld.Desc, from, tblVal, iter.primary.mut.NodeStore()); err != nil {
				return nil, err
			}
		}
		return nextRow, nil
	}
}

// Close implements the interface sql.RowIter.
func (iter prollyFkPkRowIter) Close(ctx *sql.Context) error {
	return nil
}

// prollyFkKeylessRowIter returns rows requested by a foreign key reference. For use on keyless tables.
type prollyFkKeylessRowIter struct {
	rangeIter prolly.MapIter
	primary   prollyKeylessWriter
	sqlSch    sql.Schema
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
	hashId := k.GetField(k.Count() - 1)
	iter.primary.keyBld.PutHash128(0, hashId)
	primaryKey := iter.primary.keyBld.Build(sharePool)

	nextRow := make(sql.UntypedSqlRow, len(iter.primary.valMap))
	err = iter.primary.mut.Get(ctx, primaryKey, func(tblKey, tblVal val.Tuple) error {
		for from := range iter.primary.valMap {
			to := iter.primary.valMap.MapOrdinal(from)
			if nextRow[to], err = tree.GetField(ctx, iter.primary.valBld.Desc, from+1, tblVal, iter.primary.mut.NodeStore()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nextRow, nil
}

// Close implements the interface sql.RowIter.
func (iter prollyFkKeylessRowIter) Close(ctx *sql.Context) error {
	return nil
}
