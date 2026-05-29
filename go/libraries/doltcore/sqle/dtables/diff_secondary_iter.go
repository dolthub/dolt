// Copyright 2026 Dolthub, Inc.
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

package dtables

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// prollySecondaryDiffIter iterates over diff results by diffing secondary index
// maps and looking up primary rows for each changed entry.
type prollySecondaryDiffIter struct {
	fromConverter ProllyRowConverter
	toConverter   ProllyRowConverter
	fromCm        commitInfo2
	toCm          commitInfo2

	targetFromSch schema.Schema
	targetToSch   schema.Schema

	rows    chan sql.Row
	errChan chan error
	cancel  context.CancelFunc

	fromSecondary prolly.Map
	toSecondary   prolly.Map
	fromPrimary   prolly.Map
	toPrimary     prolly.Map

	pkMap                 val.OrdinalMapping
	secondaryToPrimaryMap val.OrdinalMapping
	pkBld                 *val.TupleBuilder

	indexType index.SecondaryDiffIndexType
	ranges    []prolly.Range
}

var _ sql.RowIter = (*prollySecondaryDiffIter)(nil)

func (itr *prollySecondaryDiffIter) Next(ctx *sql.Context) (sql.Row, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-itr.errChan:
		return nil, err
	case row, ok := <-itr.rows:
		if !ok {
			return nil, io.EOF
		}
		return row, nil
	}
}

func (itr *prollySecondaryDiffIter) Close(_ *sql.Context) error {
	itr.cancel()
	return nil
}

func (itr *prollySecondaryDiffIter) queueRows(ctx context.Context) {
	cb := func(ctx context.Context, d tree.Diff) error {
		if itr.indexType == index.SecondaryDiffIndexType_To && d.Type == tree.RemovedDiff {
			return nil
		}
		if itr.indexType == index.SecondaryDiffIndexType_From && d.Type == tree.AddedDiff {
			return nil
		}

		idxKey := val.Tuple(d.Key)

		for to := range itr.pkMap {
			from := itr.pkMap.MapOrdinal(to)
			itr.pkBld.PutRaw(to, idxKey.GetField(from))
		}
		pk, err := itr.pkBld.Build(ctx, itr.toPrimary.Pool())
		if err != nil {
			return err
		}

		row, err := itr.populateRows(ctx, pk, d)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case itr.rows <- row:
			return nil
		}
	}

	for _, rng := range itr.ranges {
		err := prolly.RangeDiffMaps(ctx, itr.fromSecondary, itr.toSecondary, rng, cb)
		if err != nil && err != io.EOF {
			select {
			case <-ctx.Done():
			case itr.errChan <- err:
			}
			return
		}
	}
	close(itr.rows)
}

func (itr *prollySecondaryDiffIter) populateRows(ctx context.Context, pk val.Tuple, d tree.Diff) (row sql.Row, err error) {
	var valueFromLookup val.Tuple
	var valueExistsInLookup bool
	var primaryMapForLookup prolly.Map
	var indexedCommitInfo commitInfo2
	var lookupCommitInfo commitInfo2
	var indexedRow, indexedDetails, lookupRow, lookupDetails sql.Row
	secondaryMapKey := val.Tuple(d.Key)

	tLen := schemaSize(itr.targetToSch)
	fLen := schemaSize(itr.targetFromSch)

	if fLen == 0 && d.Type == tree.AddedDiff {
		fLen = tLen
	} else if tLen == 0 && d.Type == tree.RemovedDiff {
		tLen = fLen
	}

	// 2 commit names, 2 commit dates, 1 diff_type
	row = make(sql.Row, fLen+tLen+5)

	switch d.Type {
	case tree.ModifiedDiff:
		// Since secondary indexes only have key tuples, no value tuples, it's not possible for a diff on a secondary
		// index to contain modified rows, only adds and removes.
		return nil, fmt.Errorf("unexpected 'modified' diff when diffing secondary indexes")
	case tree.AddedDiff:
		primaryMapForLookup = itr.fromPrimary
		indexedCommitInfo = itr.toCm
		lookupCommitInfo = itr.fromCm
		indexedRow = row[0:tLen]
		indexedDetails = row[tLen : tLen+2]
		lookupRow = row[tLen+2 : tLen+2+fLen]
		lookupDetails = row[tLen+2+fLen:]

	case tree.RemovedDiff:
		primaryMapForLookup = itr.toPrimary
		indexedCommitInfo = itr.fromCm
		lookupCommitInfo = itr.toCm
		lookupRow = row[0:tLen]
		lookupDetails = row[tLen : tLen+2]
		indexedRow = row[tLen+2 : tLen+2+fLen]
		indexedDetails = row[tLen+2+fLen:]
	}
	err = primaryMapForLookup.Get(ctx, pk, func(key, value val.Tuple) error {
		if key != nil {
			valueFromLookup = value
			valueExistsInLookup = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	diffType := d.Type
	if valueExistsInLookup {
		diffType = tree.ModifiedDiff
	}

	for i, primaryPosition := range itr.secondaryToPrimaryMap {
		// If the column isn't present in the secondary index, it must be a generated column.
		// Currently, virtual generated columns are nil in the diff, so we can skip those.
		// TODO: For generated columns (virtual + stored), we should desire consistent behavior between indexed and non-indexed cases.
		// Currently, non-indexed / primary-indexed uses of DOLT_DIFF show values for stored generated columns and show NULL for virtual generated columns.
		// Whereas secondary indexed uses show values for generated columns in the secondary index and NULL for others.
		indexedRow[primaryPosition], err = tree.GetField(ctx, itr.toSecondary.KeyDesc(), i, secondaryMapKey, itr.toSecondary.NodeStore())
		if err != nil {
			return nil, err
		}
	}

	if diffType != tree.AddedDiff && valueExistsInLookup {
		err := itr.fromConverter.PutConverted(ctx, pk, valueFromLookup, lookupRow)
		if err != nil {
			return nil, err
		}
	}

	indexedDetails[0] = indexedCommitInfo.name
	indexedDetails[1] = maybeTime(indexedCommitInfo.ts)

	lookupDetails[0] = lookupCommitInfo.name
	lookupDetails[1] = maybeTime(lookupCommitInfo.ts)

	row[len(row)-1] = diffTypeStr(diffType)

	return row, err
}

func diffTypeStr(dt tree.DiffType) string {
	switch dt {
	case tree.AddedDiff:
		return "added"
	case tree.ModifiedDiff:
		return "modified"
	case tree.RemovedDiff:
		return "removed"
	}
	return ""
}

// SecondaryDiffPartition represents a partition for diff results obtained by
// diffing secondary index maps from two branches, then looking up the primary
// rows for each changed entry.
type SecondaryDiffPartition struct {
	toTable   *doltdb.Table
	fromTable *doltdb.Table
	toName    string
	fromName  string
	toDate    *types.Timestamp
	fromDate  *types.Timestamp
	toSch     schema.Schema
	fromSch   schema.Schema
	indexType index.SecondaryDiffIndexType
	indexName string
	ranges    []prolly.Range
}

func NewSecondaryDiffPartition(toTable *doltdb.Table,
	fromTable *doltdb.Table,
	toName string,
	fromName string,
	toDate *types.Timestamp,
	fromDate *types.Timestamp,
	toSch schema.Schema,
	fromSch schema.Schema,
	indexType index.SecondaryDiffIndexType,
	indexName string,
	ranges []prolly.Range) *SecondaryDiffPartition {
	return &SecondaryDiffPartition{
		toTable:   toTable,
		fromTable: fromTable,
		toName:    toName,
		fromName:  fromName,
		toDate:    toDate,
		fromDate:  fromDate,
		toSch:     toSch,
		fromSch:   fromSch,
		indexType: indexType,
		indexName: indexName,
		ranges:    ranges,
	}
}

var _ sql.Partition = (*SecondaryDiffPartition)(nil)

func (p *SecondaryDiffPartition) Key() []byte {
	return []byte(p.indexType.Prefix() + "_" + p.indexName + "_" + p.toName + "_" + p.fromName)
}

func (p *SecondaryDiffPartition) GetRowIter(ctx *sql.Context) (sql.RowIter, error) {
	// Get secondary index maps from both tables
	toIdxData, err := p.toTable.GetIndexRowData(ctx, p.indexName)
	if err != nil {
		return nil, err
	}
	toSecondary, err := durable.ProllyMapFromIndex(toIdxData)
	if err != nil {
		return nil, err
	}

	fromIdxData, err := p.fromTable.GetIndexRowData(ctx, p.indexName)
	if err != nil {
		return nil, err
	}
	fromSecondary, err := durable.ProllyMapFromIndex(fromIdxData)
	if err != nil {
		return nil, err
	}

	// Get primary maps from both tables
	toRowData, err := p.toTable.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	toPrimary, err := durable.ProllyMapFromIndex(toRowData)
	if err != nil {
		return nil, err
	}

	fromRowData, err := p.fromTable.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	fromPrimary, err := durable.ProllyMapFromIndex(fromRowData)
	if err != nil {
		return nil, err
	}

	// Build PK ordinal mapping: maps PK ordinal -> secondary key ordinal
	idxDef := p.toSch.Indexes().GetByName(p.indexName)
	pkMap := schema.OrdinalToPKOrdinal(idxDef)

	secondaryToPrimaryMap := schema.OrdinalToPrimaryOrdinal(p.toSch, idxDef)

	// Build PK tuple builder from primary map's key descriptor
	pkKd, _ := toPrimary.Descriptors()
	pkBld := val.NewTupleBuilder(pkKd, toPrimary.NodeStore())

	// Build converters
	var nodeStore tree.NodeStore
	nodeStore = p.toTable.NodeStore()

	toConverter, err := NewProllyRowConverter(ctx, p.toSch, p.toSch, ctx.Warn, nodeStore)
	if err != nil {
		return nil, err
	}
	fromConverter, err := NewProllyRowConverter(ctx, p.fromSch, p.fromSch, ctx.Warn, nodeStore)
	if err != nil {
		return nil, err
	}

	child, cancel := context.WithCancel(ctx)
	iter := &prollySecondaryDiffIter{
		fromSecondary:         fromSecondary,
		toSecondary:           toSecondary,
		fromPrimary:           fromPrimary,
		toPrimary:             toPrimary,
		pkMap:                 pkMap,
		pkBld:                 pkBld,
		secondaryToPrimaryMap: secondaryToPrimaryMap,
		fromConverter:         fromConverter,
		toConverter:           toConverter,
		targetFromSch:         p.fromSch,
		targetToSch:           p.toSch,
		fromCm:                commitInfo2{name: p.fromName, ts: (*time.Time)(p.fromDate)},
		toCm:                  commitInfo2{name: p.toName, ts: (*time.Time)(p.toDate)},
		indexType:             p.indexType,
		ranges:                p.ranges,
		rows:                  make(chan sql.Row, 64),
		errChan:               make(chan error),
		cancel:                cancel,
	}

	go func() {
		iter.queueRows(child)
	}()

	return iter, nil
}
