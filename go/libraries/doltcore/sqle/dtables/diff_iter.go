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

package dtables

import (
	"context"
	"io"
	"slices"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type commitInfo2 struct {
	ts   *time.Time
	name string
}

type prollyDiffIter struct {
	fromConverter ProllyRowConverter
	toConverter   ProllyRowConverter
	fromCm        commitInfo2
	toCm          commitInfo2

	fromSch       schema.Schema
	toSch         schema.Schema
	targetFromSch schema.Schema
	targetToSch   schema.Schema

	rows    chan sql.Row
	errChan chan error
	cancel  context.CancelFunc

	from   prolly.Map
	to     prolly.Map
	ranges []prolly.Range

	// fromUnpaired holds the from rows that a primary key change left unpaired.
	fromUnpaired diff.KeyedRows
	// toUnpaired holds the to rows that a primary key change left unpaired.
	toUnpaired diff.KeyedRows
	// movedMembers holds the reordered columns, whose rows change bytes but
	// not values.
	movedMembers diff.MovedMembers
	// fromUnpairedConverter converts the rows of fromUnpaired, which keep the
	// schema the from side had before rekeying.
	fromUnpairedConverter ProllyRowConverter
	// toUnpairedConverter converts the rows of toUnpaired, which keep the
	// schema the to side had before rekeying.
	toUnpairedConverter ProllyRowConverter

	keyless bool
}

var _ sql.RowIter = prollyDiffIter{}

// newProllyDiffIter produces dolt_diff system table and dolt_diff table
// function rows. The rows first have the "to" columns on the left and the
// "from" columns on the right. After the "to" and "from" columns, a commit
// name, and commit date is also present. The final column is the diff_type
// column.
//
// An example: to_pk, to_col1, to_commit, to_commit_date, from_pk, from_col1, from_commit, from_commit_date, diff_type
//
// |targetFromSchema| and |targetToSchema| defines what the schema should be for
// the row data on the "from" or "to" side. In the above example, both schemas are
// identical with two columns "pk" and "col1". The dolt diff table function for
// example can provide two different schemas.
//
// The |from| and |to| tables in the DiffPartition may have different schemas
// than |targetFromSchema| or |targetToSchema|. We convert the rows from the
// schema of |from| to |targetFromSchema| and the schema of |to| to
// |targetToSchema|. See the tablediff_prolly package.
func newProllyDiffIter(ctx *sql.Context, dp DiffPartition, targetFromSchema, targetToSchema schema.Schema, ranges []prolly.Range) (prollyDiffIter, error) {
	fromCm := commitInfo2{
		name: dp.fromName,
		ts:   (*time.Time)(dp.fromDate),
	}
	toCm := commitInfo2{
		name: dp.toName,
		ts:   (*time.Time)(dp.toDate),
	}
	var from, to prolly.Map

	var fsch schema.Schema = schema.EmptySchema
	if dp.from != nil {
		idx, err := dp.from.GetRowData(ctx)
		if err != nil {
			return prollyDiffIter{}, err
		}
		from, err = durable.ProllyMapFromIndex(idx)
		if err != nil {
			return prollyDiffIter{}, err
		}
		if fsch, err = dp.from.GetSchema(ctx); err != nil {
			return prollyDiffIter{}, err
		}
	}

	var tsch schema.Schema = schema.EmptySchema
	if dp.to != nil {
		idx, err := dp.to.GetRowData(ctx)
		if err != nil {
			return prollyDiffIter{}, err
		}
		to, err = durable.ProllyMapFromIndex(idx)
		if err != nil {
			return prollyDiffIter{}, err
		}
		if tsch, err = dp.to.GetSchema(ctx); err != nil {
			return prollyDiffIter{}, err
		}
	}

	if schema.ComparePrimaryKeys(fsch, tsch) == schema.PrimaryKeysRetagged {
		ctx.Warn(PrimaryKeyChangeWarningCode, retaggedKeyWarning, dp.fromName, dp.toName)
	}
	// [prolly.DiffMaps] pairs rows by key bytes, so both sides need one key.
	fromAligned, toAligned, err := diff.AlignPrimaryKeys(ctx, diff.KeyedRows{Rows: from, Sch: fsch, Height: dp.fromHeight}, diff.KeyedRows{Rows: to, Sch: tsch, Height: dp.toHeight})
	if err != nil {
		return prollyDiffIter{}, err
	}
	from, fsch, to, tsch = fromAligned.Rows, fromAligned.Sch, toAligned.Rows, toAligned.Sch
	// The ranges narrow only rows keyed by the key of dp.rangesSch. Rows
	// keyed otherwise are diffed in full, and the filter above the scan
	// selects them.
	if dp.rangesSch == nil || !slices.Equal(tsch.GetPKCols().Tags, dp.rangesSch.GetPKCols().Tags) {
		ranges = nil
	}
	var nodeStore tree.NodeStore
	if dp.to != nil {
		nodeStore = dp.to.NodeStore()
	} else {
		nodeStore = dp.from.NodeStore()
	}

	fromConverter, err := NewProllyRowConverter(ctx, fsch, targetFromSchema, ctx.Warn, nodeStore)
	if err != nil {
		return prollyDiffIter{}, err
	}

	toConverter, err := NewProllyRowConverter(ctx, tsch, targetToSchema, ctx.Warn, nodeStore)
	if err != nil {
		return prollyDiffIter{}, err
	}

	// Rows whose members moved can change while their bytes do not, so they
	// are diffed in full. A range diff compares bytes, so the filter above the
	// scan selects them instead.
	movedMembers := diff.NewMovedMembers(nodeStore, fsch, tsch)
	if movedMembers.HasMoved() {
		ranges = nil
	}

	// A diff shows unpaired rows as removed or added rather than modified,
	// which the warning explains. They convert from the schema they are
	// stored with, so a value the new key cannot hold still shows.
	var fromUnpairedConverter, toUnpairedConverter ProllyRowConverter
	fromN, toN := diff.CountUnpaired(fromAligned, toAligned)
	if fromN+toN > 0 {
		ctx.Warn(PrimaryKeyChangeWarningCode, UnpairedRowsWarning, dp.fromName, dp.toName)
	}
	if fromN > 0 {
		fromUnpairedConverter, err = NewProllyRowConverter(ctx, fromAligned.UnpairedSch(), targetFromSchema, ctx.Warn, nodeStore)
		if err != nil {
			return prollyDiffIter{}, err
		}
	}
	if toN > 0 {
		toUnpairedConverter, err = NewProllyRowConverter(ctx, toAligned.UnpairedSch(), targetToSchema, ctx.Warn, nodeStore)
		if err != nil {
			return prollyDiffIter{}, err
		}
	}

	keyless := schema.IsKeyless(targetFromSchema) && schema.IsKeyless(targetToSchema)
	child, cancel := context.WithCancel(ctx)
	iter := prollyDiffIter{
		from:          from,
		to:            to,
		fromSch:       fsch,
		toSch:         tsch,
		targetFromSch: targetFromSchema,
		targetToSch:   targetToSchema,
		fromConverter: fromConverter,
		toConverter:   toConverter,
		keyless:       keyless,
		fromCm:        fromCm,
		toCm:          toCm,
		fromUnpaired:  fromAligned,
		toUnpaired:    toAligned,
		ranges:        ranges,
		rows:          make(chan sql.Row, 64),
		errChan:       make(chan error),
		cancel:        cancel,

		movedMembers:          movedMembers,
		fromUnpairedConverter: fromUnpairedConverter,
		toUnpairedConverter:   toUnpairedConverter,
	}

	go func() {
		iter.queueRows(child)
	}()

	return iter, nil
}

// copyIndexLookupWithoutCommitRanges returns a copy of the specified |lookup|, with the lookup ranges for to_commit
// and from_commit removed. This allows the lookup ranges to be used to filter on the primary key index for the
// underlying table (if a third range for the primary key has been provided).
func copyIndexLookupWithoutCommitRanges(lookup sql.IndexLookup) sql.IndexLookup {
	if lookup.Ranges == nil {
		return lookup
	}

	// The first two columns of the index (to_commit and from_commit) don't actually exist on the underlying table.
	// We strip them out here so that we can generate the correct ranges for the eventual Diff.
	rangesCopy := make(sql.MySQLRangeCollection, 0)
	for i := range lookup.Ranges.(sql.MySQLRangeCollection) {
		rangesCopy = append(rangesCopy, lookup.Ranges.(sql.MySQLRangeCollection)[i][2:])
	}
	return sql.NewIndexLookup(lookup.Index, rangesCopy,
		lookup.IsPointLookup, lookup.IsEmptyRange, lookup.IsSpatialLookup, lookup.IsReverse)
}

func (itr prollyDiffIter) Next(ctx *sql.Context) (sql.Row, error) {
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

func (itr prollyDiffIter) Close(ctx *sql.Context) error {
	itr.cancel()
	return nil
}

// queueRows sends the rows of diffRows to the rows channel, and closes it. It
// sends the first error to errChan instead.
func (itr prollyDiffIter) queueRows(ctx context.Context) {
	if err := itr.diffRows(ctx); err != nil {
		select {
		case <-ctx.Done():
		case itr.errChan <- err:
		}
		return
	}
	// we need to drain itr.rows before returning io.EOF
	close(itr.rows)
}

// diffRows queues the diff of from and to within ranges, then the unpaired
// rows, and returns the first error.
func (itr prollyDiffIter) diffRows(ctx context.Context) error {
	// TODO: Determine whether or not the schema has changed. If it has, then all rows should count as modifications in the diff.
	if itr.ranges == nil {
		considerAllRowsModified := itr.movedMembers.HasMoved()
		err := prolly.DiffMaps(ctx, itr.from, itr.to, considerAllRowsModified, itr.queueDiff)
		if err != nil && err != io.EOF {
			return err
		}
	}
	isEveryRowPassed := itr.ranges == nil
	for _, rng := range itr.ranges {
		// if the filter can match all NILs, then we need to return every added/removed diff
		matches, err := rng.Matches(ctx, val.EmptyTuple)
		if err != nil {
			return err
		}
		if matches {
			isEveryRowPassed = true
			err = prolly.DiffMaps(ctx, itr.from, itr.to, false, itr.queueDiff)
		} else {
			err = prolly.RangeDiffMaps(ctx, itr.from, itr.to, rng, itr.queueDiff)
		}
		if err != nil && err != io.EOF {
			return err
		}
	}
	return itr.queueUnpaired(ctx, isEveryRowPassed)
}

// queueDiff sends the diff table rows of |d| to the rows channel.
//
// It sends nothing for a modified row whose values did not change.
func (itr prollyDiffIter) queueDiff(ctx context.Context, d tree.Diff) error {
	isUnchanged, err := itr.movedMembers.IsUnchanged(ctx, d)
	if err != nil || isUnchanged {
		return err
	}
	dItr, err := itr.makeDiffRowItr(ctx, d)
	if err != nil {
		return err
	}
	for {
		r, err := dItr.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case itr.rows <- r:
		}
	}
}

// queueUnpaired sends the rows that [diff.AlignPrimaryKeys] kept apart, as
// removed rows of the from side and added rows of the to side.
//
// It sends every row when |isEveryRowPassed|, and otherwise only rows whose
// key is within ranges. The rows convert from the schema they are stored with,
// not the rekeyed one.
func (itr prollyDiffIter) queueUnpaired(ctx context.Context, isEveryRowPassed bool) error {
	unpaired := itr
	unpaired.fromConverter, unpaired.toConverter = itr.fromUnpairedConverter, itr.toUnpairedConverter
	queue := func(side diff.KeyedRows, d tree.Diff, k, v val.Tuple) error {
		if isEveryRowPassed {
			return unpaired.queueDiff(ctx, d)
		}
		key, err := side.UnpairedKey(ctx, k, v)
		if err != nil {
			return err
		}
		for _, rng := range itr.ranges {
			matchesKey, err := rng.Matches(ctx, key)
			if err != nil {
				return err
			}
			if matchesKey {
				return unpaired.queueDiff(ctx, d)
			}
		}
		return nil
	}
	err := itr.fromUnpaired.IterUnpaired(ctx, func(k, v val.Tuple) error {
		return queue(itr.fromUnpaired, tree.Diff{Type: tree.RemovedDiff, Key: tree.Item(k), From: tree.Item(v)}, k, v)
	})
	if err != nil {
		return err
	}
	return itr.toUnpaired.IterUnpaired(ctx, func(k, v val.Tuple) error {
		return queue(itr.toUnpaired, tree.Diff{Type: tree.AddedDiff, Key: tree.Item(k), To: tree.Item(v)}, k, v)
	})
}

// todo(andy): copy string fields
func (itr prollyDiffIter) makeDiffRowItr(ctx context.Context, d tree.Diff) (*repeatingRowIter, error) {
	if !itr.keyless {
		r, err := itr.getDiffTableRow(ctx, d)
		if err != nil {
			return nil, err
		}
		return &repeatingRowIter{row: r, n: 1}, nil
	}

	r, n, err := itr.getDiffRowAndCardinality(ctx, d)
	if err != nil {
		return nil, err
	}
	return &repeatingRowIter{row: r, n: n}, nil
}

func (itr prollyDiffIter) getDiffRowAndCardinality(ctx context.Context, d tree.Diff) (r sql.Row, n uint64, err error) {
	switch d.Type {
	case tree.AddedDiff:
		n = val.ReadKeylessCardinality(val.Tuple(d.To))
	case tree.RemovedDiff:
		n = val.ReadKeylessCardinality(val.Tuple(d.From))
	case tree.ModifiedDiff:
		fN := val.ReadKeylessCardinality(val.Tuple(d.From))
		tN := val.ReadKeylessCardinality(val.Tuple(d.To))
		if fN < tN {
			n = tN - fN
			d.Type = tree.AddedDiff
		} else {
			n = fN - tN
			d.Type = tree.RemovedDiff
		}
	}

	r, err = itr.getDiffTableRow(ctx, d)
	if err != nil {
		return nil, 0, err
	}

	return r, n, nil
}

// getDiffTableRow returns a row for the diff table given the diff type and the row from the source and target tables. The
// output schema is intended for dolt_diff_* tables and dolt_diff function.
func (itr prollyDiffIter) getDiffTableRow(ctx context.Context, dif tree.Diff) (row sql.Row, err error) {
	tLen := schemaSize(itr.targetToSch)
	fLen := schemaSize(itr.targetFromSch)

	if fLen == 0 && dif.Type == tree.AddedDiff {
		fLen = tLen
	} else if tLen == 0 && dif.Type == tree.RemovedDiff {
		tLen = fLen
	}
	// 2 commit names, 2 commit dates, 1 diff_type
	row = make(sql.Row, fLen+tLen+5)

	// todo (dhruv): implement warnings for row column value coercions.

	if dif.Type != tree.RemovedDiff {
		err = itr.toConverter.PutConverted(ctx, val.Tuple(dif.Key), val.Tuple(dif.To), row[0:tLen])
		if err != nil {
			return nil, err
		}
	}

	idx := tLen
	row[idx] = itr.toCm.name
	row[idx+1] = maybeTime(itr.toCm.ts)

	if dif.Type != tree.AddedDiff {
		err = itr.fromConverter.PutConverted(ctx, val.Tuple(dif.Key), val.Tuple(dif.From), row[tLen+2:tLen+2+fLen])
		if err != nil {
			return nil, err
		}
	}

	idx = fLen + 2 + tLen
	row[idx] = itr.fromCm.name
	row[idx+1] = maybeTime(itr.fromCm.ts)
	row[idx+2] = diffTypeString(dif)

	return row, nil
}

type repeatingRowIter struct {
	row sql.Row
	n   uint64
}

func (r *repeatingRowIter) Next(ctx context.Context) (sql.Row, error) {
	if r.n == 0 {
		return nil, io.EOF
	}
	r.n--
	c := make(sql.Row, len(r.row))
	copy(c, r.row)
	return c, nil
}

func schemaSize(sch schema.Schema) int {
	if sch == nil {
		return 0
	}
	return sch.GetAllCols().Size()
}

func diffTypeString(d tree.Diff) (s string) {
	switch d.Type {
	case tree.AddedDiff:
		s = diffTypeAdded
	case tree.ModifiedDiff:
		s = diffTypeModified
	case tree.RemovedDiff:
		s = diffTypeRemoved
	}
	return
}

func maybeTime(t *time.Time) interface{} {
	if t != nil {
		return *t
	}
	return nil
}

// ------------------------------------
// diffPartitionRowIter
// ------------------------------------

var _ sql.RowIter = (*diffPartitionRowIter)(nil)

type diffPartitionRowIter struct {
	ddb              *doltdb.DoltDB
	currentPartition *DiffPartition
	currentRowIter   *sql.RowIter
}

func NewDiffPartitionRowIter(partition *DiffPartition, ddb *doltdb.DoltDB) *diffPartitionRowIter {
	return &diffPartitionRowIter{
		currentPartition: partition,
		ddb:              ddb,
	}
}

func (itr *diffPartitionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if itr.currentPartition == nil {
			return nil, io.EOF
		}
		if itr.currentRowIter == nil {
			rowIter, err := itr.currentPartition.GetRowIter(ctx)
			if err != nil {
				return nil, err
			}
			itr.currentRowIter = &rowIter
		}

		row, err := (*itr.currentRowIter).Next(ctx)
		if err == io.EOF {
			itr.currentPartition = nil
			itr.currentRowIter = nil
			return nil, err
		} else if err != nil {
			return nil, err
		} else {
			return row, nil
		}
	}
}

func (itr *diffPartitionRowIter) Close(_ *sql.Context) error {
	return nil
}
