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
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type diffRowItr struct {
	ad             diff.RowDiffer
	diffSrc        *diff.RowDiffSource
	joiner         *rowconv.Joiner
	sch            schema.Schema
	fromCommitInfo commitInfo
	toCommitInfo   commitInfo
}

var _ sql.RowIter = &diffRowItr{}

type commitInfo struct {
	name    types.String
	date    *types.Timestamp
	nameTag uint64
	dateTag uint64
}

func newNomsDiffIter(ctx *sql.Context, ddb *doltdb.DoltDB, joiner *rowconv.Joiner, dp DiffPartition, lookup sql.IndexLookup) (*diffRowItr, error) {
	fromData, fromSch, err := tableData(ctx, dp.from, ddb)

	if err != nil {
		return nil, err
	}

	toData, toSch, err := tableData(ctx, dp.to, ddb)

	if err != nil {
		return nil, err
	}

	fromConv, err := dp.rowConvForSchema(ctx, ddb.ValueReadWriter(), dp.fromSch, fromSch)

	if err != nil {
		return nil, err
	}

	toConv, err := dp.rowConvForSchema(ctx, ddb.ValueReadWriter(), dp.toSch, toSch)

	if err != nil {
		return nil, err
	}

	sch := joiner.GetSchema()
	toCol, _ := sch.GetAllCols().GetByName(toCommit)
	fromCol, _ := sch.GetAllCols().GetByName(fromCommit)
	toDateCol, _ := sch.GetAllCols().GetByName(toCommitDate)
	fromDateCol, _ := sch.GetAllCols().GetByName(fromCommitDate)

	fromCmInfo := commitInfo{types.String(dp.fromName), dp.fromDate, fromCol.Tag, fromDateCol.Tag}
	toCmInfo := commitInfo{types.String(dp.toName), dp.toDate, toCol.Tag, toDateCol.Tag}

	rd := diff.NewRowDiffer(ctx, ddb.Format(), fromSch, toSch, 1024)
	// TODO (dhruv) don't cast to noms map
	// Use index lookup if it exists
	if lookup.IsEmpty() {
		rd.Start(ctx, durable.NomsMapFromIndex(fromData), durable.NomsMapFromIndex(toData))
	} else {
		ranges, err := index.NomsRangesFromIndexLookup(ctx, lookup) // TODO: this is a testing method
		if err != nil {
			return nil, err
		}
		// TODO: maybe just use Check
		rangeFunc := func(ctx context.Context, val types.Value) (bool, bool, error) {
			v, ok := val.(types.Tuple)
			if !ok {
				return false, false, nil
			}
			return ranges[0].Check.Check(ctx, v)
		}
		rd.StartWithRange(ctx, durable.NomsMapFromIndex(fromData), durable.NomsMapFromIndex(toData), ranges[0].Start, rangeFunc)
	}

	src := diff.NewRowDiffSource(rd, joiner, ctx.Warn)
	src.AddInputRowConversion(fromConv, toConv)

	return &diffRowItr{
		ad:             rd,
		diffSrc:        src,
		joiner:         joiner,
		sch:            joiner.GetSchema(),
		fromCommitInfo: fromCmInfo,
		toCommitInfo:   toCmInfo,
	}, nil
}

// Next returns the next row
func (itr *diffRowItr) Next(ctx *sql.Context) (sql.Row, error) {
	r, err := itr.diffSrc.NextDiff()

	if err != nil {
		return nil, err
	}

	toAndFromRows, err := itr.joiner.Split(r)
	if err != nil {
		return nil, err
	}
	_, hasTo := toAndFromRows[diff.To]
	_, hasFrom := toAndFromRows[diff.From]

	r, err = r.SetColVal(itr.toCommitInfo.nameTag, types.String(itr.toCommitInfo.name), itr.sch)
	if err != nil {
		return nil, err
	}

	r, err = r.SetColVal(itr.fromCommitInfo.nameTag, types.String(itr.fromCommitInfo.name), itr.sch)

	if err != nil {
		return nil, err
	}

	if itr.toCommitInfo.date != nil {
		r, err = r.SetColVal(itr.toCommitInfo.dateTag, *itr.toCommitInfo.date, itr.sch)

		if err != nil {
			return nil, err
		}
	}

	if itr.fromCommitInfo.date != nil {
		r, err = r.SetColVal(itr.fromCommitInfo.dateTag, *itr.fromCommitInfo.date, itr.sch)

		if err != nil {
			return nil, err
		}
	}

	sqlRow, err := sqlutil.DoltRowToSqlRow(r, itr.sch)

	if err != nil {
		return nil, err
	}

	if hasTo && hasFrom {
		sqlRow = append(sqlRow, diffTypeModified)
	} else if hasTo && !hasFrom {
		sqlRow = append(sqlRow, diffTypeAdded)
	} else {
		sqlRow = append(sqlRow, diffTypeRemoved)
	}

	return sqlRow, nil
}

// Close closes the iterator
func (itr *diffRowItr) Close(*sql.Context) (err error) {
	defer itr.ad.Close()
	defer func() {
		closeErr := itr.diffSrc.Close()

		if err == nil {
			err = closeErr
		}
	}()

	return nil
}

type commitInfo2 struct {
	name string
	ts   *time.Time
}

type prollyDiffIter struct {
	from, to                   prolly.Map
	fromSch, toSch             schema.Schema
	targetFromSch, targetToSch schema.Schema
	fromConverter, toConverter ProllyRowConverter
	fromVD, toVD               val.TupleDesc
	keyless                    bool

	fromCm commitInfo2
	toCm   commitInfo2

	rows    chan sql.Row
	errChan chan error
	cancel  context.CancelFunc
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
func newProllyDiffIter(ctx *sql.Context, dp DiffPartition, ddb *doltdb.DoltDB, targetFromSchema, targetToSchema schema.Schema) (prollyDiffIter, error) {
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
		from = durable.ProllyMapFromIndex(idx)
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
		to = durable.ProllyMapFromIndex(idx)
		if tsch, err = dp.to.GetSchema(ctx); err != nil {
			return prollyDiffIter{}, err
		}
	}

	var nodeStore tree.NodeStore
	if dp.to != nil {
		nodeStore = dp.to.NodeStore()
	} else {
		nodeStore = dp.from.NodeStore()
	}

	fromConverter, err := NewProllyRowConverter(fsch, targetFromSchema, ctx.Warn, nodeStore)
	if err != nil {
		return prollyDiffIter{}, err
	}

	toConverter, err := NewProllyRowConverter(tsch, targetToSchema, ctx.Warn, nodeStore)
	if err != nil {
		return prollyDiffIter{}, err
	}

	fromVD := fsch.GetValueDescriptor()
	toVD := tsch.GetValueDescriptor()
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
		fromVD:        fromVD,
		toVD:          toVD,
		keyless:       keyless,
		fromCm:        fromCm,
		toCm:          toCm,
		rows:          make(chan sql.Row, 64),
		errChan:       make(chan error),
		cancel:        cancel,
	}

	go func() {
		iter.queueRows(child)
	}()

	return iter, nil
}

func (itr prollyDiffIter) Next(ctx *sql.Context) (sql.Row, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-itr.errChan:
		return nil, err
	case r, ok := <-itr.rows:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	}
}

func (itr prollyDiffIter) Close(ctx *sql.Context) error {
	itr.cancel()
	return nil
}

func (itr prollyDiffIter) queueRows(ctx context.Context) {
	err := prolly.DiffMaps(ctx, itr.from, itr.to, func(ctx context.Context, d tree.Diff) error {
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
				continue
			}
		}
	})
	if err != nil && err != io.EOF {
		select {
		case <-ctx.Done():
		case itr.errChan <- err:
		}
		return
	}
	// we need to drain itr.rows before returning io.EOF
	close(itr.rows)
}

// todo(andy): copy string fields
func (itr prollyDiffIter) makeDiffRowItr(ctx context.Context, d tree.Diff) (*repeatingRowIter, error) {
	if !itr.keyless {
		r, err := itr.getDiffRow(ctx, d)
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

	r, err = itr.getDiffRow(ctx, d)
	if err != nil {
		return nil, 0, err
	}

	return r, n, nil
}

func (itr prollyDiffIter) getDiffRow(ctx context.Context, d tree.Diff) (r sql.Row, err error) {
	n := schemaSize(itr.targetToSch)
	m := schemaSize(itr.targetFromSch)
	// 2 commit names, 2 commit dates, 1 diff_type
	r = make(sql.Row, n+m+5)

	// todo (dhruv): implement warnings for row column value coercions.

	if d.Type != tree.RemovedDiff {
		err = itr.toConverter.PutConverted(ctx, val.Tuple(d.Key), val.Tuple(d.To), r[0:n])
		if err != nil {
			return nil, err
		}
	}

	o := n
	r[o] = itr.toCm.name
	r[o+1] = maybeTime(itr.toCm.ts)

	if d.Type != tree.AddedDiff {
		err = itr.fromConverter.PutConverted(ctx, val.Tuple(d.Key), val.Tuple(d.From), r[n+2:n+2+m])
		if err != nil {
			return nil, err
		}
	}

	o = n + 2 + m
	r[o] = itr.fromCm.name
	r[o+1] = maybeTime(itr.fromCm.ts)
	r[o+2] = diffTypeString(d)

	return r, nil
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
