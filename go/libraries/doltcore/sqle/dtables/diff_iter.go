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
	"errors"
	"io"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
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

func newNomsDiffIter(ctx *sql.Context, ddb *doltdb.DoltDB, joiner *rowconv.Joiner, dp DiffPartition) (*diffRowItr, error) {
	fromData, fromSch, err := tableData(ctx, dp.from, ddb)

	if err != nil {
		return nil, err
	}

	toData, toSch, err := tableData(ctx, dp.to, ddb)

	if err != nil {
		return nil, err
	}

	fromConv, err := dp.rowConvForSchema(ctx, ddb.ValueReadWriter(), *dp.fromSch, fromSch)

	if err != nil {
		return nil, err
	}

	toConv, err := dp.rowConvForSchema(ctx, ddb.ValueReadWriter(), *dp.toSch, toSch)

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

	rd := diff.NewRowDiffer(ctx, fromSch, toSch, 1024)
	// TODO (dhruv) don't cast to noms map
	rd.Start(ctx, durable.NomsMapFromIndex(fromData), durable.NomsMapFromIndex(toData))

	warnFn := func(code int, message string, args ...string) {
		ctx.Warn(code, message, args)
	}

	src := diff.NewRowDiffSource(rd, joiner, warnFn)
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
func (itr *diffRowItr) Next(*sql.Context) (sql.Row, error) {
	r, _, err := itr.diffSrc.NextDiff()

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
	if schema.IsKeyless(targetToSchema) {
		return prollyDiffIter{}, errors.New("diffs with keyless schema have not been implemented yet")
	}

	fromCm := commitInfo2{
		name: dp.fromName,
		ts:   (*time.Time)(dp.fromDate),
	}
	toCm := commitInfo2{
		name: dp.toName,
		ts:   (*time.Time)(dp.toDate),
	}

	// dp.from may be nil
	f, fSch, err := tableData(ctx, dp.from, ddb)
	if err != nil {
		return prollyDiffIter{}, nil
	}
	from := durable.ProllyMapFromIndex(f)

	t, tSch, err := tableData(ctx, dp.to, ddb)
	if err != nil {
		return prollyDiffIter{}, nil
	}
	to := durable.ProllyMapFromIndex(t)

	fromConverter, err := NewProllyRowConverter(fSch, targetFromSchema)
	if err != nil {
		return prollyDiffIter{}, err
	}

	toConverter, err := NewProllyRowConverter(tSch, targetToSchema)
	if err != nil {
		return prollyDiffIter{}, err
	}

	child, cancel := context.WithCancel(ctx)

	iter := prollyDiffIter{
		from:          from,
		to:            to,
		fromSch:       fSch,
		toSch:         tSch,
		targetFromSch: targetFromSchema,
		targetToSch:   targetToSchema,
		fromConverter: fromConverter,
		toConverter:   toConverter,
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
		r, err := itr.makeDiffRow(d)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case itr.rows <- r:
			return nil
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
func (itr prollyDiffIter) makeDiffRow(d tree.Diff) (r sql.Row, err error) {

	n := itr.targetFromSch.GetAllCols().Size()
	m := itr.targetToSch.GetAllCols().Size()
	// 2 commit names, 2 commit dates, 1 diff_type
	r = make(sql.Row, n+m+5)

	// todo (dhruv): implement warnings for row column value coercions.

	if d.Type != tree.RemovedDiff {
		err = itr.toConverter.PutConverted(val.Tuple(d.Key), val.Tuple(d.To), r[0:n])
		if err != nil {
			return nil, err
		}
	}

	o := n
	r[o] = itr.toCm.name
	r[o+1] = itr.toCm.ts

	if d.Type != tree.AddedDiff {
		err = itr.fromConverter.PutConverted(val.Tuple(d.Key), val.Tuple(d.From), r[n+2:n+2+m])
		if err != nil {
			return nil, err
		}
	}

	o = n + 2 + m
	r[o] = itr.fromCm.name
	r[o+1] = itr.fromCm.ts
	r[o+2] = diffTypeString(d)

	return r, nil
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
