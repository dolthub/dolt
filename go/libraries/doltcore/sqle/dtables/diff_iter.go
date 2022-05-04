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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

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
	from, to prolly.Map
	kd, vd   val.TupleDesc
	fromCm   commitInfo2
	toCm     commitInfo2

	rows   chan sql.Row
	cancel context.CancelFunc
	eg     *errgroup.Group
}

func newProllyDiffIter(ctx *sql.Context, dp DiffPartition) (prollyDiffIter, error) {
	fromCm := commitInfo2{
		name: dp.fromName,
		ts:   (*time.Time)(dp.fromDate),
	}
	toCm := commitInfo2{
		name: dp.toName,
		ts:   (*time.Time)(dp.toDate),
	}

	f, err := dp.from.GetRowData(ctx)
	if err != nil {
		return prollyDiffIter{}, nil
	}
	from := durable.ProllyMapFromIndex(f)

	t, err := dp.to.GetRowData(ctx)
	if err != nil {
		return prollyDiffIter{}, nil
	}
	to := durable.ProllyMapFromIndex(t)

	kd, vd := from.Descriptors()

	child, cancel := context.WithCancel(ctx)
	eg, egCtx := errgroup.WithContext(child)

	iter := prollyDiffIter{
		from:   from,
		to:     to,
		kd:     kd,
		vd:     vd,
		fromCm: fromCm,
		toCm:   toCm,
		rows:   make(chan sql.Row, 64),
		cancel: cancel,
		eg:     eg,
	}

	eg.Go(func() error {
		return iter.queueRows(egCtx)
	})

	return iter, nil
}

func (itr prollyDiffIter) Next(ctx *sql.Context) (sql.Row, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-itr.rows:
		return r, nil
	}
}

func (itr prollyDiffIter) Close(ctx *sql.Context) error {
	itr.cancel()
	return itr.eg.Wait()
}

func (itr prollyDiffIter) queueRows(ctx context.Context) error {
	return prolly.DiffMaps(ctx, itr.from, itr.to, func(ctx context.Context, d tree.Diff) error {
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
}

// todo(andy): copy string fields
func (itr prollyDiffIter) makeDiffRow(d tree.Diff) (r sql.Row, err error) {
	keySz, valSz := itr.kd.Count(), itr.vd.Count()
	r = make(sql.Row, 5+(keySz+valSz)*2)

	o := 0
	if d.Type != tree.AddedDiff {
		for j := 0; j < keySz; j++ {
			r[j+o], err = index.GetField(itr.kd, j, val.Tuple(d.Key))
			if err != nil {
				return nil, err
			}
		}
		o = keySz
		for j := 0; j < valSz; j++ {
			r[j+o], err = index.GetField(itr.vd, j, val.Tuple(d.From))
			if err != nil {
				return nil, err
			}
		}
	}
	o = keySz + valSz
	r[o] = itr.fromCm.name
	r[o+1] = *itr.fromCm.ts
	o = keySz + valSz + 2

	if d.Type != tree.RemovedDiff {
		for j := 0; j < keySz; j++ {
			r[j+o], err = index.GetField(itr.kd, j, val.Tuple(d.Key))
			if err != nil {
				return nil, err
			}
		}
		o = keySz + valSz + keySz
		for j := 0; j < valSz; j++ {
			r[j+o], err = index.GetField(itr.vd, j, val.Tuple(d.To))
			if err != nil {
				return nil, err
			}
		}
	}
	o = (keySz + valSz) * 2
	r[o] = itr.toCm.name
	r[o+1] = *itr.toCm.ts
	r[o+2] = diffTypeString(d)

	return
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
