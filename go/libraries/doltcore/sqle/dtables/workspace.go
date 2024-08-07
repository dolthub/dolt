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

package dtables

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"
)

type WorkspaceTable struct {
	base          doltdb.RootValue
	ws            *doltdb.WorkingSet
	tableName     string
	nomsSchema    schema.Schema
	sqlSchema     sql.Schema
	stagedDeltas  *diff.TableDelta
	workingDeltas *diff.TableDelta

	headSchema schema.Schema

	ddb *doltdb.DoltDB
}

var _ sql.Table = (*WorkspaceTable)(nil)

// NM4 - drop ctx? error
func NewWorkspaceTable(ctx *sql.Context, tblName string, root doltdb.RootValue, ws *doltdb.WorkingSet) (sql.Table, error) {
	stageDlt, err := diff.GetTableDeltas(ctx, root, ws.StagedRoot())
	if err != nil {
		return nil, err
	}
	var stgDel *diff.TableDelta
	for _, delta := range stageDlt {
		if delta.ToName.Name == tblName {
			stgDel = &delta
			break
		}
	}

	workingDlt, err := diff.GetTableDeltas(ctx, root, ws.WorkingRoot())
	if err != nil {
		return nil, err
	}

	var wkDel *diff.TableDelta
	for _, delta := range workingDlt {
		if delta.ToName.Name == tblName {
			wkDel = &delta
			break
		}
	}

	if wkDel == nil && stgDel == nil {
		emptyTable := emptyWorkspaceTable{tableName: tblName}
		return &emptyTable, nil
	}

	var toSch, fromSch schema.Schema
	if stgDel == nil {
		toSch, err = wkDel.ToTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		fromSch, err = wkDel.FromTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		toSch, err = stgDel.ToTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		fromSch, err = stgDel.FromTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	}

	totalSch, err := workspaceSchema(fromSch, toSch)
	if err != nil {
		return nil, err
	}
	finalSch, err := sqlutil.FromDoltSchema("", "", totalSch)
	if err != nil {
		return nil, err
	}

	return &WorkspaceTable{
		base:          root,
		ws:            ws,
		tableName:     tblName,
		nomsSchema:    totalSch,
		sqlSchema:     finalSch.Schema,
		stagedDeltas:  stgDel,
		workingDeltas: wkDel,
		headSchema:    fromSch, // NM4 - convince myself this is correct.
		ddb:           nil,     // NM4 - not sure what this is for. Drop? I think we'll need it eventually....
	}, nil
}

func (wt *WorkspaceTable) Name() string {
	return doltdb.DoltWorkspaceTablePrefix + wt.tableName
}

func (wt *WorkspaceTable) String() string {
	return wt.Name()
}

func (wt *WorkspaceTable) Schema() sql.Schema {
	return wt.sqlSchema
}

// CalculateDiffSchema returns the schema for the dolt_diff table based on the schemas from the from and to tables.
// Either may be nil, in which case the nil argument will use the schema of the non-nil argument
func workspaceSchema(fromSch, toSch schema.Schema) (schema.Schema, error) {
	if fromSch == nil && toSch == nil {
		return nil, errors.New("Runtime error:non-nil argument required to CalculateDiffSchema")
	} else if fromSch == nil {
		fromSch = toSch
	} else if toSch == nil {
		toSch = fromSch
	}

	cols := make([]schema.Column, 0, 2+toSch.GetAllCols().Size()+fromSch.GetAllCols().Size())

	cols = append(cols,
		schema.NewColumn("id", 0, types.UintKind, true),
		schema.NewColumn("staged", 1, types.BoolKind, false),
		schema.NewColumn("diff_type", 2, types.StringKind, false),
	)

	transformer := func(sch schema.Schema, namer func(string) string) error {
		return sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			c, err := schema.NewColumnWithTypeInfo(
				namer(col.Name),
				uint64(len(cols)),
				col.TypeInfo,
				false,
				col.Default,
				false,
				col.Comment)
			if err != nil {
				return true, err
			}
			cols = append(cols, c)
			return false, nil
		})
	}

	err := transformer(toSch, diff.ToColNamer)
	if err != nil {
		return nil, err
	}
	err = transformer(fromSch, diff.FromColNamer)
	if err != nil {
		return nil, err
	}

	return schema.UnkeyedSchemaFromCols(schema.NewColCollection(cols...)), nil
}

func (wt *WorkspaceTable) Collation() sql.CollationID { return sql.Collation_Default }

type WorkspacePartitionItr struct {
	partition *WorkspacePartition
}

func (w *WorkspacePartitionItr) Close(_ *sql.Context) error {
	return nil
}

func (w *WorkspacePartitionItr) Next(_ *sql.Context) (sql.Partition, error) {
	if w.partition == nil {
		return nil, io.EOF
	}
	ans := w.partition
	w.partition = nil
	return ans, nil
}

type WorkspacePartition struct {
	base       *doltdb.Table
	baseSch    schema.Schema
	working    *doltdb.Table
	workingSch schema.Schema
	staging    *doltdb.Table
	stagingSch schema.Schema
}

var _ sql.Partition = (*WorkspacePartition)(nil)

func (w *WorkspacePartition) Key() []byte {
	// NM4 - is there ever used? We return the table names in the DiffPartition. What is the purpose of this?
	return []byte("hello world")
}

func (wt *WorkspaceTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	_, baseTable, baseTableExists, err := resolve.Table(ctx, wt.base, wt.tableName)
	if err != nil {
		return nil, err
	}
	if !baseTableExists {
		return nil, sql.ErrTableNotFound.New(wt.tableName) // NM4 - not an error. Just no changes. test/fix.
	}
	var baseSchema schema.Schema = schema.EmptySchema
	if baseSchema, err = baseTable.GetSchema(ctx); err != nil {
		return nil, err
	}

	_, stagingTable, tableExists, err := resolve.Table(ctx, wt.ws.StagedRoot(), wt.tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return nil, sql.ErrTableNotFound.New(tableName) // NM4 - not an error. Just no changes. test/fix.
	}
	var stagingSchema schema.Schema = schema.EmptySchema
	if stagingSchema, err = stagingTable.GetSchema(ctx); err != nil {
		return nil, err
	}

	_, workingTable, tableExists, err := resolve.Table(ctx, wt.ws.WorkingRoot(), wt.tableName)
	if err != nil {
		return nil, err
	}
	if !tableExists {
		return nil, sql.ErrTableNotFound.New(tableName) // NM4 - not an error. Just no changes. test/fix.
	}
	var workingSchema schema.Schema = schema.EmptySchema
	if workingSchema, err = workingTable.GetSchema(ctx); err != nil {
		return nil, err
	}

	part := WorkspacePartition{
		base:       baseTable,
		baseSch:    baseSchema,
		staging:    stagingTable,
		stagingSch: stagingSchema,
		working:    workingTable,
		workingSch: workingSchema,
	}

	return &WorkspacePartitionItr{&part}, nil
}

func (wt *WorkspaceTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	wp, ok := part.(*WorkspacePartition)
	if !ok {
		return nil, fmt.Errorf("Runtime Exception: expected a WorkspacePartition, got %T", part)
	}

	return newWorkspaceDiffIter(ctx, *wp) // NM4 - base schema. should we use base and staged?
}

type workspaceDiffIter struct {
	base    prolly.Map
	working prolly.Map
	staging prolly.Map

	baseConverter    ProllyRowConverter
	workingConverter ProllyRowConverter
	stagingConverter ProllyRowConverter

	tgtBaseSch    schema.Schema
	tgtWorkingSch schema.Schema
	tgtStagingSch schema.Schema

	keyless bool

	rows    chan sql.Row
	errChan chan error
	cancel  func()
}

func (itr workspaceDiffIter) Next(ctx *sql.Context) (sql.Row, error) {
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

func (itr workspaceDiffIter) Close(c *sql.Context) error {
	itr.cancel()
	return nil
}

func getWorkspaceRowAndCardinality(
	ctx context.Context,
	idx int,
	staging bool,
	toSch schema.Schema,
	fromSch schema.Schema,
	toConverter ProllyRowConverter,
	fromConverter ProllyRowConverter,
	d tree.Diff,
) (r sql.Row, n uint64, err error) {
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

	r, err = getWorkspaceTableRow(ctx, idx, staging, toSch, fromSch, toConverter, fromConverter, d)
	if err != nil {
		return nil, 0, err
	}

	return r, n, nil
}

// getWorkspaceTableRow returns a row for the diff table given the diff type and the row from the source and target tables. The
// output schema is intended for dolt_workspace_* tables.
func getWorkspaceTableRow(
	ctx context.Context,
	rowId int,
	staged bool,
	toSch schema.Schema,
	fromSch schema.Schema,
	toConverter ProllyRowConverter,
	fromConverter ProllyRowConverter,
	dif tree.Diff,
) (row sql.Row, err error) {
	tLen := schemaSize(toSch)
	fLen := schemaSize(fromSch)

	if fLen == 0 && dif.Type == tree.AddedDiff {
		fLen = tLen
	} else if tLen == 0 && dif.Type == tree.RemovedDiff {
		tLen = fLen
	}

	row = make(sql.Row, 3+tLen+fLen)

	row[0] = rowId
	row[1] = staged
	row[2] = diffTypeString(dif)

	idx := 3

	if dif.Type != tree.RemovedDiff {
		err = toConverter.PutConverted(ctx, val.Tuple(dif.Key), val.Tuple(dif.To), row[idx:idx+tLen])
		if err != nil {
			return nil, err
		}
	}
	idx += tLen

	if dif.Type != tree.AddedDiff {
		err = fromConverter.PutConverted(ctx, val.Tuple(dif.Key), val.Tuple(dif.From), row[idx:idx+fLen])
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}

// NM4 - Virtually identical to prollyDiffIter.queueRows, but for workspaces. Dedupe this.
func (itr *workspaceDiffIter) queueWorkspaceRows(ctx context.Context) {

	idx := 0

	err := prolly.DiffMaps(ctx, itr.base, itr.staging, false, func(ctx context.Context, d tree.Diff) error {
		dItr, err := itr.makeWorkspaceRowItr(ctx, idx, true, itr.tgtStagingSch, itr.tgtBaseSch, itr.stagingConverter, itr.baseConverter, d)
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
				idx++
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

	err = prolly.DiffMaps(ctx, itr.staging, itr.working, false, func(ctx context.Context, d tree.Diff) error {
		dItr, err := itr.makeWorkspaceRowItr(ctx, idx, false, itr.tgtWorkingSch, itr.tgtStagingSch, itr.workingConverter, itr.stagingConverter, d)
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
				idx++
				continue
			}
		}
	})

	// we need to drain itr.rows before returning io.EOF
	close(itr.rows)
}

func (itr *workspaceDiffIter) makeWorkspaceRowItr(
	ctx context.Context,
	idx int,
	staging bool,
	toSch schema.Schema,
	fromSch schema.Schema,
	toConverter ProllyRowConverter,
	fromConverter ProllyRowConverter,
	d tree.Diff,
) (*repeatingRowIter, error) {
	if !itr.keyless {
		r, err := getWorkspaceTableRow(ctx, idx, staging, toSch, fromSch, toConverter, fromConverter, d)
		if err != nil {
			return nil, err
		}
		return &repeatingRowIter{row: r, n: 1}, nil
	}

	r, n, err := getWorkspaceRowAndCardinality(ctx, idx, staging, toSch, fromSch, toConverter, fromConverter, d)
	if err != nil {
		return nil, err
	}
	return &repeatingRowIter{row: r, n: n}, nil
}

// NM4 - virtually identical to newProllyDiffIter, but for workspaces. Dedupe this.
func newWorkspaceDiffIter(ctx *sql.Context, wp WorkspacePartition) (workspaceDiffIter, error) {
	var base, working, staging prolly.Map

	if wp.base != nil {
		idx, err := wp.base.GetRowData(ctx)
		if err != nil {
			return workspaceDiffIter{}, err
		}
		base = durable.ProllyMapFromIndex(idx)
	}

	if wp.staging != nil {
		idx, err := wp.staging.GetRowData(ctx)
		if err != nil {
			return workspaceDiffIter{}, err
		}
		staging = durable.ProllyMapFromIndex(idx)
	}

	if wp.working != nil {
		idx, err := wp.working.GetRowData(ctx)
		if err != nil {
			return workspaceDiffIter{}, err
		}
		working = durable.ProllyMapFromIndex(idx)
	}

	nodeStore := wp.base.NodeStore()

	baseConverter, err := NewProllyRowConverter(wp.baseSch, wp.baseSch, ctx.Warn, nodeStore)
	if err != nil {
		return workspaceDiffIter{}, err
	}

	stagingConverter, err := NewProllyRowConverter(wp.stagingSch, wp.stagingSch, ctx.Warn, nodeStore)
	if err != nil {
		return workspaceDiffIter{}, err
	}

	workingConverter, err := NewProllyRowConverter(wp.workingSch, wp.workingSch, ctx.Warn, nodeStore)
	if err != nil {
		return workspaceDiffIter{}, err
	}

	keyless := schema.IsKeyless(wp.baseSch) && schema.IsKeyless(wp.stagingSch) && schema.IsKeyless(wp.workingSch)
	child, cancel := context.WithCancel(ctx)
	iter := workspaceDiffIter{
		base:    base,
		working: working,
		staging: staging,

		tgtBaseSch:    wp.baseSch,
		tgtWorkingSch: wp.workingSch,
		tgtStagingSch: wp.stagingSch,

		baseConverter:    baseConverter,
		workingConverter: workingConverter,
		stagingConverter: stagingConverter,

		keyless: keyless,
		rows:    make(chan sql.Row, 64),
		errChan: make(chan error),
		cancel:  cancel,
	}

	go func() {
		iter.queueWorkspaceRows(child)
	}()

	return iter, nil
}

type emptyWorkspaceTable struct {
	tableName string
}

var _ sql.Table = (*emptyWorkspaceTable)(nil)

func (e emptyWorkspaceTable) Name() string {
	return "dolt_workspace_" + e.tableName
}

func (e emptyWorkspaceTable) String() string {
	return e.Name()
}

func (e emptyWorkspaceTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "id", Type: sqltypes.Int32, Nullable: false},
		{Name: "staged", Type: sqltypes.Boolean, Nullable: false},
	}
}

func (e emptyWorkspaceTable) Collation() sql.CollationID { return sql.Collation_Default }

func (e emptyWorkspaceTable) Partitions(c *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (e emptyWorkspaceTable) PartitionRows(c *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}
