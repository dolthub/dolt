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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"
)

type WorkspaceTable struct {
	ws   *doltdb.WorkingSet // NM4 - do we need both???
	head doltdb.RootValue
	//	roots         doltdb.Roots
	tableName     string
	nomsSchema    schema.Schema // NM4 - can we get rid of this?
	sqlSchema     sql.Schema
	stagedDeltas  *diff.TableDelta
	workingDeltas *diff.TableDelta

	headSchema schema.Schema

	//////
	headTable *doltdb.Table
	stgTable  *doltdb.Table
	wrkTable  *doltdb.Table
}

type WorkspaceTableUpdater struct {
	tableName string
	ws        *doltdb.WorkingSet
	head      doltdb.RootValue

	headSch schema.Schema // We probably need three. NM4.

	headTable *doltdb.Table
	stgTable  *doltdb.Table
	wrkTable  *doltdb.Table

	tableWriter   *dsess.TableWriter
	sessionWriter *dsess.WriteSession
}

func (wtu *WorkspaceTableUpdater) StatementBegin(ctx *sql.Context) {
	sessionWriter, tableWriter, err := wtu.getWorkspaceTableWriter(ctx)
	if err != nil {
		panic(err) // NM4.
	}
	wtu.tableWriter = &tableWriter
	wtu.sessionWriter = &sessionWriter

}

func (wtu *WorkspaceTableUpdater) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return errorEncountered
}

func (wtu *WorkspaceTableUpdater) StatementComplete(ctx *sql.Context) error {
	if wtu.tableWriter != nil {
		err := (*wtu.tableWriter).Close(ctx)
		if err != nil {
			return err
		}
		wtu.tableWriter = nil
	}

	if wtu.sessionWriter != nil {
		newWorkingSet, err := (*wtu.sessionWriter).Flush(ctx)
		if err != nil {
			return err
		}
		wtu.sessionWriter = nil

		ds := dsess.DSessFromSess(ctx.Session)
		err = ds.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), newWorkingSet)
		if err != nil {
			return err
		}
	}

	return nil
}

func (wtu *WorkspaceTableUpdater) getWorkspaceTableWriter(ctx *sql.Context) (dsess.WriteSession, dsess.TableWriter, error) {
	ds := dsess.DSessFromSess(ctx.Session)
	setter := ds.SetStagingRoot

	writeSession := writer.NewWriteSession(types.Format_DOLT, wtu.ws, nil, editor.Options{TargetStaging: true})

	tableWriter, err := writeSession.GetTableWriter(ctx, doltdb.TableName{Name: wtu.tableName}, ctx.GetCurrentDatabase(), setter, true)
	if err != nil {
		return nil, nil, err
	}

	return writeSession, tableWriter, nil
}

func (wtu *WorkspaceTableUpdater) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	if old == nil || new == nil {
		panic("row is nil")
	}

	valid, isStaged := validateWorkspaceUpdate(old, new)
	if !valid {
		return errors.New("only update of column 'staged' is allowed")
	}

	// We could do this up front once. NM4. Also - not always the same schema??
	schemaLen := wtu.headSch.GetAllCols().Size()

	// old and new are the same. Just use one.
	new = nil

	toRow := old[3 : 3+schemaLen]
	fromRow := old[3+schemaLen : len(old)]
	if !isStaged {
		toRow, fromRow = fromRow, toRow
	}

	// loop over toRow, and if it's all nil, it's a delete. NM4 - is there a better way to pass through the diff type?
	isDelete := true
	for _, val := range toRow {
		if val != nil {
			isDelete = false
			break
		}
	}

	// NM4 - check nil???
	tableWriter := (*wtu.tableWriter)
	if isDelete {
		return tableWriter.Delete(ctx, fromRow)
	} else {
		return tableWriter.Update(ctx, fromRow, toRow)
	}
}

// NM4 - is there a better way?
func isTrue(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	case int8:
		return v != 0
	default:
		return false
	}
}

// validateWorkspaceUpdate returns true IFF old and new row are identical - except the "staged" column. Updating that
// column to TRUE or FALSE is the only update allowed, and any other update will result in 'valid' being false. If
// valid is true, then 'staged' will be the value in the "staged" column of the new row.
func validateWorkspaceUpdate(old, new sql.Row) (valid, staged bool) {
	// NM4 - I think it's impossible to have equal rows, but we should rule that out.
	if old == nil {
		return false, false
	}

	if len(old) != len(new) {
		return false, false
	}

	isStaged := false

	for i := range new {
		if i == 1 {
			// NM4 - not required in the iterator, right?
			isStaged = isTrue(new[i])
			// skip the "staged" column. NM4 - is there a way to not use a constant index here?
			continue
		}

		if old[i] != new[i] {
			return false, false
		}
	}
	return true, isStaged
}

func (wtu *WorkspaceTableUpdater) Close(c *sql.Context) error {
	return nil
}

var _ sql.RowUpdater = (*WorkspaceTableUpdater)(nil)

func (wt *WorkspaceTable) Deleter(c *sql.Context) sql.RowDeleter {
	//TODO implement me
	panic("I'm a deleter!!!")
}

func (wt *WorkspaceTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return &WorkspaceTableUpdater{
		tableName: wt.tableName,
		headSch:   wt.headSchema,
		/*
			headTable: wt.headTable,
			stgTable:  wt.stgTable,
			wrkTable:  wt.wrkTable,
		*/
		ws:   wt.ws,
		head: wt.head,
	}
}

var _ sql.Table = (*WorkspaceTable)(nil)
var _ sql.UpdatableTable = (*WorkspaceTable)(nil)
var _ sql.DeletableTable = (*WorkspaceTable)(nil)

func NewWorkspaceTable(ctx *sql.Context, tblName string, head doltdb.RootValue, ws *doltdb.WorkingSet) (sql.Table, error) {
	stageDlt, err := diff.GetTableDeltas(ctx, head, ws.StagedRoot())
	if err != nil {
		return nil, err
	}
	var stgDel *diff.TableDelta
	for _, delta := range stageDlt {
		if delta.FromName.Name == tblName || delta.ToName.Name == tblName {
			stgDel = &delta
			break
		}
	}

	workingDlt, err := diff.GetTableDeltas(ctx, head, ws.WorkingRoot())
	if err != nil {
		return nil, err
	}

	var wkDel *diff.TableDelta
	for _, delta := range workingDlt {
		if delta.FromName.Name == tblName || delta.ToName.Name == tblName {
			wkDel = &delta
			break
		}
	}

	if wkDel == nil && stgDel == nil {
		emptyTable := emptyWorkspaceTable{tableName: tblName}
		return &emptyTable, nil
	}

	var fromSch schema.Schema
	if stgDel != nil && stgDel.FromTable != nil {
		fromSch, err = stgDel.FromTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	} else if wkDel != nil && wkDel.FromTable != nil {
		fromSch, err = wkDel.FromTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	}

	toSch := fromSch
	if wkDel != nil && wkDel.ToTable != nil {
		toSch, err = wkDel.ToTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	} else if stgDel != nil && stgDel.ToTable != nil {
		toSch, err = stgDel.ToTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	}
	if fromSch == nil && toSch == nil {
		return nil, errors.New("Runtime error: from and to schemas are both nil")
	}
	if fromSch == nil {
		fromSch = toSch
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
		ws:            ws,
		head:          head,
		tableName:     tblName,
		nomsSchema:    totalSch,
		sqlSchema:     finalSch.Schema,
		stagedDeltas:  stgDel,
		workingDeltas: wkDel,
		headSchema:    fromSch,
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

	cols := make([]schema.Column, 0, 3+toSch.GetAllCols().Size()+fromSch.GetAllCols().Size())

	cols = append(cols,
		schema.NewColumn("id", 0, types.UintKind, true),
		schema.NewColumn("staged", 0, types.BoolKind, false),
		schema.NewColumn("diff_type", 0, types.StringKind, false),
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
	name       string
	base       *doltdb.Table
	baseSch    schema.Schema
	working    *doltdb.Table
	workingSch schema.Schema
	staging    *doltdb.Table
	stagingSch schema.Schema
}

var _ sql.Partition = (*WorkspacePartition)(nil)

func (w *WorkspacePartition) Key() []byte {
	return []byte(w.name)
}

func (wt *WorkspaceTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	_, baseTable, baseTableExists, err := resolve.Table(ctx, wt.head, wt.tableName)
	if err != nil {
		return nil, err
	}
	var baseSchema schema.Schema = schema.EmptySchema
	if baseTableExists {
		if baseSchema, err = baseTable.GetSchema(ctx); err != nil {
			return nil, err
		}
	}

	_, stagingTable, stagingTableExists, err := resolve.Table(ctx, wt.ws.StagedRoot(), wt.tableName)
	if err != nil {
		return nil, err
	}
	var stagingSchema schema.Schema = schema.EmptySchema
	if stagingTableExists {
		if stagingSchema, err = stagingTable.GetSchema(ctx); err != nil {
			return nil, err
		}
	}

	_, workingTable, workingTableExists, err := resolve.Table(ctx, wt.ws.WorkingRoot(), wt.tableName)
	if err != nil {
		return nil, err
	}
	var workingSchema schema.Schema = schema.EmptySchema
	if workingTableExists {
		if workingSchema, err = workingTable.GetSchema(ctx); err != nil {
			return nil, err
		}
	}

	part := WorkspacePartition{
		name:       wt.Name(),
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

	return newWorkspaceDiffIter(ctx, *wp)
}

// workspaceDiffIter enables the iteration over the diff information between the HEAD, STAGING, and WORKING roots.
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

// queueWorkspaceRows is similar to prollyDiffIter.queueRows, but for workspaces. It performs two seperate calls
// to prolly.DiffMaps, one for staging and one for working. The end result is queueing the rows from both maps
// into the "rows" channel of the workspaceDiffIter.
func (itr *workspaceDiffIter) queueWorkspaceRows(ctx context.Context) {
	k1 := schema.EmptySchema == itr.tgtStagingSch || schema.IsKeyless(itr.tgtStagingSch)
	k2 := schema.EmptySchema == itr.tgtBaseSch || schema.IsKeyless(itr.tgtBaseSch)
	k3 := schema.EmptySchema == itr.tgtWorkingSch || schema.IsKeyless(itr.tgtWorkingSch)

	keyless := k1 && k2 && k3

	idx := 0

	err := prolly.DiffMaps(ctx, itr.base, itr.staging, false, func(ctx context.Context, d tree.Diff) error {
		rows, err := itr.makeWorkspaceRows(ctx, idx, true, itr.tgtStagingSch, itr.tgtBaseSch, keyless, itr.stagingConverter, itr.baseConverter, d)
		if err != nil {
			return err
		}
		for _, r := range rows {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case itr.rows <- r:
				idx++
				continue
			}
		}
		return nil
	})

	if err != nil && err != io.EOF {
		select {
		case <-ctx.Done():
		case itr.errChan <- err:
		}
		return
	}

	err = prolly.DiffMaps(ctx, itr.staging, itr.working, false, func(ctx context.Context, d tree.Diff) error {
		rows, err := itr.makeWorkspaceRows(ctx, idx, false, itr.tgtWorkingSch, itr.tgtStagingSch, keyless, itr.workingConverter, itr.stagingConverter, d)
		if err != nil {
			return err
		}
		for _, r := range rows {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case itr.rows <- r:
				idx++
				continue
			}
		}
		return nil
	})

	// we need to drain itr.rows before returning io.EOF
	close(itr.rows)
}

// makeWorkspaceRows takes the diff information from the prolly.DiffMaps and converts it into a slice of rows. In the case
// of tables with a primary key, this method will return a single row. For tables without a primary key, it will return
// 1 or more rows. The rows returned are in the full schema that the workspace table returns, so the workspace table columns
// (id, staged, diff_type) are included in the returned rows with the populated values.
func (itr *workspaceDiffIter) makeWorkspaceRows(
	ctx context.Context,
	idx int,
	staging bool,
	toSch schema.Schema,
	fromSch schema.Schema,
	keyless bool,
	toConverter ProllyRowConverter,
	fromConverter ProllyRowConverter,
	d tree.Diff,
) ([]sql.Row, error) {
	n := uint64(1)
	if keyless {
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
	}

	ans := make([]sql.Row, n)
	for i := uint64(0); i < n; i++ {
		r, err := getWorkspaceTableRow(ctx, idx, staging, toSch, fromSch, toConverter, fromConverter, d)
		if err != nil {
			return nil, err
		}
		ans[i] = r
		idx++
	}
	return ans, nil
}

// newWorkspaceDiffIter takes a WorkspacePartition and returns a workspaceDiffIter. The workspaceDiffIter is used to iterate
// over the diff information from the prolly.DiffMaps.
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

	var nodeStore tree.NodeStore
	if wp.base != nil {
		nodeStore = wp.base.NodeStore()
	} else if wp.staging != nil {
		nodeStore = wp.staging.NodeStore()
	} else if wp.working != nil {
		nodeStore = wp.working.NodeStore()
	} else {
		return workspaceDiffIter{}, errors.New("no base, staging, or working table")
	}

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
	return doltdb.DoltWorkspaceTablePrefix + e.tableName
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
