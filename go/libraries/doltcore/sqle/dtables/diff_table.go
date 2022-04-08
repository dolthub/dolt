// Copyright 2019 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/expreval"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	toCommit       = "to_commit"
	fromCommit     = "from_commit"
	toCommitDate   = "to_commit_date"
	fromCommitDate = "from_commit_date"

	diffTypeColName  = "diff_type"
	diffTypeAdded    = "added"
	diffTypeModified = "modified"
	diffTypeRemoved  = "removed"
)

var _ sql.Table = (*DiffTable)(nil)
var _ sql.FilteredTable = (*DiffTable)(nil)

type DiffTable struct {
	name        string
	ddb         *doltdb.DoltDB
	workingRoot *doltdb.RootValue
	head        *doltdb.Commit

	targetSch        schema.Schema
	joiner           *rowconv.Joiner
	sqlSch           sql.PrimaryKeySchema
	partitionFilters []sql.Expression
	rowFilters       []sql.Expression
}

var PrimaryKeyChangeWarning = "cannot render full diff between commits %s and %s due to primary key set change"

const PrimaryKeyChangeWarningCode int = 1105 // Since this our own custom warning we'll use 1105, the code for an unknown error

func NewDiffTable(ctx *sql.Context, tblName string, ddb *doltdb.DoltDB, root *doltdb.RootValue, head *doltdb.Commit) (sql.Table, error) {
	diffTblName := doltdb.DoltDiffTablePrefix + tblName

	table, tblName, ok, err := root.GetTableInsensitive(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(diffTblName)
	}
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	colCollection := sch.GetAllCols()
	colCollection = colCollection.Append(
		schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
		schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
	sch = schema.MustSchemaFromCols(colCollection)

	if sch.GetAllCols().Size() <= 1 {
		return nil, sql.ErrTableNotFound.New(diffTblName)
	}

	j, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{{Name: diff.To, Sch: sch}, {Name: diff.From, Sch: sch}},
		map[string]rowconv.ColNamingFunc{
			diff.To:   diff.ToColNamer,
			diff.From: diff.FromColNamer,
		})
	if err != nil {
		return nil, err
	}

	sqlSch, err := sqlutil.FromDoltSchema(diffTblName, j.GetSchema())
	if err != nil {
		return nil, err
	}

	sqlSch.Schema = append(sqlSch.Schema, &sql.Column{
		Name:     diffTypeColName,
		Type:     sql.Text,
		Nullable: false,
		Source:   diffTblName,
	})

	return &DiffTable{
		name:             tblName,
		ddb:              ddb,
		workingRoot:      root,
		head:             head,
		targetSch:        sch,
		joiner:           j,
		sqlSch:           sqlSch,
		partitionFilters: nil,
		rowFilters:       nil,
	}, nil
}

func (dt *DiffTable) Name() string {
	return doltdb.DoltDiffTablePrefix + dt.name
}

func (dt *DiffTable) String() string {
	return doltdb.DoltDiffTablePrefix + dt.name
}

func (dt *DiffTable) Schema() sql.Schema {
	return dt.sqlSch.Schema
}

func (dt *DiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	cmItr := doltdb.CommitItrForRoots(dt.ddb, dt.head)

	sf, err := SelectFuncForFilters(dt.ddb.Format(), dt.partitionFilters)
	if err != nil {
		return nil, err
	}

	t, exactName, ok, err := dt.workingRoot.GetTableInsensitive(ctx, dt.name)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, errors.New(fmt.Sprintf("table: %s does not exist", dt.name))
	}

	wrTblHash, _, err := dt.workingRoot.GetTableHash(ctx, exactName)
	if err != nil {
		return nil, err
	}

	cmHash, _, err := cmItr.Next(ctx)
	if err != nil {
		return nil, err
	}

	cmHashToTblInfo := make(map[hash.Hash]TblInfoAtCommit)
	cmHashToTblInfo[cmHash] = TblInfoAtCommit{"WORKING", nil, t, wrTblHash}

	err = cmItr.Reset(ctx)
	if err != nil {
		return nil, err
	}

	return &DiffPartitions{
		tblName:         exactName,
		cmItr:           cmItr,
		cmHashToTblInfo: cmHashToTblInfo,
		selectFunc:      sf,
		toSch:           dt.targetSch,
		fromSch:         dt.targetSch,
	}, nil
}

var partitionFilterCols = set.NewStrSet([]string{toCommit, fromCommit, toCommitDate, fromCommitDate})

func splitPartitionFilters(filters []sql.Expression) (commitFilters, rowFilters []sql.Expression) {
	return splitFilters(filters, getColumnFilterCheck(partitionFilterCols))
}

// HandledFilters returns the list of filters that will be handled by the table itself
func (dt *DiffTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	dt.partitionFilters, dt.rowFilters = splitPartitionFilters(filters)
	return dt.partitionFilters
}

// Filters returns the list of filters that are applied to this table.
func (dt *DiffTable) Filters() []sql.Expression {
	return dt.partitionFilters
}

// WithFilters returns a new sql.Table instance with the filters applied
func (dt *DiffTable) WithFilters(_ *sql.Context, filters []sql.Expression) sql.Table {
	if dt.partitionFilters == nil {
		dt.partitionFilters, dt.rowFilters = splitPartitionFilters(filters)
	}

	return dt
}

func (dt *DiffTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	dp := part.(DiffPartition)
	return dp.GetRowIter(ctx, dt.ddb, dt.joiner)
}

// tableData returns the map of primary key to values for the specified table (or an empty map if the tbl is null)
// and the schema of the table (or EmptySchema if tbl is null).
func tableData(ctx *sql.Context, tbl *doltdb.Table, ddb *doltdb.DoltDB) (types.Map, schema.Schema, error) {
	var data types.Map
	var err error
	if tbl == nil {
		data, err = types.NewMap(ctx, ddb.ValueReadWriter())
		if err != nil {
			return types.EmptyMap, nil, err
		}
	} else {
		data, err = tbl.GetNomsRowData(ctx)
		if err != nil {
			return types.EmptyMap, nil, err
		}
	}

	var sch schema.Schema
	if tbl == nil {
		sch = schema.EmptySchema
	} else {
		sch, err = tbl.GetSchema(ctx)

		if err != nil {
			return types.EmptyMap, nil, err
		}
	}

	return data, sch, nil
}

var _ sql.RowIter = (*diffRowItr)(nil)

type diffRowItr struct {
	ad             diff.RowDiffer
	diffSrc        *diff.RowDiffSource
	joiner         *rowconv.Joiner
	sch            schema.Schema
	fromCommitInfo commitInfo
	toCommitInfo   commitInfo
}

type commitInfo struct {
	name    types.String
	date    *types.Timestamp
	nameTag uint64
	dateTag uint64
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

type TblInfoAtCommit struct {
	name    string
	date    *types.Timestamp
	tbl     *doltdb.Table
	tblHash hash.Hash
}

func NewTblInfoAtCommit(name string, date *types.Timestamp, tbl *doltdb.Table, tblHash hash.Hash) TblInfoAtCommit {
	return TblInfoAtCommit{
		name, date, tbl, tblHash,
	}
}

var _ sql.Partition = (*DiffPartition)(nil)

// DiffPartition data partitioned into pairs of table states which get compared
type DiffPartition struct {
	to       *doltdb.Table
	from     *doltdb.Table
	toName   string
	fromName string
	toDate   *types.Timestamp
	fromDate *types.Timestamp
	toSch    *schema.Schema
	fromSch  *schema.Schema
}

func NewDiffPartition(to, from *doltdb.Table, toName, fromName string, toDate, fromDate *types.Timestamp, toSch, fromSch *schema.Schema) *DiffPartition {
	return &DiffPartition{
		to:       to,
		from:     from,
		toName:   toName,
		fromName: fromName,
		toDate:   toDate,
		fromDate: fromDate,
		toSch:    toSch,
		fromSch:  fromSch,
	}
}

func (dp DiffPartition) Key() []byte {
	return []byte(dp.toName + dp.fromName)
}

func (dp DiffPartition) GetRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, joiner *rowconv.Joiner) (sql.RowIter, error) {
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
	rd.Start(ctx, fromData, toData)

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

// isDiffablePartition checks if the commit pair for this partition is "diffable".
// If the primary key sets changed between the two commits, it may not be
// possible to diff them.
func (dp *DiffPartition) isDiffablePartition(ctx *sql.Context) (bool, error) {
	// dp.from is nil when the to commit created a new table
	if dp.from == nil {
		return true, nil
	}

	fromSch, err := dp.from.GetSchema(ctx)
	if err != nil {
		return false, err
	}

	// dp.to is nil when a table has been deleted previously. In this case, we return
	// false, to stop processing diffs, since that previously deleted table is considered
	// a logically different table and we don't want to mix the diffs together.
	if dp.to == nil {
		return false, nil
	}

	toSch, err := dp.to.GetSchema(ctx)
	if err != nil {
		return false, err
	}

	return schema.ArePrimaryKeySetsDiffable(fromSch, toSch), nil
}

type partitionSelectFunc func(*sql.Context, DiffPartition) (bool, error)

func SelectFuncForFilters(nbf *types.NomsBinFormat, filters []sql.Expression) (partitionSelectFunc, error) {
	const (
		toCommitTag uint64 = iota
		fromCommitTag
		toCommitDateTag
		fromCommitDateTag
	)

	colColl := schema.NewColCollection(
		schema.NewColumn(toCommit, toCommitTag, types.StringKind, false),
		schema.NewColumn(fromCommit, fromCommitTag, types.StringKind, false),
		schema.NewColumn(toCommitDate, toCommitDateTag, types.TimestampKind, false),
		schema.NewColumn(fromCommitDate, fromCommitDateTag, types.TimestampKind, false),
	)

	expFunc, err := expreval.ExpressionFuncFromSQLExpressions(nbf, schema.UnkeyedSchemaFromCols(colColl), filters)

	if err != nil {
		return nil, err
	}

	return func(ctx *sql.Context, partition DiffPartition) (bool, error) {
		vals := row.TaggedValues{
			toCommitTag:   types.String(partition.toName),
			fromCommitTag: types.String(partition.fromName),
		}

		if partition.toDate != nil {
			vals[toCommitDateTag] = *partition.toDate
		}

		if partition.fromDate != nil {
			vals[fromCommitDateTag] = *partition.fromDate
		}

		return expFunc(ctx, vals)
	}, nil
}

var _ sql.PartitionIter = &DiffPartitions{}

// DiffPartitions a collection of partitions. Implements PartitionItr
type DiffPartitions struct {
	tblName         string
	cmItr           doltdb.CommitItr
	cmHashToTblInfo map[hash.Hash]TblInfoAtCommit
	selectFunc      partitionSelectFunc
	toSch           schema.Schema
	fromSch         schema.Schema
}

func NewDiffPartitions(tblName string, cmItr doltdb.CommitItr, cmHashToTblInfo map[hash.Hash]TblInfoAtCommit, selectFunc partitionSelectFunc, toSch, fromSch schema.Schema) *DiffPartitions {
	return &DiffPartitions{
		tblName:         tblName,
		cmItr:           cmItr,
		cmHashToTblInfo: cmHashToTblInfo,
		selectFunc:      selectFunc,
		toSch:           toSch,
		fromSch:         fromSch,
	}
}

// processCommit is called in a commit iteration loop. Adds partitions when it finds a commit and its parent that have
// different values for the hash of the table being looked at.
func (dps *DiffPartitions) processCommit(ctx *sql.Context, cmHash hash.Hash, cm *doltdb.Commit, root *doltdb.RootValue, tbl *doltdb.Table) (*DiffPartition, error) {
	tblHash, _, err := root.GetTableHash(ctx, dps.tblName)

	if err != nil {
		return nil, err
	}

	toInfoForCommit := dps.cmHashToTblInfo[cmHash]
	cmHashStr := cmHash.String()
	meta, err := cm.GetCommitMeta(ctx)

	if err != nil {
		return nil, err
	}

	ts := types.Timestamp(meta.Time())

	var nextPartition *DiffPartition
	if tblHash != toInfoForCommit.tblHash {
		partition := DiffPartition{toInfoForCommit.tbl, tbl, toInfoForCommit.name, cmHashStr, toInfoForCommit.date, &ts, &dps.toSch, &dps.fromSch}
		selected, err := dps.selectFunc(ctx, partition)

		if err != nil {
			return nil, err
		}

		if selected {
			nextPartition = &partition
		}
	}

	newInfo := TblInfoAtCommit{cmHashStr, &ts, tbl, tblHash}
	parentHashes, err := cm.ParentHashes(ctx)

	if err != nil {
		return nil, err
	}

	for _, h := range parentHashes {
		dps.cmHashToTblInfo[h] = newInfo
	}

	return nextPartition, nil
}

func (dps *DiffPartitions) Next(ctx *sql.Context) (sql.Partition, error) {
	for {
		cmHash, cm, err := dps.cmItr.Next(ctx)
		if err != nil {
			return nil, err
		}

		root, err := cm.GetRootValue(ctx)

		if err != nil {
			return nil, err
		}

		tbl, _, _, err := root.GetTableInsensitive(ctx, dps.tblName)

		if err != nil {
			return nil, err
		}

		next, err := dps.processCommit(ctx, cmHash, cm, root, tbl)

		if err != nil {
			return nil, err
		}

		if next != nil {
			// If we can't diff this commit with its parent, don't traverse any lower
			canDiff, err := next.isDiffablePartition(ctx)
			if err != nil {
				return nil, err
			}

			if !canDiff {
				ctx.Warn(PrimaryKeyChangeWarningCode, fmt.Sprintf(PrimaryKeyChangeWarning, next.fromName, next.toName))
				return nil, io.EOF
			}

			return *next, nil
		}
	}
}

func (dps *DiffPartitions) Close(*sql.Context) error {
	return nil
}

// rowConvForSchema creates a RowConverter for transforming rows with the given schema a target schema.
func (dp DiffPartition) rowConvForSchema(ctx context.Context, vrw types.ValueReadWriter, targetSch, srcSch schema.Schema) (*rowconv.RowConverter, error) {
	if schema.SchemasAreEqual(srcSch, schema.EmptySchema) {
		return rowconv.IdentityConverter, nil
	}

	fm, err := rowconv.TagMappingWithNameFallback(srcSch, targetSch)
	if err != nil {
		return nil, err
	}

	return rowconv.NewRowConverter(ctx, vrw, fm)
}
