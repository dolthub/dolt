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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/expreval"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
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
var _ sql.IndexedTable = (*DiffTable)(nil)
var _ sql.ParallelizedIndexAddressableTable = (*DiffTable)(nil)

type DiffTable struct {
	name        string
	ddb         *doltdb.DoltDB
	workingRoot *doltdb.RootValue
	head        *doltdb.Commit

	// from and to need to be mapped to this schema
	targetSch schema.Schema

	// the schema for the diff table itself. Once from and to are converted to
	// targetSch, the commit names and dates are inserted.
	diffTableSch schema.Schema

	sqlSch           sql.PrimaryKeySchema
	partitionFilters []sql.Expression
	rowFilters       []sql.Expression

	table  *doltdb.Table
	lookup sql.IndexLookup

	// noms only
	joiner *rowconv.Joiner
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

	diffTableSchema, j, err := GetDiffTableSchemaAndJoiner(ddb.Format(), sch, sch)
	if err != nil {
		return nil, err
	}

	sqlSch, err := sqlutil.FromDoltSchema(diffTblName, diffTableSchema)
	if err != nil {
		return nil, err
	}

	return &DiffTable{
		name:             tblName,
		ddb:              ddb,
		workingRoot:      root,
		head:             head,
		targetSch:        sch,
		diffTableSch:     diffTableSchema,
		sqlSch:           sqlSch,
		partitionFilters: nil,
		rowFilters:       nil,
		table:            table,
		joiner:           j,
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
	return dp.GetRowIter(ctx, dt.ddb, dt.joiner, dt.lookup)
}

func (dt *DiffTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltDiffIndexesFromTable(ctx, "", dt.name, dt.table)
}

func (dt *DiffTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	if lookup == nil {
		return dt
	}

	nt := *dt
	nt.lookup = lookup

	return &nt
}

func (dt *DiffTable) ShouldParallelizeAccess() bool {
	return true
}

// tableData returns the map of primary key to values for the specified table (or an empty map if the tbl is null)
// and the schema of the table (or EmptySchema if tbl is null).
func tableData(ctx *sql.Context, tbl *doltdb.Table, ddb *doltdb.DoltDB) (durable.Index, schema.Schema, error) {
	var data durable.Index
	var err error

	if tbl == nil {
		data, err = durable.NewEmptyIndex(ctx, ddb.ValueReadWriter(), schema.EmptySchema)
		if err != nil {
			return nil, nil, err
		}
	} else {
		data, err = tbl.GetRowData(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	var sch schema.Schema
	if tbl == nil {
		sch = schema.EmptySchema
	} else {
		sch, err = tbl.GetSchema(ctx)

		if err != nil {
			return nil, nil, err
		}
	}

	return data, sch, nil
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
	// fromSch and toSch are usually identical. It is the schema of the table at head.
	toSch   *schema.Schema
	fromSch *schema.Schema
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

func (dp DiffPartition) GetRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, joiner *rowconv.Joiner, lookup sql.IndexLookup) (sql.RowIter, error) {
	if types.IsFormat_DOLT_1(ddb.Format()) {
		return newProllyDiffIter(ctx, dp, ddb, *dp.fromSch, *dp.toSch)
	} else {
		return newNomsDiffIter(ctx, ddb, joiner, dp, lookup)
	}
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
		partition := DiffPartition{
			to:       toInfoForCommit.tbl,
			from:     tbl,
			toName:   toInfoForCommit.name,
			fromName: cmHashStr,
			toDate:   toInfoForCommit.date,
			fromDate: &ts,
			fromSch:  &dps.fromSch,
			toSch:    &dps.toSch,
		}
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

	fm, err := rowconv.TagMappingByName(srcSch, targetSch)
	if err != nil {
		return nil, err
	}

	return rowconv.NewRowConverter(ctx, vrw, fm)
}

// GetDiffTableSchemaAndJoiner returns the schema for the diff table given a
// target schema for a row |sch|. In the old storage format, it also returns the
// associated joiner.
func GetDiffTableSchemaAndJoiner(format *types.NomsBinFormat, fromTargetSch, toTargetSch schema.Schema) (diffTableSchema schema.Schema, j *rowconv.Joiner, err error) {
	if format == types.Format_DOLT_1 {
		diffTableSchema, err = CalculateDiffSchema(fromTargetSch, toTargetSch)
		if err != nil {
			return nil, nil, err
		}
	} else {
		colCollection := toTargetSch.GetAllCols()
		colCollection = colCollection.Append(
			schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
			schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
		toTargetSch = schema.MustSchemaFromCols(colCollection)

		colCollection = fromTargetSch.GetAllCols()
		colCollection = colCollection.Append(
			schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
			schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
		fromTargetSch = schema.MustSchemaFromCols(colCollection)

		j, err = rowconv.NewJoiner(
			[]rowconv.NamedSchema{{Name: diff.To, Sch: toTargetSch}, {Name: diff.From, Sch: fromTargetSch}},
			map[string]rowconv.ColNamingFunc{
				diff.To:   diff.ToColNamer,
				diff.From: diff.FromColNamer,
			})
		if err != nil {
			return nil, nil, err
		}
		diffTableSchema = j.GetSchema()
		colCollection = diffTableSchema.GetAllCols()
		colCollection = colCollection.Append(
			schema.NewColumn(diffTypeColName, schema.DiffTypeTag, types.StringKind, false),
		)
		diffTableSchema = schema.MustSchemaFromCols(colCollection)
	}

	return
}

// CalculateDiffSchema returns the schema for the dolt_diff table based on the
// schemas from the from and to tables.
func CalculateDiffSchema(fromSch schema.Schema, toSch schema.Schema) (schema.Schema, error) {
	colCollection := fromSch.GetAllCols()
	colCollection = colCollection.Append(
		schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
		schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
	fromSch = schema.MustSchemaFromCols(colCollection)

	colCollection = toSch.GetAllCols()
	colCollection = colCollection.Append(
		schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
		schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
	toSch = schema.MustSchemaFromCols(colCollection)

	cols := make([]schema.Column, toSch.GetAllCols().Size()+fromSch.GetAllCols().Size()+1)

	i := 0
	err := toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		toCol, err := schema.NewColumnWithTypeInfo("to_"+col.Name, uint64(i), col.TypeInfo, false, col.Default, false, col.Comment)
		if err != nil {
			return true, err
		}
		cols[i] = toCol
		i++
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	j := toSch.GetAllCols().Size()
	err = fromSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		fromCol, err := schema.NewColumnWithTypeInfo("from_"+col.Name, uint64(i), col.TypeInfo, false, col.Default, false, col.Comment)
		if err != nil {
			return true, err
		}
		cols[j] = fromCol

		j++
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	cols[len(cols)-1] = schema.NewColumn("diff_type", schema.DiffTypeTag, types.StringKind, false)

	return schema.UnkeyedSchemaFromCols(schema.NewColCollection(cols...)), nil
}
