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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/expreval"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

const diffTableDefaultRowCount = 10

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
var _ sql.IndexedTable = (*DiffTable)(nil)
var _ sql.IndexAddressable = (*DiffTable)(nil)
var _ sql.StatisticsTable = (*DiffTable)(nil)

type DiffTable struct {
	workingRoot doltdb.RootValue
	// from and to need to be mapped to this schema
	targetSch schema.Schema
	// the schema for the diff table itself. Once from and to are converted to
	// targetSch, the commit names and dates are inserted.
	diffTableSch     schema.Schema
	ddb              *doltdb.DoltDB
	head             *doltdb.Commit
	headAncestry     *doltdb.CommitAncestry
	table            *doltdb.Table
	tableName        doltdb.TableName
	sqlSch           sql.PrimaryKeySchema
	partitionFilters []sql.Expression
	headHash         hash.Hash
}

var PrimaryKeyChangeWarning = "cannot render full diff between commits %s and %s due to primary key set change"

const PrimaryKeyChangeWarningCode int = 1105 // Since this is our own custom warning we'll use 1105, the code for an unknown error

func getTableInsensitiveOrError(ctx *sql.Context, root doltdb.RootValue, tblName doltdb.TableName) (*doltdb.Table, doltdb.TableName, error) {
	table, correctedTableName, tableExists, err := doltdb.GetTableInsensitive(ctx, root, tblName)
	if err != nil {
		return nil, tblName, err
	}
	if !tableExists {
		return nil, tblName, sql.ErrTableNotFound.New(tblName.String())
	}
	tblName.Name = correctedTableName
	return table, tblName, nil
}

func NewDiffTable(ctx *sql.Context, dbName string, tblName doltdb.TableName, ddb *doltdb.DoltDB, root doltdb.RootValue, head *doltdb.Commit) (sql.Table, error) {
	diffTblName := doltdb.DoltDiffTablePrefix + tblName.Name

	var table *doltdb.Table
	var err error
	table, tblName, err = getTableInsensitiveOrError(ctx, root, tblName)
	if err != nil {
		return nil, err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: This assumes the schema has not changed, but it's possible that columns have been added or deleted.
	//  https://github.com/dolthub/dolt/issues/11140
	diffTableSchema, err := GetDiffTableSchemaAndJoiner(sch, sch)
	if err != nil {
		return nil, err
	}

	sqlSch, err := sqlutil.FromDoltSchema(ctx, dbName, diffTblName, diffTableSchema)
	if err != nil {
		return nil, err
	}

	return &DiffTable{
		tableName:        tblName,
		ddb:              ddb,
		workingRoot:      root,
		head:             head,
		targetSch:        sch,
		diffTableSch:     diffTableSchema,
		sqlSch:           sqlSch,
		partitionFilters: nil,
		table:            table,
	}, nil
}

func (dt *DiffTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dt.Schema(ctx))
	numRows, _, err := dt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (dt *DiffTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return diffTableDefaultRowCount, false, nil
}

func (dt *DiffTable) Name() string {
	return doltdb.DoltDiffTablePrefix + dt.tableName.Name
}

func (dt *DiffTable) String() string {
	return doltdb.DoltDiffTablePrefix + dt.tableName.Name
}

func (dt *DiffTable) Schema(ctx *sql.Context) sql.Schema {
	return dt.sqlSch.Schema
}

func (dt *DiffTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (dt *DiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return dt.PartitionRanges(ctx, nil)
}

// PartitionRanges creates a [sql.PartitionIter] across all commits reachable
// from HEAD.
//
// It diffs each reachable commit against its parents via
// [commitwalk.GetTopologicalOrderIterator], preceded by an uncommitted working
// set partition filtered by |ranges|.
func (dt *DiffTable) PartitionRanges(ctx *sql.Context, ranges []prolly.Range) (sql.PartitionIter, error) {
	headHash, err := dt.HeadHash()
	if err != nil {
		return nil, err
	}

	cmItr, err := commitwalk.GetTopologicalOrderIterator[*sql.Context](ctx, dt.ddb, []hash.Hash{headHash}, nil)
	if err != nil {
		return nil, err
	}

	sf, err := SelectFuncForFilters(ctx, dt.partitionFilters)
	if err != nil {
		return nil, err
	}

	dps := &DiffPartitions{
		tblName:    dt.tableName,
		cmItr:      cmItr,
		selectFunc: sf,
		toSch:      dt.targetSch,
		fromSch:    dt.targetSch,
		ranges:     ranges,
	}

	if err := dt.addWorkingPartition(ctx, dps, headHash); err != nil {
		return nil, err
	}

	return dps, nil
}

var commitMetaColumns = set.NewStrSet([]string{toCommit, fromCommit, toCommitDate, fromCommitDate})

// CommitIsInScope returns true if a given commit hash is head or is
// visible from the current head's ancestry graph.
func (dt *DiffTable) CommitIsInScope(ctx context.Context, height uint64, h hash.Hash) (bool, error) {
	a, err := dt.HeadAncestry(ctx)
	if err != nil {
		return false, err
	}
	return a.Contains(ctx, h, height)
}

func (dt *DiffTable) HeadAncestry(ctx context.Context) (*doltdb.CommitAncestry, error) {
	if dt.headAncestry == nil {
		a, err := dt.head.GetAncestry(ctx)
		if err != nil {
			return nil, err
		}
		dt.headAncestry = a
	}
	return dt.headAncestry, nil
}

func (dt *DiffTable) HeadHash() (hash.Hash, error) {
	if dt.headHash.IsEmpty() {
		var err error
		dt.headHash, err = dt.head.HashOf()
		if err != nil {
			return hash.Hash{}, err
		}
	}
	return dt.headHash, nil
}

func (dt *DiffTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	dp := part.(DiffPartition)
	return dp.GetRowIter(ctx)
}

func (dt *DiffTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	switch lookup.Index.ID() {
	case index.ToCommitIndexId:
		hs, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(ctx, lookup.Ranges))
		}
		hashes, commits, _ := index.HashesToCommits(ctx, dt.ddb, hs, dt.head, false)
		if len(hashes) == 0 {
			return sql.PartitionsToPartitionIter(), nil
		}
		return dt.toCommitLookupPartitions(ctx, hashes, commits)
	case index.FromCommitIndexId:
		hs, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(ctx, lookup.Ranges))
		}
		hashes, commits, _ := index.HashesToCommits(ctx, dt.ddb, hs, nil, false)
		if len(hashes) == 0 {
			return sql.PartitionsToPartitionIter(), nil
		}
		return dt.fromCommitLookupPartitions(ctx, hashes, commits)
	default:
		ranges, err := index.ProllyRangesFromIndexLookup(ctx, lookup)
		if err != nil {
			return nil, err
		}
		return dt.PartitionRanges(ctx, ranges)
	}
}

// fromCommitLookupPartitions creates a [sql.PartitionIter] for the specified
// parent |hashes| and |commits|.
//
// It configures a [commitwalk.GetTopologicalOrderIterator] down to the minimum
// height of the requested parents, retaining every child referencing a selected
// parent.
func (dt *DiffTable) fromCommitLookupPartitions(ctx *sql.Context, hashes []hash.Hash, commits []*doltdb.Commit) (sql.PartitionIter, error) {
	headHash, err := dt.HeadHash()
	if err != nil {
		return nil, err
	}

	sf, err := SelectFuncForFilters(ctx, dt.partitionFilters)
	if err != nil {
		return nil, err
	}

	dps := &DiffPartitions{
		tblName:    dt.tableName,
		selectFunc: sf,
		toSch:      dt.targetSch,
		fromSch:    dt.targetSch,
	}

	selected := make(hash.HashSet, len(hashes))
	var minHeight uint64

	for i, h := range hashes {
		if h == headHash {
			if err := dt.addWorkingPartition(ctx, dps, headHash); err != nil {
				return nil, err
			}
			continue
		}

		cm := commits[i]
		height, err := cm.Height()
		if err != nil {
			return nil, err
		}
		ok, err := dt.CommitIsInScope(ctx, height, h)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if len(selected) == 0 || height < minHeight {
			minHeight = height
		}
		selected.Insert(h)
	}

	if len(selected) == 0 {
		return dps, nil
	}

	cmItr, err := commitwalk.GetTopologicalOrderIterator[*sql.Context](ctx, dt.ddb, []hash.Hash{headHash}, nil)
	if err != nil {
		return nil, err
	}
	dps.cmItr = cmItr
	dps.fromCommits = selected
	dps.minHeight = minHeight

	return dps, nil
}

// tableInfoForCommit loads the TblInfoAtCommit for |tableName| at |cm|.
//
// It resolves the [doltdb.Table] and its schema hash from the root value of
// |cm|, packaging the committer timestamp with the commit address |hs|.
func tableInfoForCommit(ctx *sql.Context, tableName doltdb.TableName, cm *doltdb.Commit, hs hash.Hash) (TblInfoAtCommit, error) {
	r, err := cm.GetRootValue(ctx)
	if err != nil {
		return TblInfoAtCommit{}, err
	}

	tbl, _, ok, err := doltdb.GetTableInsensitive(ctx, r, tableName)
	if err != nil {
		return TblInfoAtCommit{}, err
	}

	var tblHash hash.Hash
	if ok {
		tblHash, _, err = r.GetTableHash(ctx, tableName)
		if err != nil {
			return TblInfoAtCommit{}, err
		}
	} // Still want the commit metadata below.

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return TblInfoAtCommit{}, err
	}

	ts := types.Timestamp(meta.Committer.Date.Time())
	return NewTblInfoAtCommit(hs.String(), &ts, tbl, tblHash), nil
}

// toCommitLookupPartitions creates a [sql.PartitionIter] for the specified
// child |hashes| and |commits|.
//
// It filters the requested |commits| to those in scope and configures a
// [doltdb.CommitSliceIter] so each commit's parent diffs are resolved lazily as
// DiffPartitions.Next advances.
func (dt *DiffTable) toCommitLookupPartitions(ctx *sql.Context, hashes []hash.Hash, commits []*doltdb.Commit) (sql.PartitionIter, error) {
	headHash, err := dt.HeadHash()
	if err != nil {
		return nil, err
	}

	sf, err := SelectFuncForFilters(ctx, dt.partitionFilters)
	if err != nil {
		return nil, err
	}

	dps := &DiffPartitions{
		tblName:    dt.tableName,
		selectFunc: sf,
		toSch:      dt.targetSch,
		fromSch:    dt.targetSch,
	}

	inScopeHashes := make([]hash.Hash, 0, len(hashes))
	inScopeCommits := make([]*doltdb.Commit, 0, len(commits))

	for i, h := range hashes {
		if h == headHash && commits[i] == nil {
			if err := dt.addWorkingPartition(ctx, dps, headHash); err != nil {
				return nil, err
			}
			continue
		}

		cm := commits[i]
		height, err := cm.Height()
		if err != nil {
			return nil, err
		}
		ok, err := dt.CommitIsInScope(ctx, height, h)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		inScopeHashes = append(inScopeHashes, h)
		inScopeCommits = append(inScopeCommits, cm)
	}

	if len(inScopeCommits) > 0 {
		dps.cmItr = doltdb.NewCommitSliceIter[*sql.Context](inScopeCommits, inScopeHashes)
	}

	return dps, nil
}

// addWorkingPartition appends a partition diffing the uncommitted working set
// against |headHash|.
//
// If table changes exist between the working root and HEAD, the partition is
// appended to the DiffPartitions buffer.
func (dt *DiffTable) addWorkingPartition(ctx *sql.Context, dps *DiffPartitions, headHash hash.Hash) error {
	workingTblHash, _, err := dt.workingRoot.GetTableHash(ctx, dt.tableName)
	if err != nil {
		return err
	}
	workingInfo := TblInfoAtCommit{name: "WORKING", tbl: dt.table, tblHash: workingTblHash}
	headInfo, err := tableInfoForCommit(ctx, dt.tableName, dt.head, headHash)
	if err != nil {
		return err
	}
	return dps.queuePartition(ctx, workingInfo, headInfo)
}

// GetIndexes implements sql.IndexAddressable
func (dt *DiffTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltDiffIndexesFromTable(ctx, "", dt.tableName.Name, dt.table)
}

// IndexedAccess implements sql.IndexAddressable
func (dt *DiffTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	nt := *dt
	return &nt
}

// PreciseMatch implements sql.IndexAddressable
func (dt *DiffTable) PreciseMatch() bool {
	return false
}

type TblInfoAtCommit struct {
	date    *types.Timestamp
	tbl     *doltdb.Table
	name    string
	tblHash hash.Hash
}

func NewTblInfoAtCommit(name string, date *types.Timestamp, tbl *doltdb.Table, tblHash hash.Hash) TblInfoAtCommit {
	return TblInfoAtCommit{
		name: name, date: date, tbl: tbl, tblHash: tblHash,
	}
}

func (ti TblInfoAtCommit) IsEmpty() bool {
	return ti.name == ""
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
	toSch   schema.Schema
	fromSch schema.Schema
	ranges  []prolly.Range
}

func NewDiffPartition(to, from *doltdb.Table, toName, fromName string, toDate, fromDate *types.Timestamp, toSch, fromSch schema.Schema, ranges []prolly.Range) *DiffPartition {
	return &DiffPartition{
		to:       to,
		from:     from,
		toName:   toName,
		fromName: fromName,
		toDate:   toDate,
		fromDate: fromDate,
		toSch:    toSch,
		fromSch:  fromSch,
		ranges:   ranges,
	}
}

func (dp DiffPartition) Key() []byte {
	// TODO: schema name
	return []byte(dp.toName + dp.fromName)
}

func (dp DiffPartition) GetRowIter(ctx *sql.Context) (sql.RowIter, error) {
	return newProllyDiffIter(ctx, dp, dp.fromSch, dp.toSch, dp.ranges)
}

// isDiffablePartition checks if the commit pair for this partition is "diffable".
//
// simpleDiff is returned when [schema.ComparePrimaryKeys] does not return
// [schema.PrimaryKeysDiffer]. fuzzyDiff is returned when the keys can still be
// mapped by [schema.MapSchemaBasedOnTagAndName]. Neither is returned when the
// partition cannot be diffed.
func (dp *DiffPartition) isDiffablePartition(ctx *sql.Context) (simpleDiff bool, fuzzyDiff bool, err error) {
	// dp.to is nil when a table has been deleted previously. In this case, we return
	// false, to stop processing diffs, since that previously deleted table is considered
	// a logically different table and we don't want to mix the diffs together.
	if dp.to == nil {
		return false, false, nil
	}

	// dp.from is nil when the to commit created a new table
	if dp.from == nil {
		return true, false, nil
	}

	fromSch, err := dp.from.GetSchema(ctx)
	if err != nil {
		return false, false, err
	}

	toSch, err := dp.to.GetSchema(ctx)
	if err != nil {
		return false, false, err
	}

	easyDiff := schema.ComparePrimaryKeys(fromSch, toSch) != schema.PrimaryKeysDiffer
	if easyDiff {
		return true, false, nil
	}

	_, _, err = schema.MapSchemaBasedOnTagAndName(fromSch, toSch)
	if err == nil {
		return false, true, nil
	}
	return false, false, nil
}

type partitionSelectFunc func(*sql.Context, DiffPartition) (bool, error)

func SelectFuncForFilters(ctx *sql.Context, filters []sql.Expression) (partitionSelectFunc, error) {
	sch := sql.Schema{
		{Name: toCommit, Type: gmstypes.LongText},
		{Name: fromCommit, Type: gmstypes.LongText},
		{Name: toCommitDate, Type: gmstypes.Datetime},
		{Name: fromCommitDate, Type: gmstypes.Datetime},
	}

	expFunc, err := expreval.ExpressionFuncFromSQLExpressions(ctx, sch, filters)
	if err != nil {
		return nil, err
	}

	return func(ctx *sql.Context, partition DiffPartition) (bool, error) {
		var toDate, fromDate interface{}
		if partition.toDate != nil {
			toDate = time.Time(*partition.toDate)
		}
		if partition.fromDate != nil {
			fromDate = time.Time(*partition.fromDate)
		}
		row := sql.Row{partition.toName, partition.fromName, toDate, fromDate}
		return expFunc(ctx, row)
	}, nil
}

var _ sql.PartitionIter = &DiffPartitions{}

// DiffPartitions a collection of partitions. Implements PartitionItr
type DiffPartitions struct {
	tblName     doltdb.TableName
	cmItr       doltdb.CommitItr[*sql.Context]
	toSch       schema.Schema
	fromSch     schema.Schema
	selectFunc  partitionSelectFunc
	fromCommits hash.HashSet
	ranges      []prolly.Range
	buf         []DiffPartition
	minHeight   uint64
	stopNext    bool
}

// Next returns the next [sql.Partition] in the iteration sequence.
//
// It drains buffered partitions before advancing the [doltdb.CommitItr].
// If a partition crosses an altered primary key boundary that can only be
// diffed approximately, Next returns that partition and halts subsequent
// iteration.
func (dps *DiffPartitions) Next(ctx *sql.Context) (sql.Partition, error) {
	if dps.stopNext {
		return nil, io.EOF
	}

	for {
		if len(dps.buf) > 0 {
			next := dps.buf[0]
			dps.buf = dps.buf[1:]

			simpleDiff, fuzzyDiff, err := next.isDiffablePartition(ctx)
			if err != nil {
				return nil, err
			}

			if !simpleDiff && !fuzzyDiff {
				ctx.Warn(PrimaryKeyChangeWarningCode, PrimaryKeyChangeWarning, next.fromName, next.toName)
				return nil, io.EOF
			}

			if !simpleDiff && fuzzyDiff {
				ctx.Warn(PrimaryKeyChangeWarningCode, PrimaryKeyChangeWarning, next.fromName, next.toName)
				dps.stopNext = true
			}

			return next, nil
		}

		if dps.cmItr == nil {
			return nil, io.EOF
		}

		cmHash, optCmt, _, height, err := dps.cmItr.Next(ctx)
		if err != nil {
			return nil, err
		}
		if dps.minHeight > 0 && height <= dps.minHeight {
			return nil, io.EOF
		}
		cm, ok := optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitRuntimeFailure
		}

		if err := dps.processCommit(ctx, cmHash, cm); err != nil {
			return nil, err
		}
	}
}

// processCommit resolves parent revisions for |cm| and queues diff partitions
// for any modified table states.
//
// It iterates each parent of |cm|, filtering by fromCommits when configured and
// delegating to addParentPartition.
func (dps *DiffPartitions) processCommit(ctx *sql.Context, cmHash hash.Hash, cm *doltdb.Commit) error {
	phs, err := cm.ParentHashes(ctx)
	if err != nil {
		return err
	}

	var toInfo TblInfoAtCommit
	loaded := false

	for i, ph := range phs {
		if dps.fromCommits != nil && !dps.fromCommits.Has(ph) {
			continue
		}
		if !loaded {
			toInfo, err = tableInfoForCommit(ctx, dps.tblName, cm, cmHash)
			if err != nil {
				return err
			}
			loaded = true
		}
		optParent, err := cm.GetParent(ctx, i)
		if err != nil {
			return err
		}
		if err := dps.addParentPartition(ctx, toInfo, optParent); err != nil {
			return err
		}
	}
	return nil
}

// addParentPartition resolves |optParent| and queues a diff partition against
// |toInfo|.
//
// If |optParent| resolves to a [doltdb.Commit] and table hashes differ, the
// partition is appended to the buffer.
func (dps *DiffPartitions) addParentPartition(ctx *sql.Context, toInfo TblInfoAtCommit, optParent *doltdb.OptionalCommit) error {
	parentCm, ok := optParent.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}
	fromInfo, err := tableInfoForCommit(ctx, dps.tblName, parentCm, optParent.Addr)
	if err != nil {
		return err
	}
	return dps.queuePartition(ctx, toInfo, fromInfo)
}

// queuePartition appends a DiffPartition comparing |toInfo| and |fromInfo| to
// the buffer if table hashes differ and the partition satisfies selectFunc.
func (dps *DiffPartitions) queuePartition(ctx *sql.Context, toInfo, fromInfo TblInfoAtCommit) error {
	if toInfo.tblHash == fromInfo.tblHash {
		return nil
	}
	partition := DiffPartition{
		to:       toInfo.tbl,
		from:     fromInfo.tbl,
		toName:   toInfo.name,
		fromName: fromInfo.name,
		toDate:   toInfo.date,
		fromDate: fromInfo.date,
		fromSch:  dps.fromSch,
		toSch:    dps.toSch,
		ranges:   dps.ranges,
	}
	selected, err := dps.selectFunc(ctx, partition)
	if err != nil || !selected {
		return err
	}
	dps.buf = append(dps.buf, partition)
	return nil
}

func (dps *DiffPartitions) Close(*sql.Context) error {
	return nil
}

// GetDiffTableSchemaAndJoiner returns the schema for the diff table given a
// target schema for a row |sch|. In the old storage format, it also returns the
// associated joiner.
func GetDiffTableSchemaAndJoiner(fromSch, toSch schema.Schema) (diffTableSchema schema.Schema, err error) {
	return CalculateDiffSchema(fromSch, toSch)
}

// expandFromToSchemas converts input schemas to schemas appropriate for diffs. One argument must be
// non-nil. If one is null, the result will be the columns of the non-nil argument.
func expandFromToSchemas(fromSch, toSch schema.Schema) (newFromSch, newToSch schema.Schema, err error) {
	var fromClmCol, toClmCol *schema.ColCollection
	if fromSch == nil && toSch == nil {
		return nil, nil, errors.New("non-nil argument required to CalculateDiffSchema")
	} else if fromSch == nil {
		fromClmCol = toSch.GetAllCols()
		toClmCol = toSch.GetAllCols()
	} else if toSch == nil {
		toClmCol = fromSch.GetAllCols()
		fromClmCol = fromSch.GetAllCols()
	} else {
		fromClmCol = fromSch.GetAllCols()
		toClmCol = toSch.GetAllCols()
	}

	fromClmCol = fromClmCol.Append(
		schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
		schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
	newFromSch = schema.MustSchemaFromCols(fromClmCol)

	toClmCol = toClmCol.Append(
		schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
		schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
	newToSch = schema.MustSchemaFromCols(toClmCol)

	return
}

// CalculateDiffSchema returns the schema for the dolt_diff table based on the schemas from the from and to tables.
// Either may be nil, in which case the nil argument will use the schema of the non-nil argument
func CalculateDiffSchema(fromSch, toSch schema.Schema) (schema.Schema, error) {
	fromSch, toSch, err := expandFromToSchemas(fromSch, toSch)
	if err != nil {
		return nil, err
	}

	cols := make([]schema.Column, toSch.GetAllCols().Size()+fromSch.GetAllCols().Size()+1)

	i := 0
	err = toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		toCol, err := schema.NewColumnWithTypeInfo(diff.ToColNamer(col.Name), uint64(i), col.TypeInfo, false, col.Default, false, col.Comment)
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
		fromCol, err := schema.NewColumnWithTypeInfo(diff.FromColNamer(col.Name), uint64(j), col.TypeInfo, false, col.Default, false, col.Comment)
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

	cols[len(cols)-1] = schema.NewColumn(diffTypeColName, schema.DiffTypeTag, types.StringKind, false)

	return schema.UnkeyedSchemaFromCols(schema.NewColCollection(cols...)), nil
}
