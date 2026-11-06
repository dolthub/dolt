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
	return dt.partitionsForRanges(ctx, nil)
}

// partitionsForRanges creates a [sql.PartitionIter] across all commits
// reachable from HeadHash, limiting rows to the primary key |ranges|.
//
// The [commitwalk.GetTopologicalOrderIterator] visits each commit once,
// covering a merge commit and both of its parents before walking deeper
// so the most recent commits are shown first.
//
// The working set DiffPartition, unless unchanged, is created and buffered
// since it's a [doltdb.RootValue] outside the commit graph.
func (dt *DiffTable) partitionsForRanges(ctx *sql.Context, ranges []prolly.Range) (sql.PartitionIter, error) {
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

	dpi := &diffPartitionIter{
		tblName:    dt.tableName,
		cmItr:      cmItr,
		selectFunc: sf,
		toSch:      dt.targetSch,
		fromSch:    dt.targetSch,
		ranges:     ranges,
	}

	workingInfo, err := dt.workingTableInfo(ctx)
	if err != nil {
		return nil, err
	}
	headInfo, err := tableInfoForCommit(ctx, dt.tableName, dt.head, headHash)
	if err != nil {
		return nil, err
	}
	if err := dpi.queue(ctx, headInfo, workingInfo); err != nil {
		return nil, err
	}

	return dpi, nil
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
		return dt.partitionsForToCommits(ctx, hashes, commits)
	case index.FromCommitIndexId:
		hs, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(ctx, lookup.Ranges))
		}
		hashes, commits, _ := index.HashesToCommits(ctx, dt.ddb, hs, nil, false)
		if len(hashes) == 0 {
			return sql.PartitionsToPartitionIter(), nil
		}
		return dt.partitionsForFromCommits(ctx, hashes, commits)
	default:
		ranges, err := index.ProllyRangesFromIndexLookup(ctx, lookup)
		if err != nil {
			return nil, err
		}
		return dt.partitionsForRanges(ctx, ranges)
	}
}

// partitionsForFromCommits creates a [sql.PartitionIter] that traverses the
// commit graph from HeadHash to find the children of parent commits specified
// by |hashes| and |commits|.
//
// The [commitwalk.GetTopologicalOrderIterator] halts at the minimum height of
// the target parents, since no commit at or below that height can reference
// them.
//
// The in-scope parent |hashes| are recorded in diffPartitionIter.parentHashFilter
// so only child commits referencing those parents are later evaluated. The
// working set DiffPartition is buffered similarly to partitionsForRanges.
func (dt *DiffTable) partitionsForFromCommits(ctx *sql.Context, hashes []hash.Hash, commits []*doltdb.Commit) (sql.PartitionIter, error) {
	headHash, err := dt.HeadHash()
	if err != nil {
		return nil, err
	}

	sf, err := SelectFuncForFilters(ctx, dt.partitionFilters)
	if err != nil {
		return nil, err
	}

	dpi := &diffPartitionIter{
		tblName:    dt.tableName,
		selectFunc: sf,
		toSch:      dt.targetSch,
		fromSch:    dt.targetSch,
	}

	parentHashFilter := make(hash.HashSet, len(hashes))
	var minHeight uint64

	for i, h := range hashes {
		// The working root only has this parent.
		if h == headHash {
			workingInfo, err := dt.workingTableInfo(ctx)
			if err != nil {
				return nil, err
			}
			headInfo, err := tableInfoForCommit(ctx, dt.tableName, dt.head, headHash)
			if err != nil {
				return nil, err
			}
			if err := dpi.queue(ctx, headInfo, workingInfo); err != nil {
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
		// len check to detect first in-scope commit height.
		if len(parentHashFilter) == 0 || height < minHeight {
			minHeight = height
		}
		parentHashFilter.Insert(h)
	}

	// After dpi.buf helps skips graph walk to immediately return io.EOF.
	if len(parentHashFilter) == 0 {
		return dpi, nil
	}

	cmItr, err := commitwalk.GetTopologicalOrderIterator[*sql.Context](ctx, dt.ddb, []hash.Hash{headHash}, nil)
	if err != nil {
		return nil, err
	}
	dpi.cmItr = cmItr
	dpi.parentHashFilter = parentHashFilter
	dpi.minHeight = minHeight

	return dpi, nil
}

// tableInfoForCommit loads the TblInfoAtCommit for |tableName| at |cm|.
//
// It loads the [doltdb.Table] and table hash from the root of |cm|, pairing
// them with the committer timestamp and commit address |hs|.
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
	} // Still retain metadata when !ok for tbl deletion or addition.

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return TblInfoAtCommit{}, err
	}

	ts := types.Timestamp(meta.Committer.Date.Time())
	return NewTblInfoAtCommit(hs.String(), &ts, tbl, tblHash), nil
}

// partitionsForToCommits creates a [sql.PartitionIter] that finds the parents
// of child commits specified by |hashes| and |commits|.
//
// Because each commit points directly to its own parents, pairs can be found
// without walking the commit graph from HeadHash. The in-scope |commits| are
// wrapped in a [doltdb.CommitSliceIter] to be later resolved lazily as the
// partition iterator advances.
//
// The working set DiffPartition is buffered similarly to partitionsForRanges.
func (dt *DiffTable) partitionsForToCommits(ctx *sql.Context, hashes []hash.Hash, commits []*doltdb.Commit) (sql.PartitionIter, error) {
	headHash, err := dt.HeadHash()
	if err != nil {
		return nil, err
	}

	sf, err := SelectFuncForFilters(ctx, dt.partitionFilters)
	if err != nil {
		return nil, err
	}

	dpi := &diffPartitionIter{
		tblName:    dt.tableName,
		selectFunc: sf,
		toSch:      dt.targetSch,
		fromSch:    dt.targetSch,
	}

	inScopeHashes := make([]hash.Hash, 0, len(hashes))
	inScopeCommits := make([]*doltdb.Commit, 0, len(commits))

	for i, h := range hashes {
		// The working root has a root hash but no commit object.
		// HashesToCommits uses headHash as a placeholder and a nil commit
		// to indicate the working root.
		if h == headHash && commits[i] == nil {
			workingInfo, err := dt.workingTableInfo(ctx)
			if err != nil {
				return nil, err
			}
			headInfo, err := tableInfoForCommit(ctx, dt.tableName, dt.head, headHash)
			if err != nil {
				return nil, err
			}
			if err := dpi.queue(ctx, headInfo, workingInfo); err != nil {
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

	// After dpi.buf helps skip alloc and interfacing with the iter.
	if len(inScopeCommits) == 0 {
		return dpi, nil
	}

	dpi.cmItr = doltdb.NewCommitSliceIter[*sql.Context](inScopeCommits, inScopeHashes)
	return dpi, nil
}

// workingTableInfo returns TblInfoAtCommit for the uncommitted working root.
func (dt *DiffTable) workingTableInfo(ctx *sql.Context) (TblInfoAtCommit, error) {
	workingTblHash, _, err := dt.workingRoot.GetTableHash(ctx, dt.tableName)
	if err != nil {
		return TblInfoAtCommit{}, err
	}
	return TblInfoAtCommit{name: doltdb.Working, tbl: dt.table, tblHash: workingTblHash}, nil
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

var _ sql.PartitionIter = &diffPartitionIter{}

// diffPartitionIter a collection of partitions. Implements PartitionItr
type diffPartitionIter struct {
	tblName          doltdb.TableName
	cmItr            doltdb.CommitItr[*sql.Context]
	toSch            schema.Schema
	fromSch          schema.Schema
	selectFunc       partitionSelectFunc
	parentHashFilter hash.HashSet
	ranges           []prolly.Range
	buf              []DiffPartition
	// minHeight halts Next on the first commit at or below this height.
	// In a topological walk, it reduces search space to target parents.
	minHeight uint64
	stopNext  bool
}

// Next returns the next DiffPartition by advancing the configured commit
// iterator to match commits with their parents.
//
// A buffer is used for any DiffPartition that does not map 1:1 with the commit
// iterator. Merge commits can yield multiple partitions across merge parents,
// but Next can only return them one at a time. The working root lives outside
// the commit graph, so it's typically buffered upfront too by constructors.
//
// If primary key column definitions or types differ, Next emits a warning. A
// partition can be returned if the schema is mapped approximately, but any
// later iteration will halt and return [io.EOF] to avoid older commits that
// can't be mapped to the current schema.
func (dpi *diffPartitionIter) Next(ctx *sql.Context) (sql.Partition, error) {
	if dpi.stopNext {
		return nil, io.EOF
	}

	for {
		if len(dpi.buf) > 0 {
			next := dpi.buf[0]
			dpi.buf = dpi.buf[1:]

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
				// TODO(#11140): Halting drops unrelated partitions in sibling branches.
				dpi.stopNext = true
			}

			return next, nil
		}

		if dpi.cmItr == nil {
			return nil, io.EOF
		}

		cmHash, optCmt, _, height, err := dpi.cmItr.Next(ctx)
		if err != nil {
			return nil, err
		}
		if dpi.minHeight > 0 && height <= dpi.minHeight {
			return nil, io.EOF
		}
		cm, ok := optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitRuntimeFailure
		}

		if err := dpi.queueForCommit(ctx, cmHash, cm); err != nil {
			return nil, err
		}
	}
}

// queueForCommit pairs |cm| with its parent(s) and queues a DiffPartition
// whenever the table's data or schema differ.
func (dpi *diffPartitionIter) queueForCommit(ctx *sql.Context, cmHash hash.Hash, cm *doltdb.Commit) error {
	phs, err := cm.ParentHashes(ctx)
	if err != nil {
		return err
	}

	var toInfo TblInfoAtCommit
	for i, ph := range phs {
		if dpi.parentHashFilter != nil && !dpi.parentHashFilter.Has(ph) {
			continue
		}
		// If filter set, avoid initializing toInfo until target parent.
		if toInfo.IsEmpty() {
			toInfo, err = tableInfoForCommit(ctx, dpi.tblName, cm, cmHash)
			if err != nil {
				return err
			}
		}
		optParent, err := cm.GetParent(ctx, i)
		if err != nil {
			return err
		}
		parentCm, ok := optParent.ToCommit()
		if !ok {
			return doltdb.ErrGhostCommitEncountered
		}
		fromInfo, err := tableInfoForCommit(ctx, dpi.tblName, parentCm, optParent.Addr)
		if err != nil {
			return err
		}
		if err := dpi.queue(ctx, fromInfo, toInfo); err != nil {
			return err
		}
	}
	return nil
}

// queue pairs |fromInfo| and |toInfo| into a DiffPartition and appends it to
// the buffer if the table changed and satisfied the configured partition
// filters.
func (dpi *diffPartitionIter) queue(ctx *sql.Context, fromInfo, toInfo TblInfoAtCommit) error {
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
		fromSch:  dpi.fromSch,
		toSch:    dpi.toSch,
		ranges:   dpi.ranges,
	}
	selected, err := dpi.selectFunc(ctx, partition)
	if err != nil || !selected {
		return err
	}
	dpi.buf = append(dpi.buf, partition)
	return nil
}

func (dpi *diffPartitionIter) Close(*sql.Context) error {
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
