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
	tableName   doltdb.TableName
	ddb         *doltdb.DoltDB
	workingRoot doltdb.RootValue
	head        *doltdb.Commit

	headHash          hash.Hash
	headCommitClosure *prolly.CommitClosure

	// from and to need to be mapped to this schema
	targetSch schema.Schema

	// the schema for the diff table itself. Once from and to are converted to
	// targetSch, the commit names and dates are inserted.
	diffTableSch schema.Schema

	sqlSch           sql.PrimaryKeySchema
	partitionFilters []sql.Expression

	table  *doltdb.Table
	lookup sql.IndexLookup

	// noms only
	joiner *rowconv.Joiner
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

	diffTableSchema, j, err := GetDiffTableSchemaAndJoiner(ddb.Format(), sch, sch)
	if err != nil {
		return nil, err
	}

	sqlSch, err := sqlutil.FromDoltSchema(dbName, diffTblName, diffTableSchema)
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
		joiner:           j,
	}, nil
}

func (dt *DiffTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dt.Schema())
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

func (dt *DiffTable) Schema() sql.Schema {
	return dt.sqlSch.Schema
}

func (dt *DiffTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (dt *DiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	cmItr := doltdb.CommitItrForRoots(dt.ddb, dt.head)

	sf, err := SelectFuncForFilters(dt.ddb.ValueReadWriter(), dt.partitionFilters)
	if err != nil {
		return nil, err
	}

	table, exists, err := dt.workingRoot.GetTable(ctx, dt.tableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("table: %s does not exist", dt.tableName.String())
	}

	wrTblHash, _, err := dt.workingRoot.GetTableHash(ctx, dt.tableName)
	if err != nil {
		return nil, err
	}

	cmHash, _, err := cmItr.Next(ctx)
	if err != nil {
		return nil, err
	}

	cmHashToTblInfo := make(map[hash.Hash]TblInfoAtCommit)
	cmHashToTblInfo[cmHash] = TblInfoAtCommit{"WORKING", nil, table, wrTblHash}

	err = cmItr.Reset(ctx)
	if err != nil {
		return nil, err
	}

	return &DiffPartitions{
		tblName:         dt.tableName,
		cmItr:           cmItr,
		cmHashToTblInfo: cmHashToTblInfo,
		selectFunc:      sf,
		toSch:           dt.targetSch,
		fromSch:         dt.targetSch,
	}, nil
}

var commitMetaColumns = set.NewStrSet([]string{toCommit, fromCommit, toCommitDate, fromCommitDate})

// CommitIsInScope returns true if a given commit hash is head or is
// visible from the current head's ancestry graph.
func (dt *DiffTable) CommitIsInScope(ctx context.Context, height uint64, h hash.Hash) (bool, error) {
	cc, err := dt.HeadCommitClosure(ctx)
	if err != nil {
		return false, err
	}
	headHash, err := dt.HeadHash()
	if err != nil {
		return false, err
	}
	if headHash == h {
		return true, nil
	}
	return cc.ContainsKey(ctx, h, height)
}

func (dt *DiffTable) HeadCommitClosure(ctx context.Context) (*prolly.CommitClosure, error) {
	if dt.headCommitClosure == nil {
		cc, err := dt.head.GetCommitClosure(ctx)
		dt.headCommitClosure = &cc
		if err != nil {
			return nil, err
		}
	}
	return dt.headCommitClosure, nil
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
	return dp.GetRowIter(ctx, dt.ddb, dt.joiner, dt.lookup)
}

func (dt *DiffTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	switch lookup.Index.ID() {
	case index.ToCommitIndexId:
		hs, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(lookup.Ranges))
		}
		hashes, commits, _ := index.HashesToCommits(ctx, dt.ddb, hs, dt.head, false)
		if len(hashes) == 0 {
			return sql.PartitionsToPartitionIter(), nil
		}
		return dt.toCommitLookupPartitions(ctx, hashes, commits)
	case index.FromCommitIndexId:
		hs, ok := index.LookupToPointSelectStr(lookup)
		if !ok {
			return nil, fmt.Errorf("failed to parse commit lookup ranges: %s", sql.DebugString(lookup.Ranges))
		}
		hashes, commits, _ := index.HashesToCommits(ctx, dt.ddb, hs, nil, false)
		if len(hashes) == 0 {
			return sql.PartitionsToPartitionIter(), nil
		}
		return dt.fromCommitLookupPartitions(ctx, hashes, commits)
	default:
		return dt.Partitions(ctx)
	}
}

// fromCommitLookupPartitions creates a diff partition iterator for a set
// of commits. The structure of the iter requires we pre-populate the
// children of from_commit for diffing. We walk the commit graph looking
// for commits that reference |from_commit| as a parent, and forward populate
// for the |from_commit| diff partitions we will iterate.
// TODO the structure of the diff iterator doesn't appear to accommodate
// several children for a parent hash.
func (dt *DiffTable) fromCommitLookupPartitions(ctx *sql.Context, hashes []hash.Hash, commits []*doltdb.Commit) (sql.PartitionIter, error) {
	tbl, ok, err := dt.workingRoot.GetTable(ctx, dt.tableName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("table: %s does not exist", dt.tableName.String())
	}

	var parentHashes []hash.Hash
	cmHashToTblInfo := make(map[hash.Hash]TblInfoAtCommit)
	var pCommits []*doltdb.Commit
	for i, hs := range hashes {
		headHash, err := dt.head.HashOf()
		if err != nil {
			return nil, err
		}
		if hs.Equal(headHash) {
			// If from_commit specifies the 'HEAD' commit, we need to include
			// the working root in the parent commits because 'WORKING' is one step
			// above 'HEAD'.
			wrTblHash, _, err := dt.workingRoot.GetTableHash(ctx, dt.tableName)
			if err != nil {
				return nil, err
			}
			toCmInfo := TblInfoAtCommit{"WORKING", nil, tbl, wrTblHash}
			cmHashToTblInfo[hs] = toCmInfo
			parentHashes = append(parentHashes, hs)
			pCommits = append(pCommits, dt.head)
			continue
		}

		cm := commits[i]

		// scope check
		height, err := cm.Height()
		if err != nil {
			return nil, err
		}

		childCm, childHs, err := dt.scanHeightForChild(ctx, hs, height+1)
		if err != nil {
			return nil, err
		}
		if childCm == nil {
			// non-linear commit graph, fallback to top-down scan
			childCm, childHs, err = dt.reverseIterForChild(ctx, hs)
			if err != nil {
				return nil, err
			}
		}

		if childCm != nil {
			ti, err := tableInfoForCommit(ctx, dt.tableName, childCm, childHs)
			if err != nil {
				return nil, err
			}
			cmHashToTblInfo[hs] = ti
			parentHashes = append(parentHashes, hs)
			pCommits = append(pCommits, cm)
		}
	}

	if len(parentHashes) == 0 {
		return sql.PartitionsToPartitionIter(), nil
	}

	sf, err := SelectFuncForFilters(dt.ddb.ValueReadWriter(), dt.partitionFilters)
	if err != nil {
		return nil, err
	}

	cmItr := doltdb.NewCommitSliceIter(pCommits, parentHashes)

	return &DiffPartitions{
		tblName:         dt.tableName,
		cmItr:           cmItr,
		cmHashToTblInfo: cmHashToTblInfo,
		selectFunc:      sf,
		toSch:           dt.targetSch,
		fromSch:         dt.targetSch,
	}, nil
}

// scanHeightForChild searches for a child commit that references a target parent hash
// at a specific height. This is an optimization for the common case where a parent and
// its child are one level apart, and there is no branching that creates the potential
// for a child higher in the graph.
func (dt *DiffTable) scanHeightForChild(ctx *sql.Context, parent hash.Hash, height uint64) (*doltdb.Commit, hash.Hash, error) {
	cc, err := dt.HeadCommitClosure(ctx)
	if err != nil {
		return nil, hash.Hash{}, err
	}
	iter, err := cc.IterHeight(ctx, height)
	if err != nil {
		return nil, hash.Hash{}, err
	}
	var childHs hash.Hash
	var childCm *doltdb.Commit
	var cnt int
	for {
		k, _, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, hash.Hash{}, err
		}
		cnt++
		if cnt > 1 {
			return nil, hash.Hash{}, nil
		}

		c, err := doltdb.HashToCommit(ctx, dt.ddb.ValueReadWriter(), dt.ddb.NodeStore(), k.Addr())
		phs, err := c.ParentHashes(ctx)
		if err != nil {
			return nil, hash.Hash{}, err
		}
		for _, ph := range phs {
			if ph == parent {
				childCm = c
				childHs = k.Addr()
				break
			}
		}
	}
	return childCm, childHs, nil
}

// reverseIterForChild finds the commit with the largest height that
// is a child of the |parent| hash, or nil if no commit is found.
func (dt *DiffTable) reverseIterForChild(ctx *sql.Context, parent hash.Hash) (*doltdb.Commit, hash.Hash, error) {
	iter := doltdb.CommitItrForRoots(dt.ddb, dt.head)
	for {
		childHs, optCmt, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil, hash.Hash{}, nil
		} else if err != nil {
			return nil, hash.Hash{}, err
		}

		childCm, ok := optCmt.ToCommit()
		if !ok {
			// Should have been caught above from the Next() call on the iter. This is a runtime error.
			return nil, hash.Hash{}, doltdb.ErrGhostCommitRuntimeFailure
		}

		phs, err := childCm.ParentHashes(ctx)
		if err != nil {
			return nil, hash.Hash{}, err
		}
		for _, ph := range phs {
			if ph == parent {
				return childCm, childHs, nil
			}
		}
	}
}

func tableInfoForCommit(ctx *sql.Context, tableName doltdb.TableName, cm *doltdb.Commit, hs hash.Hash) (TblInfoAtCommit, error) {
	r, err := cm.GetRootValue(ctx)
	if err != nil {
		return TblInfoAtCommit{}, err
	}

	tbl, exists, err := r.GetTable(ctx, tableName)
	if err != nil {
		return TblInfoAtCommit{}, err
	}
	if !exists {
		return TblInfoAtCommit{}, nil
	}

	tblHash, _, err := r.GetTableHash(ctx, tableName)
	if err != nil {
		return TblInfoAtCommit{}, err
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return TblInfoAtCommit{}, err
	}

	ts := types.Timestamp(meta.Time())
	return NewTblInfoAtCommit(hs.String(), &ts, tbl, tblHash), nil
}

// toCommitLookupPartitions creates a diff partition iterator for a set of
// commits. The structure of the iter requires we pre-populate the parents
// of to_commit for diffing.
func (dt *DiffTable) toCommitLookupPartitions(ctx *sql.Context, hashes []hash.Hash, commits []*doltdb.Commit) (sql.PartitionIter, error) {
	t, ok, err := dt.workingRoot.GetTable(ctx, dt.tableName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("table: %s does not exist", dt.tableName.String())
	}

	working, err := dt.head.HashOf()
	if err != nil {
		return nil, err
	}

	var parentHashes []hash.Hash
	cmHashToTblInfo := make(map[hash.Hash]TblInfoAtCommit)
	var pCommits []*doltdb.Commit
	for i, hs := range hashes {
		cm := commits[i]

		var toCmInfo TblInfoAtCommit
		if hs == working && cm == nil {
			wrTblHash, _, err := dt.workingRoot.GetTableHash(ctx, dt.tableName)
			if err != nil {
				return nil, err
			}

			toCmInfo = TblInfoAtCommit{"WORKING", nil, t, wrTblHash}
			cmHashToTblInfo[hs] = toCmInfo
			parentHashes = append(parentHashes, hs)
			pCommits = append(pCommits, dt.head)
			continue
		}

		// scope check
		height, err := cm.Height()
		if err != nil {
			return nil, err
		}
		ok, err = dt.CommitIsInScope(ctx, height, hs)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		ti, err := tableInfoForCommit(ctx, dt.tableName, cm, hs)
		if err != nil {
			return nil, err
		}
		if ti.IsEmpty() {
			continue
		}

		ph, err := cm.ParentHashes(ctx)
		if err != nil {
			return nil, err
		}

		for i, pj := range ph {
			optCmt, err := cm.GetParent(ctx, i)
			if err != nil {
				return nil, err
			}
			pc, ok := optCmt.ToCommit()
			if !ok {
				return nil, doltdb.ErrGhostCommitEncountered
			}

			cmHashToTblInfo[pj] = toCmInfo
			cmHashToTblInfo[pj] = ti
			pCommits = append(pCommits, pc)
		}
		parentHashes = append(parentHashes, ph...)
	}

	if len(parentHashes) == 0 {
		return sql.PartitionsToPartitionIter(), nil
	}

	sf, err := SelectFuncForFilters(dt.ddb.ValueReadWriter(), dt.partitionFilters)
	if err != nil {
		return nil, err
	}

	cmItr := doltdb.NewCommitSliceIter(pCommits, parentHashes)

	return &DiffPartitions{
		tblName:         dt.tableName,
		cmItr:           cmItr,
		cmHashToTblInfo: cmHashToTblInfo,
		selectFunc:      sf,
		toSch:           dt.targetSch,
		fromSch:         dt.targetSch,
	}, nil
}

// GetIndexes implements sql.IndexAddressable
func (dt *DiffTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return index.DoltDiffIndexesFromTable(ctx, "", dt.tableName.Name, dt.table)
}

// IndexedAccess implements sql.IndexAddressable
func (dt *DiffTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	nt := *dt
	return &nt
}

// PreciseMatch implements sql.IndexAddressable
func (dt *DiffTable) PreciseMatch() bool {
	return false
}

// tableData returns the map of primary key to values for the specified table (or an empty map if the tbl is null)
// and the schema of the table (or EmptySchema if tbl is null).
func tableData(ctx *sql.Context, tbl *doltdb.Table, ddb *doltdb.DoltDB) (durable.Index, schema.Schema, error) {
	var data durable.Index
	var err error

	if tbl == nil {
		data, err = durable.NewEmptyPrimaryIndex(ctx, ddb.ValueReadWriter(), ddb.NodeStore(), schema.EmptySchema)
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
}

func NewDiffPartition(to, from *doltdb.Table, toName, fromName string, toDate, fromDate *types.Timestamp, toSch, fromSch schema.Schema) *DiffPartition {
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
	// TODO: schema name
	return []byte(dp.toName + dp.fromName)
}

func (dp DiffPartition) GetRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, joiner *rowconv.Joiner, lookup sql.IndexLookup) (sql.RowIter, error) {
	if types.IsFormat_DOLT(ddb.Format()) {
		return newProllyDiffIter(ctx, dp, dp.fromSch, dp.toSch)
	} else {
		return newLdDiffIter(ctx, ddb, joiner, dp, lookup)
	}
}

// isDiffablePartition checks if the commit pair for this partition is "diffable".
// If the primary key sets changed between the two commits, it may not be
// possible to diff them. We return two bools: simpleDiff is returned if the primary key sets are close enough that we
// can confidently merge the diff (using schema.ArePrimaryKeySetsDiffable). fuzzyDiff is returned if the primary key
// sets are not close enough to merge the diff, but we can still make an approximate comparison (using schema.MapSchemaBasedOnTagAndName).
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

	easyDiff := schema.ArePrimaryKeySetsDiffable(dp.from.Format(), fromSch, toSch)
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

func SelectFuncForFilters(vr types.ValueReader, filters []sql.Expression) (partitionSelectFunc, error) {
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

	expFunc, err := expreval.ExpressionFuncFromSQLExpressions(vr, schema.UnkeyedSchemaFromCols(colColl), filters)

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
	tblName         doltdb.TableName
	cmItr           doltdb.CommitItr
	cmHashToTblInfo map[hash.Hash]TblInfoAtCommit
	selectFunc      partitionSelectFunc
	toSch           schema.Schema
	fromSch         schema.Schema
	stopNext        bool
}

// processCommit is called in a commit iteration loop. Adds partitions when it finds a commit and its parent that have
// different values for the hash of the table being looked at.
func (dps *DiffPartitions) processCommit(ctx *sql.Context, cmHash hash.Hash, cm *doltdb.Commit, root doltdb.RootValue, tbl *doltdb.Table) (*DiffPartition, error) {
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
			fromSch:  dps.fromSch,
			toSch:    dps.toSch,
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
	if dps.stopNext {
		return nil, io.EOF
	}

	for {
		cmHash, optCmt, err := dps.cmItr.Next(ctx)
		if err != nil {
			return nil, err
		}
		cm, ok := optCmt.ToCommit()
		if !ok {
			// Should have been caught above from the Next() call on the iter. This is a runtime error.
			return nil, doltdb.ErrGhostCommitRuntimeFailure
		}

		root, err := cm.GetRootValue(ctx)

		if err != nil {
			return nil, err
		}

		tbl, _, _, err := doltdb.GetTableInsensitive(ctx, root, dps.tblName)

		if err != nil {
			return nil, err
		}

		next, err := dps.processCommit(ctx, cmHash, cm, root, tbl)

		if err != nil {
			return nil, err
		}

		if next != nil {
			// If we can't diff this commit with its parent, don't traverse any lower
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

	fm, err := rowconv.TagMappingByTagAndName(srcSch, targetSch)
	if err != nil {
		return nil, err
	}

	return rowconv.NewRowConverter(ctx, vrw, fm)
}

// GetDiffTableSchemaAndJoiner returns the schema for the diff table given a
// target schema for a row |sch|. In the old storage format, it also returns the
// associated joiner.
func GetDiffTableSchemaAndJoiner(format *types.NomsBinFormat, fromSch, toSch schema.Schema) (diffTableSchema schema.Schema, j *rowconv.Joiner, err error) {
	if format == types.Format_DOLT {
		diffTableSchema, err = CalculateDiffSchema(fromSch, toSch)
		if err != nil {
			return nil, nil, err
		}
	} else {
		fromSch, toSch, err = expandFromToSchemas(fromSch, toSch)
		if err != nil {
			return nil, nil, err
		}

		j, err = rowconv.NewJoiner(
			[]rowconv.NamedSchema{{Name: diff.To, Sch: toSch}, {Name: diff.From, Sch: fromSch}},
			map[string]rowconv.ColNamingFunc{
				diff.To:   diff.ToColNamer,
				diff.From: diff.FromColNamer,
			})
		if err != nil {
			return nil, nil, err
		}

		diffTableSchema = j.GetSchema()
		fullDiffCols := diffTableSchema.GetAllCols()
		fullDiffCols = fullDiffCols.Append(
			schema.NewColumn(diffTypeColName, schema.DiffTypeTag, types.StringKind, false),
		)
		diffTableSchema = schema.MustSchemaFromCols(fullDiffCols)
	}

	return
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
