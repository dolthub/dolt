// Copyright 2020 Dolthub, Inc.
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
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

const commitDiffDefaultRowCount = 1000

var ErrExactlyOneToCommit = errors.New("dolt_commit_diff_* tables must be filtered to a single 'to_commit'")
var ErrExactlyOneFromCommit = errors.New("dolt_commit_diff_* tables must be filtered to a single 'from_commit'")
var ErrInvalidCommitDiffTableArgs = errors.New("commit_diff_<table> requires one 'to_commit' and one 'from_commit'")

type CommitDiffTable struct {
	tableName   doltdb.TableName
	dbName      string
	ddb         *doltdb.DoltDB
	joiner      *rowconv.Joiner
	sqlSch      sql.PrimaryKeySchema
	workingRoot doltdb.RootValue
	stagedRoot  doltdb.RootValue
	// toCommit and fromCommit are set via the
	// sql.IndexAddressable interface
	toCommit          string
	fromCommit        string
	requiredFilterErr error
	targetSchema      schema.Schema
}

var _ sql.Table = (*CommitDiffTable)(nil)
var _ sql.IndexAddressable = (*CommitDiffTable)(nil)
var _ sql.StatisticsTable = (*CommitDiffTable)(nil)

func NewCommitDiffTable(ctx *sql.Context, dbName string, tblName doltdb.TableName, ddb *doltdb.DoltDB, wRoot, sRoot doltdb.RootValue) (sql.Table, error) {
	diffTblName := doltdb.DoltCommitDiffTablePrefix + tblName.Name

	var table *doltdb.Table
	var err error
	table, tblName, err = mustGetTableInsensitive(ctx, wRoot, tblName)
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

	return &CommitDiffTable{
		dbName:       dbName,
		tableName:    tblName,
		ddb:          ddb,
		workingRoot:  wRoot,
		stagedRoot:   sRoot,
		joiner:       j,
		sqlSch:       sqlSch,
		targetSchema: sch,
	}, nil
}

func (dt *CommitDiffTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dt.Schema())
	numRows, _, err := dt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (dt *CommitDiffTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return commitDiffDefaultRowCount, false, nil
}

func (dt *CommitDiffTable) Name() string {
	return doltdb.DoltCommitDiffTablePrefix + dt.tableName.Name
}

func (dt *CommitDiffTable) String() string {
	return doltdb.DoltCommitDiffTablePrefix + dt.tableName.Name
}

func (dt *CommitDiffTable) Schema() sql.Schema {
	return dt.sqlSch.Schema
}

// Collation implements the sql.Table interface.
func (dt *CommitDiffTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// GetIndexes implements sql.IndexAddressable
func (dt *CommitDiffTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return []sql.Index{index.DoltToFromCommitIndex(dt.tableName.Name)}, nil
}

// IndexedAccess implements sql.IndexAddressable
func (dt *CommitDiffTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	nt := *dt
	return &nt
}

func (dt *CommitDiffTable) PreciseMatch() bool {
	return false
}

// RequiredPredicates implements sql.IndexRequired
func (dt *CommitDiffTable) RequiredPredicates() []string {
	return []string{"to_commit", "from_commit"}
}

func (dt *CommitDiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return nil, fmt.Errorf("error querying table %s: %w", dt.Name(), ErrExactlyOneToCommit)
}

func (dt *CommitDiffTable) LookupPartitions(ctx *sql.Context, i sql.IndexLookup) (sql.PartitionIter, error) {
	ranges, ok := i.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		return nil, fmt.Errorf("commit diff table requires MySQL ranges")
	}
	if len(ranges) != 1 || len(ranges[0]) != 2 {
		return nil, ErrInvalidCommitDiffTableArgs
	}
	to := ranges[0][0]
	from := ranges[0][1]
	switch to.UpperBound.(type) {
	case sql.Above, sql.Below:
	default:
		return nil, ErrInvalidCommitDiffTableArgs
	}
	switch from.UpperBound.(type) {
	case sql.Above, sql.Below:
	default:
		return nil, ErrInvalidCommitDiffTableArgs
	}
	toCommit, _, err := to.Typ.Convert(sql.GetMySQLRangeCutKey(to.UpperBound))
	if err != nil {
		return nil, err
	}
	dt.toCommit, ok = toCommit.(string)
	if !ok {
		return nil, fmt.Errorf("to_commit must be string, found %T", toCommit)
	}
	fromCommit, _, err := from.Typ.Convert(sql.GetMySQLRangeCutKey(from.UpperBound))
	if err != nil {
		return nil, err
	}
	dt.fromCommit, ok = fromCommit.(string)
	if !ok {
		return nil, fmt.Errorf("from_commit must be string, found %T", fromCommit)
	}

	toRoot, toHash, toDate, err := dt.rootValForHash(ctx, dt.toCommit)
	if err != nil {
		return nil, err
	}

	fromRoot, fromHash, fromDate, err := dt.rootValForHash(ctx, dt.fromCommit)
	if err != nil {
		return nil, err
	}

	toTable, _, _, err := doltdb.GetTableInsensitive(ctx, toRoot, dt.tableName)
	if err != nil {
		return nil, err
	}

	fromTable, _, _, err := doltdb.GetTableInsensitive(ctx, fromRoot, dt.tableName)
	if err != nil {
		return nil, err
	}

	dp := DiffPartition{
		to:       toTable,
		from:     fromTable,
		toName:   toHash,
		fromName: fromHash,
		toDate:   toDate,
		fromDate: fromDate,
		toSch:    dt.targetSchema,
		fromSch:  dt.targetSchema,
	}

	isDiffable, err := dp.isDiffablePartition(ctx)
	if err != nil {
		return nil, err
	}

	if !isDiffable {
		ctx.Warn(PrimaryKeyChangeWarningCode, fmt.Sprintf(PrimaryKeyChangeWarning, dp.fromName, dp.toName))
		return NewSliceOfPartitionsItr([]sql.Partition{}), nil
	}

	return NewSliceOfPartitionsItr([]sql.Partition{dp}), nil
}

type SliceOfPartitionsItr struct {
	partitions []sql.Partition
	i          int
	mu         *sync.Mutex
}

func NewSliceOfPartitionsItr(partitions []sql.Partition) *SliceOfPartitionsItr {
	return &SliceOfPartitionsItr{
		partitions: partitions,
		mu:         &sync.Mutex{},
	}
}

func (itr *SliceOfPartitionsItr) Next(*sql.Context) (sql.Partition, error) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if itr.i >= len(itr.partitions) {
		return nil, io.EOF
	}

	next := itr.partitions[itr.i]
	itr.i++

	return next, nil
}

func (itr *SliceOfPartitionsItr) Close(*sql.Context) error {
	return nil
}

func (dt *CommitDiffTable) rootValForHash(ctx *sql.Context, hashStr string) (doltdb.RootValue, string, *types.Timestamp, error) {
	var root doltdb.RootValue
	var commitTime *types.Timestamp
	if strings.EqualFold(hashStr, doltdb.Working) {
		root = dt.workingRoot
	} else if strings.EqualFold(hashStr, doltdb.Staged) {
		root = dt.stagedRoot
	} else {
		cs, err := doltdb.NewCommitSpec(hashStr)
		if err != nil {
			return nil, "", nil, err
		}

		optCmt, err := dt.ddb.Resolve(ctx, cs, nil)
		if err != nil {
			return nil, "", nil, err
		}
		cm, ok := optCmt.ToCommit()
		if !ok {
			return nil, "", nil, doltdb.ErrGhostCommitEncountered
		}

		root, err = cm.GetRootValue(ctx)

		if err != nil {
			return nil, "", nil, err
		}

		meta, err := cm.GetCommitMeta(ctx)

		if err != nil {
			return nil, "", nil, err
		}

		t := meta.Time()
		commitTime = (*types.Timestamp)(&t)
	}

	return root, hashStr, commitTime, nil
}

func (dt *CommitDiffTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	dp := part.(DiffPartition)
	return dp.GetRowIter(ctx, dt.ddb, dt.joiner, sql.IndexLookup{})
}
