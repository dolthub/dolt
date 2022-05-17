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
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrExactlyOneToCommit = errors.New("dolt_commit_diff_* tables must be filtered to a single 'to_commit'")
var ErrExactlyOneFromCommit = errors.New("dolt_commit_diff_* tables must be filtered to a single 'from_commit'")

var _ sql.Table = (*CommitDiffTable)(nil)
var _ sql.FilteredTable = (*CommitDiffTable)(nil)

type CommitDiffTable struct {
	name              string
	ddb               *doltdb.DoltDB
	joiner            *rowconv.Joiner
	sqlSch            sql.PrimaryKeySchema
	workingRoot       *doltdb.RootValue
	fromCommitFilter  *expression.Equals
	toCommitFilter    *expression.Equals
	requiredFilterErr error
	targetSchema      *schema.Schema
}

func NewCommitDiffTable(ctx *sql.Context, tblName string, ddb *doltdb.DoltDB, root *doltdb.RootValue) (sql.Table, error) {
	diffTblName := doltdb.DoltCommitDiffTablePrefix + tblName

	table, _, ok, err := root.GetTableInsensitive(ctx, tblName)
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

	return &CommitDiffTable{
		name:         tblName,
		ddb:          ddb,
		workingRoot:  root,
		joiner:       j,
		sqlSch:       sqlSch,
		targetSchema: &sch,
	}, nil
}

func (dt *CommitDiffTable) Name() string {
	return doltdb.DoltCommitDiffTablePrefix + dt.name
}

func (dt *CommitDiffTable) String() string {
	return doltdb.DoltCommitDiffTablePrefix + dt.name
}

func (dt *CommitDiffTable) Schema() sql.Schema {
	return dt.sqlSch.Schema
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

func (dt *CommitDiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	if dt.requiredFilterErr != nil {
		return nil, fmt.Errorf("error querying table %s: %w", dt.Name(), dt.requiredFilterErr)
	} else if dt.toCommitFilter == nil {
		return nil, fmt.Errorf("error querying table %s: %w", dt.Name(), ErrExactlyOneToCommit)
	} else if dt.fromCommitFilter == nil {
		return nil, fmt.Errorf("error querying table %s: %w", dt.Name(), ErrExactlyOneFromCommit)
	}

	toRoot, toHash, toDate, err := dt.rootValForFilter(ctx, dt.toCommitFilter)
	if err != nil {
		return nil, err
	}

	fromRoot, fromHash, fromDate, err := dt.rootValForFilter(ctx, dt.fromCommitFilter)
	if err != nil {
		return nil, err
	}

	toTable, _, _, err := toRoot.GetTableInsensitive(ctx, dt.name)
	if err != nil {
		return nil, err
	}

	fromTable, _, _, err := fromRoot.GetTableInsensitive(ctx, dt.name)
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

func (dt *CommitDiffTable) rootValForFilter(ctx *sql.Context, eqFilter *expression.Equals) (*doltdb.RootValue, string, *types.Timestamp, error) {
	gf, nonGF := eqFilter.Left(), eqFilter.Right()
	if _, ok := gf.(*expression.GetField); !ok {
		nonGF, gf = eqFilter.Left(), eqFilter.Right()
	}

	val, err := nonGF.Eval(ctx, nil)

	if err != nil {
		return nil, "", nil, err
	}

	hashStr, ok := val.(string)

	if !ok {
		return nil, "", nil, fmt.Errorf("received '%v' when expecting commit hash string", val)
	}

	var root *doltdb.RootValue
	var commitTime *types.Timestamp
	if strings.ToLower(hashStr) == "working" {
		root = dt.workingRoot
	} else {
		cs, err := doltdb.NewCommitSpec(hashStr)

		if err != nil {
			return nil, "", nil, err
		}

		cm, err := dt.ddb.Resolve(ctx, cs, nil)

		if err != nil {
			return nil, "", nil, err
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

// HandledFilters returns the list of filters that will be handled by the table itself
func (dt *CommitDiffTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	var commitFilters []sql.Expression
	for _, filter := range filters {
		isCommitFilter := false

		if eqFilter, isEquality := filter.(*expression.Equals); isEquality {
			for _, e := range []sql.Expression{eqFilter.Left(), eqFilter.Right()} {
				if val, ok := e.(*expression.GetField); ok {
					switch strings.ToLower(val.Name()) {
					case toCommit:
						if dt.toCommitFilter != nil {
							dt.requiredFilterErr = ErrExactlyOneToCommit
						}

						isCommitFilter = true
						dt.toCommitFilter = eqFilter
					case fromCommit:
						if dt.fromCommitFilter != nil {
							dt.requiredFilterErr = ErrExactlyOneFromCommit
						}

						isCommitFilter = true
						dt.fromCommitFilter = eqFilter
					}
				}
			}
		}

		if isCommitFilter {
			commitFilters = append(commitFilters, filter)
		}
	}

	return commitFilters
}

// Filters returns the list of filters that are applied to this table.
func (dt *CommitDiffTable) Filters() []sql.Expression {
	if dt.toCommitFilter == nil || dt.fromCommitFilter == nil {
		return nil
	}

	return []sql.Expression{dt.toCommitFilter, dt.fromCommitFilter}
}

// WithFilters returns a new sql.Table instance with the filters applied
func (dt *CommitDiffTable) WithFilters(_ *sql.Context, _ []sql.Expression) sql.Table {
	return dt
}

func (dt *CommitDiffTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	dp := part.(DiffPartition)
	return dp.GetRowIter(ctx, dt.ddb, dt.joiner)
}
