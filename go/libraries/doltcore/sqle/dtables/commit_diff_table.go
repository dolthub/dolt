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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
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
	ss                *schema.SuperSchema
	joiner            *rowconv.Joiner
	sqlSch            sql.PrimaryKeySchema
	workingRoot       *doltdb.RootValue
	fromCommitFilter  *expression.Equals
	toCommitFilter    *expression.Equals
	requiredFilterErr error
}

func NewCommitDiffTable(ctx *sql.Context, tblName string, ddb *doltdb.DoltDB, root *doltdb.RootValue) (sql.Table, error) {
	tblName, ok, err := root.ResolveTableName(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(doltdb.DoltCommitDiffTablePrefix + tblName)
	}

	diffTblName := doltdb.DoltCommitDiffTablePrefix + tblName
	ss, err := calcSuperDuperSchema(ctx, ddb, root, tblName)
	if err != nil {
		return nil, err
	}

	_ = ss.AddColumn(schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false))
	_ = ss.AddColumn(schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))

	sch, err := ss.GenerateSchema()
	if err != nil {
		return nil, err
	}

	if sch.GetAllCols().Size() <= 1 {
		return nil, sql.ErrTableNotFound.New(diffTblName)
	}

	j, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{{Name: diff.To, Sch: sch}, {Name: diff.From, Sch: sch}},
		map[string]rowconv.ColNamingFunc{
			diff.To:   toNamer,
			diff.From: fromNamer,
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

	return &CommitDiffTable{
		name:        tblName,
		ddb:         ddb,
		workingRoot: root,
		ss:          ss,
		joiner:      j,
		sqlSch:      sqlSch,
	}, nil
}

func calcSuperDuperSchema(ctx context.Context, ddb *doltdb.DoltDB, working *doltdb.RootValue, tblName string) (*schema.SuperSchema, error) {
	refs, err := ddb.GetBranches(ctx)

	if err != nil {
		return nil, err
	}

	var superSchemas []*schema.SuperSchema
	ss, found, err := working.GetSuperSchema(ctx, tblName)

	if err != nil {
		return nil, err
	}

	if found {
		superSchemas = append(superSchemas, ss)
	}

	for _, ref := range refs {
		cm, err := ddb.ResolveCommitRef(ctx, ref)

		if err != nil {
			return nil, err
		}

		cmRoot, err := cm.GetRootValue()

		if err != nil {
			return nil, err
		}

		ss, found, err = cmRoot.GetSuperSchema(ctx, tblName)

		if err != nil {
			return nil, err
		}

		if found {
			superSchemas = append(superSchemas, ss)
		}
	}

	if len(superSchemas) == 0 {
		return nil, sql.ErrTableNotFound.New(tblName)
	}

	superDuperSchema, err := schema.SuperSchemaUnion(superSchemas...)

	if err != nil {
		return nil, err
	}

	return superDuperSchema, nil
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

	toRoot, toName, toDate, err := dt.rootValForFilter(ctx, dt.toCommitFilter)

	if err != nil {
		return nil, err
	}

	fromRoot, fromName, fromDate, err := dt.rootValForFilter(ctx, dt.fromCommitFilter)

	if err != nil {
		return nil, err
	}

	toTable, _, err := toRoot.GetTable(ctx, dt.name)

	if err != nil {
		return nil, err
	}

	fromTable, _, err := fromRoot.GetTable(ctx, dt.name)

	dp := diffPartition{
		to:       toTable,
		from:     fromTable,
		toName:   toName,
		fromName: fromName,
		toDate:   toDate,
		fromDate: fromDate,
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

		root, err = cm.GetRootValue()

		if err != nil {
			return nil, "", nil, err
		}

		meta, err := cm.GetCommitMeta()

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
func (dt *CommitDiffTable) WithFilters(ctx *sql.Context, filters []sql.Expression) sql.Table {
	return dt
}

func (dt *CommitDiffTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	dp := part.(diffPartition)
	// TODO: commit_diff_table reuses diffPartition from diff_table and we've switched diff_table over
	//       to a new format. After we switch commit_diff_table over to the same new format, we can
	//       remove this getLegacyRowIter method.
	return dp.getLegacyRowIter(ctx, dt.ddb, dt.ss, dt.joiner)
}
