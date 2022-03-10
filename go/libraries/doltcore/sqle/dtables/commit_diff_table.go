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
	ctx               *sql.Context
	name              string
	ddb               *doltdb.DoltDB
	ss                *schema.SuperSchema
	joiner            *rowconv.Joiner
	sqlSch            sql.PrimaryKeySchema
	workingRoot       *doltdb.RootValue
	fromCommitFilter  *expression.Equals
	toCommitFilter    *expression.Equals
	requiredFilterErr error
	targetSchema      *schema.Schema
}

func NewCommitDiffTable(ctx *sql.Context, tblName string, ddb *doltdb.DoltDB, root *doltdb.RootValue) (sql.Table, error) {
	// TODO: Does this need to be case insensitive?
	tblName, ok, err := root.ResolveTableName(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(doltdb.DoltCommitDiffTablePrefix + tblName)
	}

	// We need toCommitFilter and fromCommitFilter in order to determine what
	// schema we should return. But... those aren't available until the
	// HandledFilters method is called.
	//
	// Could we return a "close" schema initially until the filters are set,
	// and then return a different schema?
	// But then the expected schema won't match with the returned columns and
	// we'll have problems.

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
			diff.To:   diff.ToNamer,
			diff.From: diff.FromNamer,
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
		ctx:          ctx,
		name:         tblName,
		ddb:          ddb,
		workingRoot:  root,
		ss:           ss,
		joiner:       j,
		sqlSch:       sqlSch,
		targetSchema: &sch,
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

// TODO: Why does this need to use a mutex? Where is the concurrent access coming from?
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

	// TODO: Copy and pasted into earlier codepath
	toRoot, toName, toDate, err := rootValForFilter(ctx, dt.toCommitFilter, dt.workingRoot, dt.ddb)
	if err != nil {
		return nil, err
	}

	fromRoot, fromName, fromDate, err := rootValForFilter(ctx, dt.fromCommitFilter, dt.workingRoot, dt.ddb)
	if err != nil {
		return nil, err
	}

	toTable, _, err := toRoot.GetTable(ctx, dt.name)
	if err != nil {
		return nil, err
	}

	fromTable, _, err := fromRoot.GetTable(ctx, dt.name)
	if err != nil {
		return nil, err
	}

	dp := DiffPartition{
		to:       toTable,
		from:     fromTable,
		toName:   toName,
		fromName: fromName,
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

func rootValForFilter(ctx *sql.Context, eqFilter *expression.Equals, workingRoot *doltdb.RootValue, ddb *doltdb.DoltDB) (*doltdb.RootValue, string, *types.Timestamp, error) {
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
		root = workingRoot
	} else {
		cs, err := doltdb.NewCommitSpec(hashStr)

		if err != nil {
			return nil, "", nil, err
		}

		cm, err := ddb.Resolve(ctx, cs, nil)

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

	err := dt.updateSchemaFromFilters()
	if err != nil {
		panic(err)
	}

	return commitFilters
}

func (dt *CommitDiffTable) updateSchemaFromFilters() error {
	toRoot, _, _, err := rootValForFilter(dt.ctx, dt.toCommitFilter, dt.workingRoot, dt.ddb)
	if err != nil {
		return err
	}

	fromRoot, _, _, err := rootValForFilter(dt.ctx, dt.fromCommitFilter, dt.workingRoot, dt.ddb)
	if err != nil {
		return err
	}

	toTable, _, err := toRoot.GetTable(dt.ctx, dt.name)
	if err != nil {
		return err
	}

	fromTable, _, err := fromRoot.GetTable(dt.ctx, dt.name)
	if err != nil {
		return err
	}

	var fromSchema, toSchema schema.Schema

	if fromTable != nil && toTable != nil {
		fromSchema, err = fromTable.GetSchema(dt.ctx)
		if err != nil {
			return err
		}

		toSchema, err = toTable.GetSchema(dt.ctx)
		if err != nil {
			return err
		}
	} else if toTable != nil {
		toSchema, err = toTable.GetSchema(dt.ctx)
		if err != nil {
			return err
		}
		// TODO: Do we still need this copy?
		fromSchema = schema.MustSchemaFromCols(toSchema.GetAllCols())
	} else if fromTable != nil {
		fromSchema, err = fromTable.GetSchema(dt.ctx)
		if err != nil {
			return err
		}
		// TODO: Do we still need this copy?
		toSchema = schema.MustSchemaFromCols(fromSchema.GetAllCols())
	} else {
		panic("No from or to tables valid")
	}

	// Now that we have the filters set... we can access the to/from tables and
	// set the exact schema we want...

	toColCollection := toSchema.GetAllCols()
	toColCollection = toColCollection.Append(
		schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
		schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
	toSchema = schema.MustSchemaFromCols(toColCollection)

	fromColCollection := fromSchema.GetAllCols()
	fromColCollection = fromColCollection.Append(
		schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
		schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
	fromSchema = schema.MustSchemaFromCols(fromColCollection)

	j, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{{Name: diff.To, Sch: toSchema}, {Name: diff.From, Sch: fromSchema}},
		map[string]rowconv.ColNamingFunc{
			diff.To:   diff.ToNamer,
			diff.From: diff.FromNamer,
		})
	if err != nil {
		return err
	}

	diffTblName := doltdb.DoltCommitDiffTablePrefix + dt.name

	sqlSch, err := sqlutil.FromDoltSchema(diffTblName, j.GetSchema())
	if err != nil {
		return err
	}

	// TDOO: Copied from
	sqlSch.Schema = append(sqlSch.Schema, &sql.Column{
		Name:     diffTypeColName,
		Type:     sql.Text,
		Nullable: false,
		Source:   diffTblName,
	})

	dt.joiner = j
	dt.sqlSch = sqlSch

	// TODO: Do we need to set the targetSchema?
	//dt.targetSchema = ??? // Union of toSchema and fromSchema???

	return nil
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
	dp := part.(DiffPartition)
	// TODO: commit_diff_table reuses DiffPartition from diff_table and we've switched diff_table over
	//       to a new format. After we switch commit_diff_table over to the same new format, we can
	//       remove this getLegacyRowIter method.
	//return dp.getLegacyRowIter(ctx, dt.ddb, dt.ss, dt.joiner)
	return dp.GetRowIter(ctx, dt.ddb, dt.joiner)
}
