// Copyright 2019 Liquidata, Inc.
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

package sqle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	DoltDiffTablePrefix = "dolt_diff_"
	toCommit            = "to_commit"
	fromCommit          = "from_commit"

	diffTypeColName  = "diff_type"
	diffTypeAdded    = "added"
	diffTypeModified = "modified"
	diffTypeRemoved  = "removed"
)

var _ sql.FilteredTable = (*DiffTable)(nil)

type DiffTable struct {
	name          string
	ddb           *doltdb.DoltDB
	rsr           env.RepoStateReader
	ss            *schema.SuperSchema
	joiner        *rowconv.Joiner
	sqlSch        sql.Schema
	fromRoot      *doltdb.RootValue
	toRoot        *doltdb.RootValue
	fromCommitVal string
	toCommitVal   string
	filters       []sql.Expression
}

func NewDiffTable(ctx context.Context, name string, ddb *doltdb.DoltDB, rsr env.RepoStateReader) (*DiffTable, error) {
	diffTblName := DoltDiffTablePrefix + name

	cmItr, err := doltdb.CommitItrForAllBranches(ctx, ddb)

	if err != nil {
		return nil, err
	}

	ss, err := SuperSchemaForAllBranches(ctx, cmItr, ddb, rsr, name)

	if err != nil {
		return nil, err
	}

	_ = ss.AddColumn(schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false))

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

	root1, err := ddb.ReadRootValue(ctx, rsr.WorkingHash())

	if err != nil {
		return nil, err
	}

	root2, err := ddb.ReadRootValue(ctx, rsr.StagedHash())

	if err != nil {
		return nil, err
	}

	sqlSch, err := doltSchemaToSqlSchema(diffTblName, j.GetSchema())

	// TODO: fix panics
	if err != nil {
		panic(err)
	}

	sqlSch = append(sqlSch, &sql.Column{
		Name:     diffTypeColName,
		Type:     sql.Text,
		Default:  diffTypeModified,
		Nullable: false,
		Source:   diffTblName,
	})

	return &DiffTable{name, ddb, rsr, ss, j, sqlSch, root2, root1, "current", "HEAD", nil}, nil
}

func (dt *DiffTable) Name() string {
	return DoltDiffTablePrefix + dt.name
}

func (dt *DiffTable) String() string {
	return DoltDiffTablePrefix + dt.name
}

func (dt *DiffTable) Schema() sql.Schema {
	return dt.sqlSch
}

func toNamer(name string) string {
	return diff.To + "_" + name
}

func fromNamer(name string) string {
	return diff.From + "_" + name
}

func (dt *DiffTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &doltTablePartitionIter{}, nil
}

func tableData(ctx *sql.Context, root *doltdb.RootValue, tblName string, ddb *doltdb.DoltDB) (types.Map, schema.Schema, error) {
	tbl, _, ok, err := root.GetTableInsensitive(ctx, tblName)

	if err != nil {
		return types.EmptyMap, nil, err
	}

	var data types.Map
	if !ok {
		data, err = types.NewMap(ctx, ddb.ValueReadWriter())
	} else {
		data, err = tbl.GetRowData(ctx)
	}

	if err != nil {
		return types.EmptyMap, nil, err
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return types.EmptyMap, nil, err
	}

	return data, sch, nil
}

func (dt *DiffTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	fromData, fromSch, err := tableData(ctx, dt.fromRoot, dt.name, dt.ddb)

	if err != nil {
		return nil, err
	}

	toData, toSch, err := tableData(ctx, dt.toRoot, dt.name, dt.ddb)

	if err != nil {
		return nil, err
	}

	fromConv, err := rowConvForSchema(dt.ss, fromSch)

	if err != nil {
		return nil, err
	}

	toConv, err := rowConvForSchema(dt.ss, toSch)

	if err != nil {
		return nil, err
	}

	sch := dt.joiner.GetSchema()
	toCol, ok := sch.GetAllCols().GetByName(toCommit)

	if !ok {
		panic("missing required column")
	}

	fromCol, ok := sch.GetAllCols().GetByName(fromCommit)

	if !ok {
		panic("missing required column")
	}

	return newDiffRowItr(ctx, dt.joiner, fromData, toData, fromConv, toConv, dt.fromCommitVal, dt.toCommitVal, fromCol.Tag, toCol.Tag), nil
}

var _ sql.RowIter = (*diffRowItr)(nil)

type diffRowItr struct {
	ad      *diff.AsyncDiffer
	diffSrc *diff.RowDiffSource
	joiner  *rowconv.Joiner
	sch     schema.Schema
	to      string
	from    string
	fromTag uint64
	toTag   uint64
}

func newDiffRowItr(ctx context.Context, joiner *rowconv.Joiner, rowDataFrom, rowDataTo types.Map, convFrom, convTo *rowconv.RowConverter, from, to string, fromTag, toTag uint64) *diffRowItr {
	ad := diff.NewAsyncDiffer(1024)
	ad.Start(ctx, rowDataTo, rowDataFrom)

	src := diff.NewRowDiffSource(ad, joiner)
	src.AddInputRowConversion(convFrom, convTo)

	return &diffRowItr{ad, src, joiner, joiner.GetSchema(), to, from, fromTag, toTag}
}

// Next returns the next row
func (itr *diffRowItr) Next() (sql.Row, error) {
	r, _, err := itr.diffSrc.NextDiff()

	if err != nil {
		return nil, err
	}

	toAndFromRows, err := itr.joiner.Split(r)
	_, hasTo := toAndFromRows[diff.To]
	_, hasFrom := toAndFromRows[diff.From]

	r, err = r.SetColVal(itr.toTag, types.String(itr.to), itr.sch)

	if err != nil {
		return nil, err
	}

	r, err = r.SetColVal(itr.fromTag, types.String(itr.from), itr.sch)

	if err != nil {
		return nil, err
	}

	sqlRow, err := doltRowToSqlRow(r, itr.sch)

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
func (itr *diffRowItr) Close() (err error) {
	defer itr.ad.Close()
	defer func() {
		closeErr := itr.diffSrc.Close()

		if err == nil {
			err = closeErr
		}
	}()

	return nil
}

// HandledFilters returns the list of filters that will be handled by the table itself
func (dt *DiffTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	handled := make([]sql.Expression, 0, len(filters))
	for _, f := range filters {
		if _, ok := f.(*expression.Equals); !ok {
			continue
		}

		sql.Inspect(f, func(e sql.Expression) bool {
			if e, ok := e.(*expression.GetField); ok {
				if e.Table() == dt.Name() && e.Name() == toCommit || e.Name() == fromCommit {
					handled = append(handled, f)
					return false
				}
			}
			return true
		})
	}

	return handled
}

// WithFilters returns a new sql.Table instance with the filters applied
func (dt *DiffTable) WithFilters(filters []sql.Expression) sql.Table {
	ctx := context.TODO()

	for _, f := range filters {
		if _, ok := f.(*expression.Equals); !ok {
			continue
		}

		var fieldName string
		var value string
		sql.Inspect(f, func(e sql.Expression) bool {
			if e == nil {
				return true
			}

			switch val := e.(type) {
			case *expression.GetField:
				fieldName = val.Name()
			case *expression.Literal:
				value = val.String()
			}

			return true
		})

		value = strings.Trim(value, " \t\n\r\"")

		cs, err := doltdb.NewCommitSpec(value, dt.rsr.CWBHeadRef().String())

		if err != nil {
			panic(err)
		}

		cm, err := dt.ddb.Resolve(ctx, cs)

		if err != nil {
			panic(err)
		}

		root, err := cm.GetRootValue()

		if err != nil {
			panic(err)
		}

		switch fieldName {
		case toCommit:
			dt.toRoot = root
			dt.toCommitVal = value
		case fromCommit:
			dt.fromRoot = root
			dt.fromCommitVal = value
		}
	}

	dt.filters = filters
	return dt
}

// Filters returns the list of filters that are applied to this table.
func (dt *DiffTable) Filters() []sql.Expression {
	return dt.filters
}

func SuperSchemaForAllBranches(ctx context.Context, cmItr doltdb.CommitItr, ddb *doltdb.DoltDB, rsr env.RepoStateReader, tblName string) (*schema.SuperSchema, error) {
	wr, err := ddb.ReadRootValue(ctx, rsr.WorkingHash())

	if err != nil {
		return nil, err
	}

	t, ok, err := wr.GetTable(ctx, tblName)

	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, errors.New(fmt.Sprintf("table: %s does not exist", tblName))
	}

	sch, err := t.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	ss, err := schema.NewSuperSchema(sch)

	if err != nil {
		return nil, err
	}

	addedSchemas := make(map[hash.Hash]bool)

	for {
		_, cm, err := cmItr.Next(ctx)

		if err != nil {
			if err == io.EOF {
				return ss, nil
			}

			return nil, err
		}

		root, err := cm.GetRootValue()

		if err != nil {
			return nil, err
		}

		tbl, _, ok, err := root.GetTableInsensitive(ctx, tblName)

		if err != nil {
			return nil, err
		}

		if ok {
			schRef, err := tbl.GetSchemaRef()

			if err != nil {
				return nil, err
			}

			h := schRef.TargetHash()

			if !addedSchemas[h] {
				addedSchemas[h] = true
				sch, err := tbl.GetSchema(ctx)

				if err != nil {
					return nil, err
				}

				err = ss.AddSchemas(sch)

				if err != nil {
					return nil, err
				}
			}
		}
	}
}

// creates a RowConverter for transforming rows with the the given schema to this super schema.
func rowConvForSchema(ss *schema.SuperSchema, sch schema.Schema) (*rowconv.RowConverter, error) {
	inNameToOutName, err := ss.NameMapForSchema(sch)

	if err != nil {
		return nil, err
	}

	ssch, err := ss.GenerateSchema()

	if err != nil {
		return nil, err
	}

	fm, err := rowconv.NewFieldMappingFromNameMap(sch, ssch, inNameToOutName)

	if err != nil {
		return nil, err
	}

	return rowconv.NewRowConverter(fm)
}
