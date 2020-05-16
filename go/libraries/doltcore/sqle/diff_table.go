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

	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
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

func toNamer(name string) string {
	return diff.To + "_" + name
}

func fromNamer(name string) string {
	return diff.From + "_" + name
}

var _ sql.Table = (*DiffTable)(nil)

type DiffTable struct {
	name       string
	ddb        *doltdb.DoltDB
	ss         *schema.SuperSchema
	joiner     *rowconv.Joiner
	sqlSch     sql.Schema
	partitions *diffPartitions
}

func NewDiffTable(ctx *sql.Context, dbName, tblName string) (*DiffTable, error) {
	sess := DSessFromSess(ctx.Session)
	ddb, ok := sess.GetDoltDB(dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	diffTblName := doltdb.DoltDiffTablePrefix + tblName
	rootCmt, err := sess.GetParentCommit(ctx, dbName)

	if err != nil {
		return nil, err
	}

	workingRoot, ok := sess.GetRoot(dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	cmItr := doltdb.CommitItrForRoots(ddb, rootCmt)
	ss, partitions, err := calcSuperSchemaAndPartitions(ctx, cmItr, workingRoot, tblName)

	if err != nil {
		return nil, err
	}

	_ = ss.AddColumn(schema.NewColumn("commit", doltdb.DiffCommitTag, types.StringKind, false))
	_ = ss.AddColumn(schema.NewColumn("commit_date", doltdb.DiffCommitDateTag, types.TimestampKind, false))

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

	return &DiffTable{tblName, ddb, ss, j, sqlSch, partitions}, nil
}

func (dt *DiffTable) Name() string {
	return doltdb.DoltDiffTablePrefix + dt.name
}

func (dt *DiffTable) String() string {
	return doltdb.DoltDiffTablePrefix + dt.name
}

func (dt *DiffTable) Schema() sql.Schema {
	return dt.sqlSch
}

func (dt *DiffTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return dt.partitions, nil
}

func (dt *DiffTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	dp := part.(diffPartition)
	fromData, fromSch, err := tableData(ctx, dp.from, dt.ddb)

	if err != nil {
		return nil, err
	}

	toData, toSch, err := tableData(ctx, dp.to, dt.ddb)

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
	toCol, _ := sch.GetAllCols().GetByName(toCommit)
	fromCol, _ := sch.GetAllCols().GetByName(fromCommit)
	toDateCol, _ := sch.GetAllCols().GetByName(toCommitDate)
	fromDateCol, _ := sch.GetAllCols().GetByName(fromCommitDate)

	fromCmInfo := commitInfo{types.String(dp.fromName), dp.fromDate, fromCol.Tag, fromDateCol.Tag}
	toCmInfo := commitInfo{types.String(dp.toName), dp.toDate, toCol.Tag, toDateCol.Tag}

	return newDiffRowItr(
		ctx,
		dt.joiner,
		fromData,
		toData,
		fromConv,
		toConv,
		fromCmInfo,
		toCmInfo,
	), nil
}

func tableData(ctx *sql.Context, tbl *doltdb.Table, ddb *doltdb.DoltDB) (types.Map, schema.Schema, error) {
	var data types.Map
	var err error
	if tbl == nil {
		data, err = types.NewMap(ctx, ddb.ValueReadWriter())
	} else {
		data, err = tbl.GetRowData(ctx)

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
	ad             *diff.AsyncDiffer
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

func newDiffRowItr(ctx context.Context, joiner *rowconv.Joiner, rowDataFrom, rowDataTo types.Map, convFrom, convTo *rowconv.RowConverter, from, to commitInfo) *diffRowItr {
	ad := diff.NewAsyncDiffer(1024)
	ad.Start(ctx, rowDataTo, rowDataFrom)

	src := diff.NewRowDiffSource(ad, joiner)
	src.AddInputRowConversion(convFrom, convTo)

	return &diffRowItr{ad, src, joiner, joiner.GetSchema(), from, to}
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

type tblInfoAtCommit struct {
	name    string
	date    *types.Timestamp
	tbl     *doltdb.Table
	tblHash hash.Hash
}

// data partitioned into pairs of table states which get compared
type diffPartition struct {
	to       *doltdb.Table
	from     *doltdb.Table
	toName   string
	fromName string
	toDate   *types.Timestamp
	fromDate *types.Timestamp
}

func (dp diffPartition) Key() []byte {
	return []byte(dp.toName + dp.fromName)
}

var _ sql.PartitionIter = &diffPartitions{}

// collection of paratitions. Implements PartitionItr
type diffPartitions struct {
	partitions []diffPartition
	pos        int

	tblName         string
	cmHashToTblInfo map[hash.Hash]tblInfoAtCommit
}

func newDiffPartitions(tblName string) *diffPartitions {
	return &diffPartitions{nil, 0, tblName, make(map[hash.Hash]tblInfoAtCommit)}
}

// called in a commit iteration loop. Adds partitions when it finds a commit and it's parent that have different values
// for the hash of the table being looked at.
func (dp *diffPartitions) processCommit(ctx context.Context, cmHash hash.Hash, cm *doltdb.Commit, root *doltdb.RootValue, tbl *doltdb.Table) error {
	tblHash, _, err := root.GetTableHash(ctx, dp.tblName)

	if err != nil {
		return err
	}

	toInfoForCommit := dp.cmHashToTblInfo[cmHash]
	cmHashStr := cmHash.String()
	meta, err := cm.GetCommitMeta()

	if err != nil {
		return err
	}

	ts := types.Timestamp(meta.Time())

	if tblHash != toInfoForCommit.tblHash {
		dp.partitions = append(dp.partitions, diffPartition{toInfoForCommit.tbl, tbl, toInfoForCommit.name, cmHashStr, toInfoForCommit.date, &ts})
	}

	newInfo := tblInfoAtCommit{cmHashStr, &ts, tbl, tblHash}
	parentHashes, err := cm.ParentHashes(ctx)

	if err != nil {
		return err
	}

	for _, h := range parentHashes {
		dp.cmHashToTblInfo[h] = newInfo
	}

	return nil
}

func (dp *diffPartitions) Next() (sql.Partition, error) {
	if dp.pos < len(dp.partitions) {
		nextPart := dp.partitions[dp.pos]
		dp.pos++

		return nextPart, nil
	}

	return nil, io.EOF
}

func (dp *diffPartitions) Close() error {
	return nil
}

// exhaustively iterates through commit graph calculating the super schema, and finding all the partitions.
func calcSuperSchemaAndPartitions(ctx context.Context, cmItr doltdb.CommitItr, wr *doltdb.RootValue, tblName string) (*schema.SuperSchema, *diffPartitions, error) {
	t, exactName, ok, err := wr.GetTableInsensitive(ctx, tblName)

	if err != nil {
		return nil, nil, err
	}

	if !ok {
		return nil, nil, errors.New(fmt.Sprintf("table: %s does not exist", tblName))
	}

	sch, err := t.GetSchema(ctx)

	if err != nil {
		return nil, nil, err
	}

	ss, err := schema.NewSuperSchema(sch)

	if err != nil {
		return nil, nil, err
	}

	schRef, err := t.GetSchemaRef()

	if err != nil {
		return nil, nil, err
	}

	h := schRef.TargetHash()
	addedSchemas := map[hash.Hash]bool{h: true}

	diffPartitions := newDiffPartitions(exactName)
	wrTblHash, _, err := wr.GetTableHash(ctx, exactName)

	if err != nil {
		return nil, nil, err
	}

	cmHash, _, err := cmItr.Next(ctx)

	if err != nil {
		return nil, nil, err
	}

	diffPartitions.cmHashToTblInfo[cmHash] = tblInfoAtCommit{"WORKING", nil, t, wrTblHash}
	err = cmItr.Reset(ctx)

	if err != nil {
		return nil, nil, err
	}

	for {
		cmHash, cm, err := cmItr.Next(ctx)

		if err != nil {
			if err == io.EOF {
				return ss, diffPartitions, nil
			}

			return nil, nil, err
		}

		root, err := cm.GetRootValue()

		if err != nil {
			return nil, nil, err
		}

		tbl, ok, err := root.GetTable(ctx, exactName)

		if err != nil {
			return nil, nil, err
		}

		err = diffPartitions.processCommit(ctx, cmHash, cm, root, tbl)

		if ok {
			schRef, err := tbl.GetSchemaRef()

			if err != nil {
				return nil, nil, err
			}

			h := schRef.TargetHash()

			if !addedSchemas[h] {
				addedSchemas[h] = true
				sch, err := tbl.GetSchema(ctx)

				if err != nil {
					return nil, nil, err
				}

				err = ss.AddSchemas(sch)

				if err != nil {
					return nil, nil, err
				}
			}
		}
	}
}

// creates a RowConverter for transforming rows with the the given schema to this super schema.
func rowConvForSchema(ss *schema.SuperSchema, sch schema.Schema) (*rowconv.RowConverter, error) {
	eq, err := schema.SchemasAreEqual(sch, schema.EmptySchema)
	if err != nil {
		return nil, err
	}
	if eq {
		return rowconv.IdentityConverter, nil
	}

	inNameToOutName, err := ss.NameMapForSchema(sch)

	if err != nil {
		return nil, err
	}

	ssch, err := ss.GenerateSchema()

	if err != nil {
		return nil, err
	}

	fm, err := rowconv.NameMapping(sch, ssch, inNameToOutName)

	if err != nil {
		return nil, err
	}

	return rowconv.NewRowConverter(fm)
}
