// Copyright 2023 Dolthub, Inc.
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

package statspro

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func (sc *StatsCoord) seedDbTables(ctx context.Context, j SeedDbTablesJob) (ret []StatsJob, err error) {
	// get list of tables, get list of indexes, partition index ranges into ordinal blocks
	// return list of IO jobs for table/index/ordinal blocks
	defer func() {
		if errors.Is(doltdb.ErrWorkingSetNotFound, err) {
			err = nil
			ret = []StatsJob{NewSeedJob(j.sqlDb)}
		} else if err != nil {
			sc.seedCnt.Add(-1)
		}
	}()

	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return nil, err
	}
	dSess := dsess.DSessFromSess(sqlCtx.Session)
	db, err := dSess.Provider().Database(sqlCtx, j.sqlDb.AliasedName())
	if err != nil {
		return nil, err
	}
	sqlDb, err := sqle.RevisionDbForBranch(sqlCtx, db.(dsess.SqlDatabase), j.sqlDb.Revision(), j.sqlDb.Revision()+"/"+j.sqlDb.AliasedName())
	if err != nil {
		return nil, err
	}
	tableNames, err := sqlDb.GetTableNames(sqlCtx)
	if err != nil {
		return nil, err
	}

	var newTableInfo []tableStatsInfo
	var bucketDiff int

	i := 0
	k := 0
	for i < len(tableNames) && k < len(j.tables) {
		var jobs []StatsJob
		var ti tableStatsInfo
		switch strings.Compare(tableNames[i], j.tables[k].name) {
		case 0:
			// continue
			jobs, ti, err = sc.readJobsForTable(sqlCtx, sqlDb, j.tables[k])
			bucketDiff += ti.bucketCount - j.tables[k].bucketCount
			i++
			k++
		case -1:
			// new table
			jobs, ti, err = sc.readJobsForTable(sqlCtx, sqlDb, tableStatsInfo{name: tableNames[i]})
			bucketDiff += ti.bucketCount
			i++
		case +1:
			// dropped table
			jobs = append(jobs, sc.dropTableJob(sqlDb, j.tables[k].name))
			bucketDiff -= j.tables[k].bucketCount
			k++
		}
		if err != nil {
			return nil, err
		}
		if ti.name != "" {
			newTableInfo = append(newTableInfo, ti)
		}
		ret = append(ret, jobs...)
	}
	for i < len(tableNames) {
		jobs, ti, err := sc.readJobsForTable(sqlCtx, sqlDb, tableStatsInfo{name: tableNames[i]})
		if err != nil {
			return nil, err
		}
		bucketDiff += ti.bucketCount
		newTableInfo = append(newTableInfo, ti)
		ret = append(ret, jobs...)
		i++
	}

	for k < len(j.tables) {
		ret = append(ret, sc.dropTableJob(sqlDb, j.tables[k].name))
		bucketDiff -= j.tables[k].bucketCount
		k++
	}

	if bucketDiff > 0 {
		// flush results
		// TODO maybe make this a ticker
		ret = append(ret, NewControl("flush", func(sc *StatsCoord) error {
			ctx, err := sc.ctxGen(ctx)
			if err != nil {
				return err
			}
			if cnt, err := sc.kv.Flush(ctx); err != nil {
				return err
			} else if cnt > sc.kv.Len()*2 {
				sc.doGc.Store(true)
			}
			return nil
		}))
	}
	// retry again after finishing planned work
	ret = append(ret, SeedDbTablesJob{tables: newTableInfo, sqlDb: sqlDb, done: make(chan struct{})})
	return ret, nil
}

// GetLatestTable will get the WORKING root table for the current database/branch
func GetLatestTable(ctx *sql.Context, tableName string, sqlDb sql.Database) (*sqle.DoltTable, *doltdb.Table, error) {
	var db sqle.Database
	switch d := sqlDb.(type) {
	case sqle.Database:
		db = d
	case sqle.ReadReplicaDatabase:
		db = d.Database
	default:
		return nil, nil, fmt.Errorf("expected sqle.Database, found %T", sqlDb)
	}
	sqlTable, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("statistics refresh error: table not found %s", tableName)
	}

	var dTab *doltdb.Table
	var sqleTable *sqle.DoltTable
	switch t := sqlTable.(type) {
	case *sqle.AlterableDoltTable:
		sqleTable = t.DoltTable
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.WritableDoltTable:
		sqleTable = t.DoltTable
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.DoltTable:
		sqleTable = t
		dTab, err = t.DoltTable(ctx)
	default:
		err = fmt.Errorf("failed to unwrap dolt table from type: %T", sqlTable)
	}
	if err != nil {
		return nil, nil, err
	}
	return sqleTable, dTab, nil
}

func (sc *StatsCoord) readJobsForTable(ctx *sql.Context, sqlDb dsess.SqlDatabase, tableInfo tableStatsInfo) ([]StatsJob, tableStatsInfo, error) {
	if tableInfo.name == "is_restricted" {
		print()
	}
	var ret []StatsJob
	var bucketCnt int
	sqlTable, dTab, err := GetLatestTable(ctx, tableInfo.name, sqlDb)
	if err != nil {
		return nil, tableStatsInfo{}, err
	}
	indexes, err := sqlTable.GetIndexes(ctx)
	if err != nil {
		return nil, tableStatsInfo{}, err
	}

	schHashKey, _, err := sqlTable.IndexCacheKey(ctx)
	if err != nil {
		return nil, tableStatsInfo{}, err
	}

	schemaChanged := !tableInfo.schHash.Equal(schHashKey.Hash)
	if !tableInfo.schHash.IsEmpty() && schemaChanged {
		sc.setGc()
	}

	var dataChanged bool
	var isNewData bool
	var newIdxRoots []hash.Hash

	keepIndexes := make(map[sql.StatQualifier]bool)
	fullIndexBuckets := make(map[templateCacheKey]finalizeStruct)
	for i, sqlIdx := range indexes {
		var idx durable.Index
		var err error
		if strings.EqualFold(sqlIdx.ID(), "PRIMARY") {
			idx, err = dTab.GetRowData(ctx)
		} else {
			idx, err = dTab.GetIndexRowData(ctx, sqlIdx.ID())
		}
		if err != nil {
			return nil, tableStatsInfo{}, err
		}

		prollyMap := durable.ProllyMapFromIndex(idx)

		idxRoot := prollyMap.Node().HashOf()
		newIdxRoots = append(newIdxRoots, idxRoot)

		levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
		if err != nil {
			return nil, tableStatsInfo{}, err
		}

		bucketCnt += len(levelNodes)

		indexKey := templateCacheKey{h: schHashKey.Hash, idxName: sqlIdx.ID()}

		if i < len(tableInfo.idxRoots) && idxRoot.Equal(tableInfo.idxRoots[i]) && !schemaChanged {
			qual := sql.StatQualifier{
				Tab:      tableInfo.name,
				Database: strings.ToLower(sqlDb.AliasedName()),
				Idx:      strings.ToLower(sqlIdx.ID()),
			}
			keepIndexes[qual] = true
			continue
		}
		dataChanged = true

		var buckets []hash.Hash
		for _, n := range levelNodes {
			buckets = append(buckets, n.HashOf())
		}
		fullIndexBuckets[indexKey] = finalizeStruct{
			buckets: buckets,
			tupB:    val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(len(sqlIdx.Expressions()))),
		}

		key, template, err := sc.getTemplate(ctx, sqlTable, sqlIdx)
		if err != nil {
			sc.logger.Errorf("stats collection failed to generate a statistic template: %s.%s.%s:%T; %s", sqlDb.RevisionQualifiedName(), tableInfo.name, sqlIdx, sqlIdx, err)
			continue
		}

		readJobs, err := sc.partitionStatReadJobs(ctx, sqlDb, tableInfo.name, key, template, levelNodes, prollyMap, len(sqlIdx.Expressions()))
		if err != nil {
			return nil, tableStatsInfo{}, err
		}
		ret = append(ret, readJobs...)
		isNewData = isNewData || dataChanged
	}
	if len(ret) > 0 || isNewData || schemaChanged {
		// if there are any reads to perform, we follow those reads with a table finalize
		ret = append(ret, FinalizeJob{
			sqlDb: sqlDb,
			tableKey: tableIndexesKey{
				db:     sqlDb.AliasedName(),
				branch: sqlDb.Revision(),
				table:  tableInfo.name,
			},
			keepIndexes: keepIndexes,
			editIndexes: fullIndexBuckets,
			done:        make(chan struct{}),
		})
	}

	return ret, tableStatsInfo{name: tableInfo.name, schHash: schHashKey.Hash, idxRoots: newIdxRoots, bucketCount: bucketCnt}, nil
}

type updateOrdinal struct {
	start, stop uint64
}

func (sc *StatsCoord) partitionStatReadJobs(ctx *sql.Context, sqlDb dsess.SqlDatabase, tableName string, key templateCacheKey, template stats.Statistic, levelNodes []tree.Node, prollyMap prolly.Map, idxCnt int) ([]StatsJob, error) {
	if cnt, err := prollyMap.Count(); err != nil {
		return nil, err
	} else if cnt == 0 {
		return nil, nil
	}

	curCnt := 0
	jobSize := 100_000
	var jobs []StatsJob
	var batchOrdinals []updateOrdinal
	var nodes []tree.Node
	var offset uint64
	for _, n := range levelNodes {
		treeCnt, err := n.TreeCount()
		if err != nil {
			return nil, err
		}
		ord := updateOrdinal{
			start: offset,
			stop:  offset + uint64(treeCnt),
		}
		offset += uint64(treeCnt)

		if _, ok, err := sc.kv.GetBucket(ctx, n.HashOf(), val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxCnt))); err != nil {
			return nil, err
		} else if ok {
			// skip redundant work
			continue
		}

		curCnt += treeCnt
		batchOrdinals = append(batchOrdinals, ord)
		nodes = append(nodes, n)

		if curCnt > jobSize {
			first := batchOrdinals[0].start == 0
			jobs = append(jobs, ReadJob{ctx: ctx, db: sqlDb, first: first, table: tableName, key: key, template: template, m: prollyMap, nodes: nodes, ordinals: batchOrdinals, idxLen: idxCnt, done: make(chan struct{})})
			curCnt = 0
			batchOrdinals = nil
			nodes = nil
		}
	}
	if curCnt > 0 {
		first := batchOrdinals[0].start == 0
		jobs = append(jobs, ReadJob{ctx: ctx, db: sqlDb, first: first, table: tableName, key: key, template: template, m: prollyMap, nodes: nodes, ordinals: batchOrdinals, idxLen: idxCnt, done: make(chan struct{})})
	}

	return jobs, nil
}

type templateCacheKey struct {
	h       hash.Hash
	idxName string
}

func (k templateCacheKey) String() string {
	return k.idxName + "/" + k.h.String()[:5]
}

func (sc *StatsCoord) getTemplate(ctx *sql.Context, sqlTable *sqle.DoltTable, sqlIdx sql.Index) (templateCacheKey, stats.Statistic, error) {
	schHash, _, err := sqlTable.IndexCacheKey(ctx)
	key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
	if template, ok := sc.kv.GetTemplate(key); ok {
		return key, template, nil
	}
	fds, colset, err := stats.IndexFds(strings.ToLower(sqlTable.Name()), sqlTable.Schema(), sqlIdx)
	if err != nil {
		return templateCacheKey{}, stats.Statistic{}, err
	}

	var class sql.IndexClass
	switch {
	case sqlIdx.IsSpatial():
		class = sql.IndexClassSpatial
	case sqlIdx.IsFullText():
		class = sql.IndexClassFulltext
	default:
		class = sql.IndexClassDefault
	}

	var types []sql.Type
	for _, cet := range sqlIdx.ColumnExpressionTypes() {
		types = append(types, cet.Type)
	}

	tablePrefix := sqlTable.Name() + "."
	cols := make([]string, len(sqlIdx.Expressions()))
	for i, c := range sqlIdx.Expressions() {
		cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
	}

	template := stats.Statistic{
		Cols:     cols,
		Typs:     types,
		IdxClass: uint8(class),
		Fds:      fds,
		Colset:   colset,
	}

	// We put template twice, once for schema changes with no data
	// changes (here), and once when we put chunks to avoid GC dropping
	// templates before the finalize job.
	sc.kv.PutTemplate(key, template)

	return key, template, nil
}
