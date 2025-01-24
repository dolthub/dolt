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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"strings"
)

func (sc *StatsCoord) seedDbTables(_ context.Context, j SeedDbTablesJob) ([]StatsJob, error) {
	// get list of tables, get list of indexes, partition index ranges into ordinal blocks
	// return list of IO jobs for table/index/ordinal blocks
	tableNames, err := j.sqlDb.GetTableNames(j.ctx)
	if err != nil {
		if errors.Is(err, doltdb.ErrBranchNotFound) {
			return []StatsJob{sc.dropBranchJob(j.sqlDb.AliasedName(), j.sqlDb.Revision())}, nil
		}
		return nil, err
	}

	var newTableInfo []tableStatsInfo
	var ret []StatsJob

	var bucketDiff int

	i := 0
	k := 0
	for i < len(tableNames) && k < len(j.tables) {
		var jobs []StatsJob
		var ti tableStatsInfo
		switch strings.Compare(tableNames[i], j.tables[k].name) {
		case 0:
			// continue
			jobs, ti, err = sc.readJobsForTable(j.ctx, j.sqlDb, j.tables[k])
			bucketDiff += ti.bucketCount - j.tables[k].bucketCount
			i++
			k++
		case -1:
			// new table
			jobs, ti, err = sc.readJobsForTable(j.ctx, j.sqlDb, tableStatsInfo{name: tableNames[i]})
			bucketDiff += ti.bucketCount
			i++
		case +1:
			// dropped table
			jobs = append(jobs, sc.dropTableJob(j.sqlDb, j.tables[k].name))
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
		jobs, ti, err := sc.readJobsForTable(j.ctx, j.sqlDb, tableStatsInfo{name: tableNames[i]})
		if err != nil {
			return nil, err
		}
		bucketDiff += ti.bucketCount
		newTableInfo = append(newTableInfo, ti)
		ret = append(ret, jobs...)
		i++
	}

	for k < len(j.tables) {
		ret = append(ret, sc.dropTableJob(j.sqlDb, j.tables[k].name))
		bucketDiff -= j.tables[k].bucketCount
		k++
	}

	sc.bucketCnt.Add(int64(bucketDiff))

	for sc.bucketCnt.Load() > sc.bucketCap {
		sc.bucketCap *= 2
		sc.doGc.Store(true)
	}

	// retry again after finishing planned work
	ret = append(ret, SeedDbTablesJob{tables: newTableInfo, sqlDb: j.sqlDb, ctx: j.ctx, done: make(chan struct{})})
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
	if schemaChanged {
		sc.setGc()
	}

	var dataChanged bool
	var isNewData bool
	var newIdxRoots []hash.Hash

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

		if err := sc.cacheTemplate(ctx, sqlTable, sqlIdx); err != nil {
			sc.logger.Debugf("stats collection failed to generate a statistic template: %s.%s.%s:%T; %s", sqlDb.RevisionQualifiedName(), tableInfo.name, sqlIdx, sqlIdx, err)
			continue
		}

		prollyMap := durable.ProllyMapFromIndex(idx)

		idxRoot := prollyMap.Node().HashOf()
		newIdxRoots = append(newIdxRoots, idxRoot)

		levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
		if err != nil {
			return nil, tableStatsInfo{}, err
		}

		bucketCnt += len(levelNodes)

		if i < len(tableInfo.idxRoots) && idxRoot.Equal(tableInfo.idxRoots[i]) && !schemaChanged && !sc.activeGc.Load() {
			continue
		}
		dataChanged = true

		indexKey := templateCacheKey{h: schHashKey.Hash, idxName: sqlIdx.ID()}
		var buckets []hash.Hash
		for _, n := range levelNodes {
			buckets = append(buckets, n.HashOf())
		}
		fullIndexBuckets[indexKey] = finalizeStruct{
			buckets: buckets,
			tupB:    val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(len(sqlIdx.Expressions()))),
		}

		readJobs, err := sc.partitionStatReadJobs(ctx, sqlDb, tableInfo.name, levelNodes, prollyMap, len(sqlIdx.Expressions()))
		if err != nil {
			return nil, tableStatsInfo{}, err
		}
		ret = append(ret, readJobs...)
		isNewData = isNewData || len(readJobs) > 0
	}
	if len(ret) > 0 && (isNewData || schemaChanged || dataChanged) {
		// if there are any reads to perform, we follow those reads with a table finalize
		ret = append(ret, FinalizeJob{
			tableKey: tableIndexesKey{
				db:     sqlDb.AliasedName(),
				branch: sqlDb.Revision(),
				table:  tableInfo.name,
			},
			indexes: fullIndexBuckets,
			done:    make(chan struct{}),
		})
	}

	return ret, tableStatsInfo{name: tableInfo.name, schHash: schHashKey.Hash, idxRoots: newIdxRoots, bucketCount: bucketCnt}, nil
}

type updateOrdinal struct {
	start, stop uint64
}

func (sc *StatsCoord) partitionStatReadJobs(ctx *sql.Context, sqlDb dsess.SqlDatabase, tableName string, levelNodes []tree.Node, prollyMap prolly.Map, idxCnt int) ([]StatsJob, error) {
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
			jobs = append(jobs, ReadJob{ctx: ctx, db: sqlDb, table: tableName, m: prollyMap, nodes: nodes, ordinals: batchOrdinals, colCnt: idxCnt, done: make(chan struct{})})
			curCnt = 0
			batchOrdinals = batchOrdinals[:0]
			nodes = nodes[:0]
		}
	}
	if curCnt > 0 {
		jobs = append(jobs, ReadJob{ctx: ctx, db: sqlDb, table: tableName, m: prollyMap, nodes: nodes, ordinals: batchOrdinals, colCnt: idxCnt, done: make(chan struct{})})
	}

	if len(jobs) > 0 || sc.activeGc.Load() {
		firstNodeHash := levelNodes[0].HashOf()
		if _, ok := sc.kv.GetBound(firstNodeHash); !ok {
			firstRow, err := firstRowForIndex(ctx, prollyMap, val.NewTupleBuilder(prollyMap.KeyDesc()))
			if err != nil {
				return nil, err
			}
			fmt.Printf("%s bound %s: %v\n", tableName, firstNodeHash.String(), firstRow)
			sc.kv.PutBound(firstNodeHash, firstRow)
		}
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

func (sc *StatsCoord) cacheTemplate(ctx *sql.Context, sqlTable *sqle.DoltTable, sqlIdx sql.Index) error {
	schHash, _, err := sqlTable.IndexCacheKey(ctx)
	key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
	if _, ok := sc.kv.GetTemplate(key); ok {
		return nil
	}
	fds, colset, err := stats.IndexFds(sqlTable.Name(), sqlTable.Schema(), sqlIdx)
	if err != nil {
		return err
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

	sc.kv.PutTemplate(key, stats.Statistic{
		Cols:     cols,
		Typs:     types,
		IdxClass: uint8(class),
		Fds:      fds,
		Colset:   colset,
	})
	return nil
}
