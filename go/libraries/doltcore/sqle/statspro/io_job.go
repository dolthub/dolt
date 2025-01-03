package statspro

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/go-mysql-server/sql"
)

func (sc *StatsCoord) partitionStatReadJobs(ctx *sql.Context, sqlDb dsess.SqlDatabase, tableName string, levelNodes []tree.Node, prollyMap prolly.Map) ([]StatsJob, error) {

	if cnt, err := prollyMap.Count(); err != nil {
		return nil, err
	} else if cnt == 0 {
		return nil, nil
	}

	curCnt := 0
	lastStart := 0
	jobSize := 100_000
	var jobs []StatsJob
	var batchOrdinals []updateOrdinal
	var offset uint64
	for i, n := range levelNodes {
		treeCnt, err := n.TreeCount()
		if err != nil {
			return nil, err
		}
		ord := updateOrdinal{
			start: offset,
			stop:  offset + uint64(treeCnt),
		}
		offset += uint64(treeCnt)

		if _, ok := sc.BucketCache[n.HashOf()]; ok {
			// skip redundant work
			continue
		}

		curCnt += treeCnt
		batchOrdinals = append(batchOrdinals, ord)

		if curCnt > jobSize {
			jobs = append(jobs, ReadJob{ctx: ctx, db: sqlDb, table: tableName, m: prollyMap, nodes: levelNodes[lastStart : i+1], ordinals: batchOrdinals, done: make(chan struct{})})
			curCnt = 0
			batchOrdinals = batchOrdinals[:0]
			lastStart = i + 1
		}
	}
	if curCnt > 0 {
		jobs = append(jobs, ReadJob{ctx: ctx, db: sqlDb, table: tableName, m: prollyMap, nodes: levelNodes[lastStart:], ordinals: batchOrdinals, done: make(chan struct{})})
	}
	return jobs, nil
}
