package statspro

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
)

func (sc *StatsCoord) partitionStatReadJobs(ctx *sql.Context, sqlDb sqle.Database, tableName string, levelNodes []tree.Node, prollyMap prolly.Map) ([]StatsJob, error) {
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

		if _, ok := sc.BucketCache[n.HashOf()]; ok {
			// skip redundant work
			continue
		}

		curCnt += treeCnt
		batchOrdinals = append(batchOrdinals, ord)
		nodes = append(nodes, n)

		if curCnt > jobSize {
			jobs = append(jobs, ReadJob{ctx: ctx, db: sqlDb, table: tableName, m: prollyMap, nodes: nodes, ordinals: batchOrdinals, done: make(chan struct{})})
			curCnt = 0
			batchOrdinals = batchOrdinals[:0]
			nodes = nodes[:0]
		}
	}
	if curCnt > 0 {
		jobs = append(jobs, ReadJob{ctx: ctx, db: sqlDb, table: tableName, m: prollyMap, nodes: nodes, ordinals: batchOrdinals, done: make(chan struct{})})
	}

	if len(jobs) > 0 {
		firstNodeHash := levelNodes[0].HashOf()
		if _, ok := sc.LowerBoundCache[firstNodeHash]; !ok {
			firstRow, err := firstRowForIndex(ctx, prollyMap, val.NewTupleBuilder(prollyMap.KeyDesc()), prollyMap.KeyDesc().Count())
			if err != nil {
				return nil, err
			}
			sc.putFirstRow(firstNodeHash, firstRow)
		}
	}
	return jobs, nil
}
