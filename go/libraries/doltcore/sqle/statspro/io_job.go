package statspro

import (
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

func (sc *StatsCoord) partitionStatReadJobs(levelNodes []tree.Node, prollyMap prolly.Map) ([]StatsJob, error) {

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
			stop:  uint64(treeCnt),
		}
		offset += uint64(treeCnt)

		if _, ok := sc.BucketCache[n.HashOf()]; ok {
			// skip redundant work
			continue
		}

		curCnt += treeCnt
		batchOrdinals = append(batchOrdinals, ord)

		if curCnt > jobSize {
			jobs = append(jobs, ReadJob{m: prollyMap, nodes: levelNodes[lastStart : i+1]})
		}
	}
	return jobs, nil
}
