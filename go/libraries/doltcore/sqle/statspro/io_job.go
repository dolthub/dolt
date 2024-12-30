package statspro

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

func partitionStatReadJobs(ctx context.Context, doltTable *doltdb.Table, sqlIndex sql.Index) ([]StatsJob, error) {
	var idx durable.Index
	var err error
	if strings.EqualFold(sqlIndex.ID(), "PRIMARY") {
		idx, err = doltTable.GetRowData(ctx)
	} else {
		idx, err = doltTable.GetIndexRowData(ctx, sqlIndex.ID())
	}
	if err != nil {
		return nil, err
	}

	prollyMap := durable.ProllyMapFromIndex(idx)

	if cnt, err := prollyMap.Count(); err != nil {
		return nil, err
	} else if cnt == 0 {
		return nil, nil
	}

	// get newest histogram target level hashes
	levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
	if err != nil {
		return nil, err
	}

	//var addrs []hash.Hash
	//var keepChunks []sql.HistogramBucket
	//var missingAddrs float64
	//var missingChunks []tree.Node
	//var missingOffsets []updateOrdinal
	//var offset uint64

	// todo accumulate node ordinals until we reach a batch threshold size
	// maybe like 100k rows minimum

	curCnt := 0
	lastStart := 0
	jobSize := 100_000
	var jobs []StatsJob
	//var batchOrdinals []updateOrdinal
	for i, n := range levelNodes {
		treeCnt, err := n.TreeCount()
		if err != nil {
			return nil, err
		}

		//batchOrdinals = append(batchOrdinals, updateOrdinal{
		//	start: offset,
		//	stop:  uint64(treeCnt),
		//})
		curCnt += treeCnt
		if curCnt > jobSize {
			jobs = append(jobs, ReadJob{m: prollyMap, nodes: levelNodes[lastStart : i+1]})
		}
		//offset += uint64(treeCnt)
	}
	return jobs, nil

	//for _, n := range levelNodes {
	//	// Compare the previous histogram chunks to the newest tree chunks.
	//	// Partition the newest chunks into 1) preserved or 2) missing.
	//	// Missing chunks will need to be scanned on a stats update, so
	//	// track the (start, end) ordinal offsets to simplify the read iter.
	//	treeCnt, err := n.TreeCount()
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	curCnt += treeCnt
	//
	//	addrs = append(addrs, n.HashOf())
	//	if bucketIdx, ok := curStats.Active[n.HashOf()]; !ok {
	//		missingChunks = append(missingChunks, n)
	//		missingOffsets = append(missingOffsets, updateOrdinal{offset, offset + uint64(treeCnt)})
	//		missingAddrs++
	//	} else {
	//		keepChunks = append(keepChunks, curStats.Hist[bucketIdx])
	//	}
	//	offset += uint64(treeCnt)
	//}

	//var dropChunks []sql.HistogramBucket
	//for _, h := range curStats.Chunks {
	//	var match bool
	//	for _, b := range keepChunks {
	//		if DoltBucketChunk(b) == h {
	//			match = true
	//			break
	//		}
	//	}
	//	if !match {
	//		dropChunks = append(dropChunks, curStats.Hist[curStats.Active[h]])
	//	}
	//}

	//return indexMeta{
	//	qual:           curStats.Statistic.Qual,
	//	cols:           cols,
	//	newNodes:       missingChunks,
	//	updateOrdinals: missingOffsets,
	//	keepChunks:     keepChunks,
	//	dropChunks:     dropChunks,
	//	allAddrs:       addrs,
	//}, nil
}
