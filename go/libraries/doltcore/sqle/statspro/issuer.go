package statspro

import (
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"io"
	"log"
	"strings"
	"time"
)

// thread that does a full root walk, gets databases/branches/tables

// control work throughput on sender or receiver side?

//

func (sc *StatsCoord) newCycle(ctx context.Context) (context.Context, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	select {
	case <-ctx.Done():
		// thread invalidated and doesn't own stack
		return ctx, nil
	default:
		// otherwise we still own the stack
	}
	if len(sc.activeCancels) != 2 || len(sc.activeCtx) != 2 {
		return nil, fmt.Errorf("thread owning stasts issuing expects two context, found %d", len(sc.activeCtx))
	}

	sc.activeCancels[1]()
	sc.activeCtx[1], sc.activeCancels[1] = context.WithCancel(sc.activeCtx[0])
	return sc.activeCtx[1], nil
}

func (sc *StatsCoord) newThreadCtx() (context.Context, context.Context) {
	sc.Stop()

	sc.statsMu.Lock()
	sc.statsMu.Unlock()

	newCtx, cancel := context.WithCancel(context.Background())
	cycleCtx, cycleCancel := context.WithCancel(newCtx)

	sc.activeCtx = append(sc.activeCtx, newCtx, cycleCtx)
	sc.activeCancels = append(sc.activeCancels, cancel, cycleCancel)
	return newCtx, cycleCtx
}

var ErrStatsIssuerPaused = fmt.Errorf("stats issuer is paused")

func (sc *StatsCoord) getLatestCtx() (context.Context, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	if len(sc.activeCtx) != 2 {
		return nil, ErrStatsIssuerPaused
	}
	return sc.activeCtx[1], nil
}

// Stop stops the sender thread and then pauses the queue
func (sc *StatsCoord) Stop() {
	sc.statsMu.Lock()
	sc.statsMu.Unlock()
	for _, f := range sc.activeCancels {
		f()
	}
	sc.swapCond.Broadcast()
	sc.activeCtx = sc.activeCtx[:0]
	sc.activeCancels = sc.activeCancels[:0]
	return
}

// Restart continues the queue and blocks until sender is running
func (sc *StatsCoord) Restart() error {
	select {
	case <-sc.closed:
		return fmt.Errorf("StatsCoord is closed")
	default:
	}
	sc.sq.Start()
	done := make(chan struct{})
	go func() {
		ctx, _ := sc.newThreadCtx()
		close(done)
		sc.runIssuer(ctx)
	}()
	// only return after latestCtx updated
	<-done
	return nil
}

func (sc *StatsCoord) runIssuer(ctx context.Context) (err error) {
	var gcKv *memStats
	gcTicker := time.NewTicker(sc.gcInterval)
	for {
		cycleCtx, err := sc.newCycle(ctx)
		if err != nil {
			return err
		}

		select {
		case <-gcTicker.C:
			sc.setDoGc()
		default:
		}

		genStart := sc.genCnt.Load()
		genCand := sc.genCand.Add(1)
		gcKv = nil
		if sc.gcIsSet() {
			gcKv = NewMemStats()
			gcKv.gcGen = genCand
		}

		sqlCtx, err := sc.ctxGen(cycleCtx)
		if err != nil {
			return err
		}

		newStats, err := sc.newStatsForRoot(sqlCtx, gcKv)
		if err != nil {
			sc.descError("", err)
		}

		if ok, err := sc.trySwapStats(ctx, genStart, genCand, newStats, gcKv); err != nil || !ok {
			sc.descError("failed to swap stats", err)
		}
	}
}

func (sc *StatsCoord) trySwapStats(ctx context.Context, prevGen, newGen uint64, newStats *rootStats, gcKv *memStats) (bool, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()

	select {
	case <-ctx.Done():
		return false, context.Cause(ctx)
	default:
	}

	var err error
	if sc.genCnt.CompareAndSwap(prevGen, newGen) {
		// Replace stats and new Kv if no replacements happened
		// in-between.
		sc.swapCond.Broadcast()
		sc.Stats = newStats
		if gcKv != nil {
			// The new KV has all buckets for the latest root stats,
			// background job will to swap the disk location and put
			// entries into a prolly tree.
			if newGen != gcKv.GcGen() {
				return false, fmt.Errorf("gc gen didn't match update gen")
			}
			sc.doGc = false
			sc.gcCnt++
			sc.kv = gcKv
			if !sc.memOnly {
				err = sc.sq.DoAsync(func() error {
					return sc.rotateStorage(ctx)
				})
			}
		}
		// Flush new changes to disk.
		err = sc.sq.DoAsync(func() error {
			_, err := sc.Flush(ctx)
			return err
		})
		return true, err
	}
	return false, nil
}

func (sc *StatsCoord) newStatsForRoot(ctx *sql.Context, gcKv *memStats) (newStats *rootStats, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("serialQueue panicked running work: %s", r)
		}
		if err != nil {
			sc.descError("", err)
		}
	}()

	dSess := dsess.DSessFromSess(ctx.Session)
	dbs := dSess.Provider().AllDatabases(ctx)
	newStats = newRootStats()
	for _, db := range dbs {
		sqlDb, ok := db.(sqle.Database)
		if !ok {
			continue
		}

		var branches []ref.DoltRef
		if err := sc.sq.DoSync(ctx, func() error {
			ddb, ok := dSess.GetDoltDB(ctx, db.Name())
			if !ok {
				return fmt.Errorf("dolt database not found %s", db.Name())
			}
			branches, err = ddb.GetBranches(ctx)
			return err
		}); err != nil {
			return nil, err
		}

		for _, br := range branches {
			sqlDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), br.GetPath(), br.GetPath()+"/"+sqlDb.AliasedName())
			if err != nil {
				sc.descError("revisionForBranch", err)
				continue
			}

			newStats.dbCnt++

			var tableNames []string
			if err := sc.sq.DoSync(ctx, func() error {
				tableNames, err = sqlDb.GetTableNames(ctx)
				return err
			}); err != nil {
				return nil, err
			}

			for _, tableName := range tableNames {
				tableKey, newTableStats, err := sc.updateTable(ctx, tableName, sqlDb, gcKv)
				if err != nil {
					return nil, err
				}
				newStats.stats[tableKey] = newTableStats
			}
		}
	}

	return newStats, nil
}

func (sc *StatsCoord) finalizeHistogram(template stats.Statistic, buckets []*stats.Bucket, firstBound sql.Row) *stats.Statistic {
	template.LowerBnd = firstBound
	for _, b := range buckets {
		// accumulate counts
		template.RowCnt += b.RowCnt
		template.DistinctCnt += b.DistinctCnt
		template.NullCnt += b.NullCnt
		template.Hist = append(template.Hist, b)
	}
	return &template
}

func (sc *StatsCoord) collectIndexNodes(ctx *sql.Context, prollyMap prolly.Map, idxLen int, nodes []tree.Node) ([]*stats.Bucket, sql.Row, error) {
	updater := newBucketBuilder(sql.StatQualifier{}, idxLen, prollyMap.KeyDesc())
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxLen))

	firstNodeHash := nodes[0].HashOf()
	lowerBound, ok := sc.kv.GetBound(firstNodeHash, idxLen)
	if !ok {
		sc.sq.DoSync(ctx, func() error {
			var err error
			lowerBound, err = firstRowForIndex(ctx, idxLen, prollyMap, keyBuilder)
			if err != nil {
				sc.descError("get histogram bucket for node", err)
				return err
			}
			if sc.Debug {
				log.Printf("put bound:  %s: %v\n", firstNodeHash.String()[:5], lowerBound)
			}

			sc.kv.PutBound(firstNodeHash, lowerBound, idxLen)
			return nil
		})
	}

	var offset uint64
	for _, n := range nodes {
		if _, ok, err := sc.GetBucket(ctx, n.HashOf(), keyBuilder); err != nil {
			return nil, nil, err
		} else if ok {
			continue
		}

		treeCnt, err := n.TreeCount()
		if err != nil {
			return nil, nil, err
		}

		err = sc.sq.DoSync(ctx, func() error {
			updater.newBucket()

			// we read exclusive range [node first key, next node first key)
			start, stop := offset, offset+uint64(treeCnt)
			offset += uint64(treeCnt)
			iter, err := prollyMap.IterOrdinalRange(ctx, start, stop)
			if err != nil {
				return err
			}
			for {
				// stats key will be a prefix of the index key
				keyBytes, _, err := iter.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					return err
				}
				// build full key
				for i := range keyBuilder.Desc.Types {
					keyBuilder.PutRaw(i, keyBytes.GetField(i))
				}

				updater.add(keyBuilder.BuildPrefixNoRecycle(prollyMap.Pool(), updater.prefixLen))
				keyBuilder.Recycle()
			}

			// finalize the aggregation
			newBucket, err := updater.finalize(ctx, prollyMap.NodeStore())
			if err != nil {
				return err
			}
			return sc.PutBucket(ctx, n.HashOf(), newBucket, keyBuilder)
		})
		if err != nil {
			return nil, nil, err
		}
	}

	var buckets []*stats.Bucket
	for _, n := range nodes {
		newBucket, ok, err := sc.GetBucket(ctx, n.HashOf(), keyBuilder)
		if err != nil || !ok {
			sc.descError(fmt.Sprintf("missing histogram bucket for node %s", n.HashOf().String()[:5]), err)
			return nil, nil, err
		}
		buckets = append(buckets, newBucket)
	}

	return buckets, lowerBound, nil
}

func (sc *StatsCoord) updateTable(ctx *sql.Context, tableName string, sqlDb dsess.SqlDatabase, gcKv *memStats) (tableIndexesKey, []*stats.Statistic, error) {
	var err error
	var sqlTable *sqle.DoltTable
	var dTab *doltdb.Table
	if err := sc.sq.DoSync(ctx, func() error {
		sqlTable, dTab, err = GetLatestTable(ctx, tableName, sqlDb)
		return err
	}); err != nil {
		return tableIndexesKey{}, nil, err
	}

	tableKey := tableIndexesKey{
		db:     sqlDb.AliasedName(),
		branch: sqlDb.Revision(),
		table:  tableName,
		schema: "",
	}

	var indexes []sql.Index
	if err := sc.sq.DoSync(ctx, func() error {
		indexes, err = sqlTable.GetIndexes(ctx)
		return err
	}); err != nil {
		return tableIndexesKey{}, nil, err
	}

	var newTableStats []*stats.Statistic
	for _, sqlIdx := range indexes {
		var idx durable.Index
		var err error
		if strings.EqualFold(sqlIdx.ID(), "PRIMARY") {
			idx, err = dTab.GetRowData(ctx)
		} else {
			idx, err = dTab.GetIndexRowData(ctx, sqlIdx.ID())
		}
		if err != nil {
			sc.descError("GetRowData", err)
			continue
		}

		var template stats.Statistic
		if err := sc.sq.DoSync(ctx, func() error {
			_, template, err = sc.getTemplate(ctx, sqlTable, sqlIdx)
			if err != nil {
				return fmt.Errorf("stats collection failed to generate a statistic template: %s.%s.%s:%T; %s", sqlDb.RevisionQualifiedName(), tableName, sqlIdx, sqlIdx, err.Error())
			}
			return nil
		}); err != nil {
			return tableIndexesKey{}, nil, err
		} else if template.Fds.Empty() {
			return tableIndexesKey{}, nil, fmt.Errorf("failed to creat template for %s/%s/%s/%s", sqlDb.Revision(), sqlDb.AliasedName(), tableName, sqlIdx.ID())
		}

		template.Qual.Database = sqlDb.AliasedName()

		idxLen := len(sqlIdx.Expressions())

		prollyMap := durable.ProllyMapFromIndex(idx)
		var levelNodes []tree.Node
		if err := sc.sq.DoSync(ctx, func() error {
			levelNodes, err = tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
			return err
		}); err != nil {
			return tableIndexesKey{}, nil, err
		}
		var buckets []*stats.Bucket
		var firstBound sql.Row
		if len(levelNodes) > 0 {
			buckets, firstBound, err = sc.collectIndexNodes(ctx, prollyMap, idxLen, levelNodes)
			if err != nil {
				sc.descError("", err)
				continue
			}
		}

		newTableStats = append(newTableStats, sc.finalizeHistogram(template, buckets, firstBound))

		if gcKv != nil {
			keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxLen))
			if !gcKv.GcMark(sc.kv, levelNodes, buckets, idxLen, keyBuilder) {
				return tableIndexesKey{}, nil, fmt.Errorf("GC interrupted updated")
			}
			schHash, _, err := sqlTable.IndexCacheKey(ctx)
			if err != nil {
				return tableIndexesKey{}, nil, err
			}
			key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
			if t, ok := sc.GetTemplate(key); ok {
				gcKv.PutTemplate(key, t)
			}
		}
	}
	return tableKey, newTableStats, nil
}
