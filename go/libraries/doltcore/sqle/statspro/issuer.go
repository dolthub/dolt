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
	"github.com/dolthub/dolt/go/store/hash"
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

func (sc *StatsController) CollectOnce(ctx context.Context) (string, error) {
	genStart := sc.genCnt.Load()
	newStats, err := sc.newStatsForRoot(ctx, nil)
	if errors.Is(err, context.Canceled) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	if ok, err := sc.trySwapStats(ctx, genStart, newStats, nil); err != nil || !ok {
		return "", err
	}
	return newStats.String(), nil
}

func (sc *StatsController) runIssuer(ctx context.Context) (err error) {
	var gcKv *memStats
	var newStats *rootStats
	gcTicker := time.NewTicker(sc.gcInterval)
	for {
		// This loops tries to update stats as long as context
		// is active. Thread contexts governs who "owns" the update
		// process. The generation counters ensure atomic swapping.

		gcKv = nil
		genStart := sc.genCnt.Load()

		select {
		case <-gcTicker.C:
			sc.setDoGc()
		default:
		}

		if sc.gcIsSet() {
			gcKv = NewMemStats()
			gcKv.gcGen = genStart
		}

		newStats, err = sc.newStatsForRoot(ctx, gcKv)
		if errors.Is(err, context.Canceled) {
			log.Printf("stats context cancelled")
			return nil
		} else if err != nil {
			sc.descError("", err)
		}

		if ok, err := sc.trySwapStats(ctx, genStart, newStats, gcKv); err != nil {
			if !ok {
				sc.descError("failed to swap stats", err)
			} else {
				sc.descError("swapped stats with flush failure", err)
			}
		}

		select {
		case <-ctx.Done():
			// is double check necessary?
			log.Printf("stats context cancelled")
			return context.Cause(ctx)
		default:
		}

	}
}

func (sc *StatsController) trySwapStats(ctx context.Context, prevGen uint64, newStats *rootStats, gcKv *memStats) (ok bool, err error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	signal := leSwap
	defer func() {
		if ok {
			sc.signalListener(signal)
		}
	}()

	if sc.genCnt.CompareAndSwap(prevGen, prevGen+1) {
		// Replace stats and new Kv if no replacements happened
		// in-between.
		sc.Stats = newStats
		if gcKv != nil {
			signal |= leGc
			// The new KV has all buckets for the latest root stats,
			// background job will to swap the disk location and put
			// entries into a prolly tree.
			if prevGen != gcKv.GcGen() {
				err = fmt.Errorf("gc gen didn't match update gen")
				return
			}
			sc.doGc = false
			sc.gcCnt++
			sc.kv = gcKv
			ok = true
			if !sc.memOnly {
				sc.mu.Unlock()
				if err = sc.sq.DoSync(ctx, func() error {
					return sc.rotateStorage(ctx)
				}); err != nil {
					return
				}
				sc.mu.Lock()
			}
		}
		// Flush new changes to disk, unlocked
		if !sc.memOnly {
			sc.mu.Unlock()
			err = sc.sq.DoSync(ctx, func() error {
				_, err := sc.Flush(ctx)
				return err
			})
			sc.mu.Lock()
			if err != nil {
				return true, err
			}
		}
		signal = signal | leFlush
		return true, nil
	}
	return false, nil
}

func (sc *StatsController) newStatsForRoot(baseCtx context.Context, gcKv *memStats) (newStats *rootStats, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("issuer panicked running work: %s", r)
		}
		if err != nil {
			sc.descError("stats update interrupted", err)
		}
	}()

	ctx, err := sc.ctxGen(baseCtx)
	if err != nil {
		return nil, err
	}
	
	sql.SessionCommandBegin(ctx.Session)
	defer sql.SessionCommandEnd(ctx.Session)
	defer sql.SessionEnd(ctx.Session)

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
				return fmt.Errorf("get dolt db dolt database not found %s", db.Name())
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

			newStats.DbCnt++

			var tableNames []string
			if err := sc.sq.DoSync(ctx, func() error {
				tableNames, err = sqlDb.GetTableNames(ctx)
				return err
			}); err != nil {
				return nil, err
			}

			for _, tableName := range tableNames {
				err := sc.updateTable(ctx, newStats, tableName, sqlDb, gcKv)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return newStats, nil
}

func (sc *StatsController) preexistingStats(k tableIndexesKey, h hash.Hash) ([]*stats.Statistic, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.Stats.hashes[k].Equal(h) {
		return sc.Stats.stats[k], true
	}
	return nil, false
}

func (sc *StatsController) finalizeHistogram(template stats.Statistic, buckets []*stats.Bucket, firstBound sql.Row) *stats.Statistic {
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

func (sc *StatsController) collectIndexNodes(ctx *sql.Context, prollyMap prolly.Map, idxLen int, nodes []tree.Node) ([]*stats.Bucket, sql.Row, int, error) {
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

	var writes int
	var offset uint64
	for _, n := range nodes {
		treeCnt, err := n.TreeCount()
		if err != nil {
			return nil, nil, 0, err
		}
		start, stop := offset, offset+uint64(treeCnt)
		offset = stop

		if _, ok, err := sc.GetBucket(ctx, n.HashOf(), keyBuilder); err != nil {
			return nil, nil, 0, err
		} else if ok {
			continue
		}

		writes++
		err = sc.sq.DoSync(ctx, func() error {
			updater.newBucket()

			// we read exclusive range [node first key, next node first key)
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
			return nil, nil, 0, err
		}
	}

	var buckets []*stats.Bucket
	for _, n := range nodes {
		newBucket, ok, err := sc.GetBucket(ctx, n.HashOf(), keyBuilder)
		if err != nil || !ok {
			sc.descError(fmt.Sprintf("missing histogram bucket for node %s", n.HashOf().String()[:5]), err)
			return nil, nil, 0, err
		}
		buckets = append(buckets, newBucket)
	}

	return buckets, lowerBound, writes, nil
}

func (sc *StatsController) updateTable(ctx *sql.Context, newStats *rootStats, tableName string, sqlDb dsess.SqlDatabase, gcKv *memStats) error {
	var err error
	var sqlTable *sqle.DoltTable
	var dTab *doltdb.Table
	if err := sc.sq.DoSync(ctx, func() error {
		sqlTable, dTab, err = GetLatestTable(ctx, tableName, sqlDb)
		return err
	}); err != nil {
		return err
	}

	tableKey := tableIndexesKey{
		db:     strings.ToLower(sqlDb.AliasedName()),
		branch: strings.ToLower(sqlDb.Revision()),
		table:  strings.ToLower(tableName),
		schema: "",
	}

	tableHash, err := dTab.HashOf()
	if err != nil {
		return err
	}
	if gcKv == nil {
		if stats, ok := sc.preexistingStats(tableKey, tableHash); ok {
			newStats.stats[tableKey] = stats
			newStats.hashes[tableKey] = tableHash
			newStats.TablesSkipped++
			return nil
		}
	}

	var indexes []sql.Index
	if err := sc.sq.DoSync(ctx, func() error {
		indexes, err = sqlTable.GetIndexes(ctx)
		return err
	}); err != nil {
		return err
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
			return err
		} else if template.Fds.Empty() {
			return fmt.Errorf("failed to creat template for %s/%s/%s/%s", sqlDb.Revision(), sqlDb.AliasedName(), tableName, sqlIdx.ID())
		}

		template.Qual.Database = sqlDb.AliasedName()

		idxLen := len(sqlIdx.Expressions())

		prollyMap := durable.ProllyMapFromIndex(idx)
		var levelNodes []tree.Node
		if err = sc.sq.DoSync(ctx, func() error {
			levelNodes, err = tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
			if err != nil {
				sc.descError("get level", err)
			}
			return err
		}); err != nil {
			return err
		}
		var buckets []*stats.Bucket
		var firstBound sql.Row
		if len(levelNodes) > 0 {
			var writes int
			buckets, firstBound, writes, err = sc.collectIndexNodes(ctx, prollyMap, idxLen, levelNodes)
			if err != nil {
				sc.descError("", err)
				continue
			}
			newStats.BucketWrites += writes
		}

		newTableStats = append(newTableStats, sc.finalizeHistogram(template, buckets, firstBound))

		if gcKv != nil {
			keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxLen))
			if !gcKv.GcMark(sc.kv, levelNodes, buckets, idxLen, keyBuilder) {
				return fmt.Errorf("GC interrupted updated")
			}
			schHash, _, err := sqlTable.IndexCacheKey(ctx)
			if err != nil {
				return err
			}
			key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
			if t, ok := sc.GetTemplate(key); ok {
				gcKv.PutTemplate(key, t)
			}
		}
	}
	newStats.stats[tableKey] = newTableStats
	newStats.hashes[tableKey] = tableHash
	newStats.TablesProcessed++
	return nil
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

type templateCacheKey struct {
	h       hash.Hash
	idxName string
}

func (k templateCacheKey) String() string {
	return k.idxName + "/" + k.h.String()[:5]
}

func (sc *StatsController) getTemplate(ctx *sql.Context, sqlTable *sqle.DoltTable, sqlIdx sql.Index) (templateCacheKey, stats.Statistic, error) {
	schHash, _, err := sqlTable.IndexCacheKey(ctx)
	if err != nil {
		return templateCacheKey{}, stats.Statistic{}, err
	}
	key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
	if template, ok := sc.GetTemplate(key); ok {
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

	// xxx: the lower here is load bearing, index comparison
	// expects the expressions to be stripped of table name.
	tablePrefix := strings.ToLower(sqlTable.Name()) + "."
	cols := make([]string, len(sqlIdx.Expressions()))
	for i, c := range sqlIdx.Expressions() {
		cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
	}

	template := stats.Statistic{
		Qual:     sql.NewStatQualifier("", "", sqlTable.Name(), sqlIdx.ID()),
		Cols:     cols,
		Typs:     types,
		IdxClass: uint8(class),
		Fds:      fds,
		Colset:   colset,
	}

	// We put template twice, once for schema changes with no data
	// changes (here), and once when we put chunks to avoid GC dropping
	// templates before the finalize job.
	sc.PutTemplate(key, template)

	return key, template, nil
}
