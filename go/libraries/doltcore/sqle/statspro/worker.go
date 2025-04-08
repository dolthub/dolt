// Copyright 2025 Dolthub, Inc.
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
	"io"
	"log"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const collectBatchSize = 20

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

func (sc *StatsController) runWorker(ctx context.Context) (err error) {
	var gcKv *memStats
	var newStats *rootStats
	var lastSuccessfulStats *rootStats
	gcTicker := time.NewTicker(sc.gcInterval)
	for {
		// This loops tries to update stats as long as context
		// is active. Thread contexts governs who "owns" the update
		// process. The generation counters ensure atomic swapping.

		gcKv = nil
		genStart := sc.genCnt.Load()

		select {
		case <-gcTicker.C:
			sc.setDoGc(false)
		default:
		}

		if sc.gcIsSet() {
			gcKv = NewMemStats()
			gcKv.gcGen = genStart
		}

		newStats, err = sc.newStatsForRoot(ctx, gcKv)
		if errors.Is(err, context.Canceled) {
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
		} else if ok && lastSuccessfulStats != nil && lastSuccessfulStats.hash != newStats.hash {
			lastSuccessfulStats = newStats
			sc.logger.Tracef("stats successful swap: %s\n", newStats.String())
		}

		select {
		case <-ctx.Done():
			// is double check necessary?
			return context.Cause(ctx)
		default:
		}

	}
}

func (sc *StatsController) trySwapStats(ctx context.Context, prevGen uint64, newStats *rootStats, gcKv *memStats) (ok bool, err error) {
	if newStats == nil {
		return false, fmt.Errorf("attempted to place a nil stats object")
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if ctx.Err() != nil {
		// final ctx check in critical section, avoid races on
		// stats after calling stop
		return false, context.Cause(ctx)
	}

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
				func() {
					sc.mu.Unlock()
					defer sc.mu.Lock()
					if err := sc.sq.DoSync(ctx, func() error {
						return sc.rotateStorage(ctx)
					}); err != nil {
						sc.descError("", err)
					}
				}()
			}
		}
		// Flush new changes to disk, unlocked
		if !sc.memOnly {
			func() {
				sc.mu.Unlock()
				defer sc.mu.Lock()
				if err := sc.sq.DoSync(ctx, func() error {
					_, err := sc.Flush(ctx)
					return err
				}); err != nil {
					sc.descError("", err)
				}
			}()
		}
		signal = signal | leFlush
		return true, nil
	}
	return false, nil
}

func (sc *StatsController) newStatsForRoot(baseCtx context.Context, gcKv *memStats) (newStats *rootStats, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("worker panicked running work: %s", r)
		}
		if err != nil {
			sc.descError("stats update interrupted", err)
		}
	}()

	ctx, err := sc.ctxGen(baseCtx)
	if err != nil {
		return nil, err
	}

	defer sql.SessionEnd(ctx.Session)

	dSess := dsess.DSessFromSess(ctx.Session)
	var dbs []sql.Database
	func() {
		sql.SessionCommandBegin(ctx.Session)
		defer sql.SessionCommandEnd(ctx.Session)
		dbs = dSess.Provider().AllDatabases(ctx)
	}()
	newStats = newRootStats()
	digest := xxhash.New()
	for _, db := range dbs {
		sqlDb, ok := db.(sqle.Database)
		if !ok {
			continue
		}

		var branches []ref.DoltRef
		if err := sc.sq.DoSyncSessionAware(ctx, func() (err error) {
			root, err := sqlDb.GetRoot(ctx)
			if err != nil {
				return err
			}
			rootHash, err := root.HashOf()
			if err != nil {
				return err
			}
			digest.Write(rootHash[:])
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
			// this call avoids the chunkstore
			sqlDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), br.GetPath(), br.GetPath()+"/"+sqlDb.AliasedName())
			if err != nil {
				sc.descError("revisionForBranch", err)
				continue
			}

			var schDbs []sql.DatabaseSchema
			if err := sc.sq.DoSyncSessionAware(ctx, func() (err error) {
				schDbs, err = sqlDb.AllSchemas(ctx)
				return err
			}); err != nil {
				sc.descError("getDatabaseSchemas", err)
				continue
			}

			for _, sqlDb := range schDbs {
				switch sqlDb.SchemaName() {
				case "dolt", sql.InformationSchemaDatabaseName, "pg_catalog":
					continue
				}
				var tableNames []string
				if err := sc.sq.DoSyncSessionAware(ctx, func() (err error) {
					tableNames, err = sqlDb.GetTableNames(ctx)
					return err
				}); err != nil {
					sc.descError("getTableNames", err)
					continue
				}

				newStats.DbCnt++

				for _, tableName := range tableNames {
					err := sc.updateTable(ctx, newStats, tableName, sqlDb.(dsess.SqlDatabase), gcKv)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}

	newStats.hash = digest.Sum64()
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
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxLen), prollyMap.NodeStore())

	firstNodeHash := nodes[0].HashOf()
	lowerBound, ok := sc.GetBound(firstNodeHash, idxLen)
	if !ok {
		sc.sq.DoSyncSessionAware(ctx, func() (err error) {
			lowerBound, err = firstRowForIndex(ctx, idxLen, prollyMap, keyBuilder)
			if err != nil {
				return fmt.Errorf("get histogram bucket for node; %w", err)
			}
			if sc.Debug {
				log.Printf("put bound:  %s: %v\n", firstNodeHash.String()[:5], lowerBound)
			}

			sc.PutBound(firstNodeHash, lowerBound, idxLen)
			return nil
		})
	}

	var writes int
	var offset uint64
	for i := 0; i < len(nodes); {
		err := sc.sq.DoSyncSessionAware(ctx, func() (err error) {
			newWrites := 0
			for i < len(nodes) && newWrites < collectBatchSize {
				n := nodes[i]
				i++

				treeCnt, err := n.TreeCount()
				if err != nil {
					return err
				}
				start, stop := offset, offset+uint64(treeCnt)
				offset = stop

				if _, ok, err := sc.GetBucket(ctx, n.HashOf(), keyBuilder); err != nil {
					return err
				} else if ok {
					continue
				}

				writes++
				newWrites++

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

					updater.add(ctx, keyBuilder.BuildPrefixNoRecycle(prollyMap.Pool(), updater.prefixLen))
					keyBuilder.Recycle()
				}

				// finalize the aggregation
				newBucket, err := updater.finalize(ctx, prollyMap.NodeStore())
				if err != nil {
					return err
				}
				if err := sc.PutBucket(ctx, n.HashOf(), newBucket, keyBuilder); err != nil {
					return err
				}
			}
			return nil
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
	if err := sc.sq.DoSyncSessionAware(ctx, func() (err error) {
		sqlTable, dTab, err = GetLatestTable(ctx, tableName, sqlDb)
		return err
	}); err != nil {
		return err
	}

	schemaName := sqlTable.DatabaseSchema().SchemaName()

	tableKey := tableIndexesKey{
		db:     strings.ToLower(sqlDb.AliasedName()),
		branch: strings.ToLower(sqlDb.Revision()),
		table:  strings.ToLower(tableName),
		schema: strings.ToLower(schemaName),
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
	if err := sc.sq.DoSyncSessionAware(ctx, func() (err error) {
		indexes, err = sqlTable.GetIndexes(ctx)
		return err
	}); err != nil {
		return err
	}

	var newTableStats []*stats.Statistic
	for _, sqlIdx := range indexes {
		if sqlIdx.IsSpatial() || sqlIdx.IsFullText() || sqlIdx.IsGenerated() || sqlIdx.IsVector() {
			continue
		}
		var idx durable.Index
		var err error
		var prollyMap prolly.Map
		var template stats.Statistic
		if sc.sq.DoSyncSessionAware(ctx, func() (err error) {
			if strings.EqualFold(sqlIdx.ID(), "PRIMARY") {
				idx, err = dTab.GetRowData(ctx)
			} else {
				idx, err = dTab.GetIndexRowData(ctx, sqlIdx.ID())
			}
			if err == nil {
				prollyMap, err = durable.ProllyMapFromIndex(idx)
				if err != nil {
					return err
				}
			}

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

		var levelNodes []tree.Node
		if err := sc.sq.DoSyncSessionAware(ctx, func() (err error) {
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
			keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxLen), prollyMap.NodeStore())
			if !gcKv.GcMark(sc.kv, levelNodes, buckets, idxLen, keyBuilder) {
				return fmt.Errorf("GC interrupted updated")
			}
			if err := func() error {
				sql.SessionCommandBegin(ctx.Session)
				defer sql.SessionCommandEnd(ctx.Session)
				schHash, _, err := sqlTable.IndexCacheKey(ctx)
				if err != nil {
					return err
				}
				key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
				if t, ok := sc.GetTemplate(key); ok {
					gcKv.PutTemplate(key, t)
				}
				return nil
			}(); err != nil {
				return err
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
