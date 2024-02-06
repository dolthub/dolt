// Copyright 2024 Dolthub, Inc.
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

package stats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	types2 "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

const asyncAutoRefreshStats = "async_auto_refresh_stats"

func (p *Provider) Configure(ctx context.Context, ctxFactory func(ctx context.Context) (*sql.Context, error), bThreads *sql.BackgroundThreads, pro *sqle.DoltDatabaseProvider, dbs []dsess.SqlDatabase) error {
	p.SetStarter(NewInitDatabaseHook(p, ctxFactory, bThreads, nil))

	if _, disabled, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsMemoryOnly); disabled == int8(1) {
		return nil
	}

	loadCtx, err := ctxFactory(ctx)
	if err != nil {
		return err
	}
	if err := p.Load(loadCtx, dbs); err != nil {
		return err
	}
	if _, enabled, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshEnabled); enabled == int8(1) {
		_, threshold, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshThreshold)
		_, interval, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshInterval)
		interval64, _, _ := types2.Int64.Convert(interval)
		intervalSec := time.Second * time.Duration(interval64.(int64))
		thresholdf64 := threshold.(float64)

		for _, db := range dbs {
			if err := p.InitAutoRefresh(ctxFactory, db.Name(), bThreads, intervalSec, thresholdf64); err != nil {
				return err
			}
		}
		pro.InitDatabaseHook = NewInitDatabaseHook(p, ctxFactory, bThreads, pro.InitDatabaseHook)
		pro.DropDatabaseHook = NewDropDatabaseHook(p, ctxFactory, pro.DropDatabaseHook)
	}
	return nil
}

func (p *Provider) InitAutoRefresh(ctxFactory func(ctx context.Context) (*sql.Context, error), dbName string, bThreads *sql.BackgroundThreads, checkInterval time.Duration, updateThresh float64) error {
	// this is only called after initial statistics are finished loading
	// launch a thread that periodically checks freshness

	// retain handle to cancel on drop database
	// todo: add Cancel(name) to sql.BackgroundThreads interface
	p.mu.Lock()
	defer p.mu.Unlock()

	dropDbCtx, dbStatsCancel := context.WithCancel(context.Background())
	p.cancelers[dbName] = dbStatsCancel

	return bThreads.Add(fmt.Sprintf("%s_%s", asyncAutoRefreshStats, dbName), func(ctx context.Context) {
		timer := time.NewTimer(checkInterval)
		for {
			// wake up checker on interval
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-dropDbCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
				sqlCtx, err := ctxFactory(ctx)
				if err != nil {
					return
				}

				sqlCtx.GetLogger().Debugf("starting statistics refresh check for '%s': %s", dbName, time.Now().String())
				timer.Reset(checkInterval)

				// Iterate all dbs, tables, indexes. Each db will collect
				// []indexMeta above refresh threshold. We read and process those
				// chunks' statistics. We merge updated chunks with precomputed
				// chunks. The full set of statistics for each database lands
				// 1) in the provider's most recent set of database statistics, and
				// 2) on disk in the database's statistics ref'd prolly.Map.
				curStats := p.getStats(dbName)
				if curStats == nil {
					curStats = newDbStats(dbName)
				}

				newStats := make(map[sql.StatQualifier]*DoltStats)
				var deletedStats []sql.StatQualifier
				qualExists := make(map[sql.StatQualifier]bool)
				tableExistsAndSkipped := make(map[string]bool)

				// important: update session references every loop
				dSess := dsess.DSessFromSess(sqlCtx.Session)
				prov := dSess.Provider()
				ddb, ok := dSess.GetDoltDB(sqlCtx, dbName)
				if !ok {
					sqlCtx.GetLogger().Debugf("statistics refresh error: database not found %s", dbName)
				}

				sqlDb, err := prov.Database(sqlCtx, dbName)
				if err != nil {
					sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
					continue
				}

				tables, err := sqlDb.GetTableNames(sqlCtx)
				if err != nil {
					sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
					continue
				}

				for _, table := range tables {
					sqlTable, ok, err := sqlDb.GetTableInsensitive(sqlCtx, table)
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}
					if !ok {
						sqlCtx.GetLogger().Debugf("statistics refresh error: table not found %s", table)
						continue
					}

					var dTab *doltdb.Table
					switch t := sqlTable.(type) {
					case *sqle.AlterableDoltTable:
						dTab, err = t.DoltTable.DoltTable(sqlCtx)
					case *sqle.WritableDoltTable:
						dTab, err = t.DoltTable.DoltTable(sqlCtx)
					case *sqle.DoltTable:
						dTab, err = t.DoltTable(sqlCtx)
					default:
						err = fmt.Errorf("failed to unwrap dolt table from type: %T", sqlTable)
					}
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}

					tableHash, err := dTab.GetRowDataHash(ctx)
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}

					if curStats.getLatestHash(table) == tableHash {
						// no data changes since last check
						tableExistsAndSkipped[table] = true
						sqlCtx.GetLogger().Debugf("statistics refresh: table hash unchanged since last check: %s", tableHash)
						continue
					} else {
						sqlCtx.GetLogger().Debugf("statistics refresh: new table hash: %s", tableHash)
					}

					iat, ok := sqlTable.(sql.IndexAddressableTable)
					if !ok {
						sqlCtx.GetLogger().Debugf("statistics refresh error: table does not support indexes %s", table)
						continue
					}

					indexes, err := iat.GetIndexes(sqlCtx)
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}

					// collect indexes and ranges to be updated
					var idxMetas []indexMeta
					for _, index := range indexes {
						qual := sql.NewStatQualifier(dbName, table, strings.ToLower(index.ID()))
						qualExists[qual] = true
						curStat := curStats.getIndexStats(qual)
						if curStat == nil {
							curStat = NewDoltStats()
							curStat.Qual = qual

							cols := make([]string, len(index.Expressions()))
							tablePrefix := fmt.Sprintf("%s.", table)
							for i, c := range index.Expressions() {
								cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
							}
							curStat.Columns = cols
						}
						sqlCtx.GetLogger().Debugf("statistics refresh index: %s", qual.String())

						updateMeta, err := newIdxMeta(sqlCtx, curStat, dTab, index, curStat.Columns)
						if err != nil {
							sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
							continue
						}
						curCnt := float64(len(curStat.active))
						updateCnt := float64(len(updateMeta.updateChunks))
						deleteCnt := float64(len(curStat.active) - len(updateMeta.preexisting))
						sqlCtx.GetLogger().Debugf("statistics current: %d, new: %d, delete: %d", int(curCnt), int(updateCnt), int(deleteCnt))

						if curCnt == 0 || (deleteCnt+updateCnt)/curCnt > updateThresh {
							sqlCtx.GetLogger().Debugf("statistics updating: %s", updateMeta.qual)
							// mark index for updating
							idxMetas = append(idxMetas, updateMeta)
							// update lastest hash if we haven't already
							curStats.setLatestHash(table, tableHash)
						}
					}
					// get new buckets for index chunks to update
					newTableStats, err := updateStats(sqlCtx, sqlTable, dTab, indexes, idxMetas)
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}

					// merge new chunks with preexisting chunks
					for _, updateMeta := range idxMetas {
						stat := newTableStats[updateMeta.qual]
						if stat != nil {
							newStats[updateMeta.qual] = mergeStatUpdates(stat, updateMeta)
						}
					}
				}

				func() {
					curStats.mu.Lock()
					defer curStats.mu.Unlock()
					for _, s := range curStats.stats {
						// table or index delete leaves hole in stats
						// this is separate from threshold check
						if !tableExistsAndSkipped[s.Qual.Table()] && !qualExists[s.Qual] {
							// only delete stats we've verified are deleted
							deletedStats = append(deletedStats, s.Qual)
						}
					}
				}()

				prevMap := curStats.getCurrentMap()
				if prevMap.KeyDesc().Count() == 0 {
					kd, vd := schema.StatsTableDoltSchema.GetMapDescriptors()
					prevMap, err = prolly.NewMapFromTuples(ctx, ddb.NodeStore(), kd, vd)
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}
				}

				if len(deletedStats) == 0 && len(newStats) == 0 {
					continue
				}

				if len(deletedStats) > 0 {
					sqlCtx.GetLogger().Debugf("statistics refresh: deleting stats %#v", deletedStats)
				}
				delMap, err := deleteStats(sqlCtx, prevMap, deletedStats...)
				if err != nil {
					sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
					continue
				}

				newMap, err := flushStats(sqlCtx, delMap, newStats)
				if err != nil {
					sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
					continue
				}

				curStats.setCurrentMap(newMap)
				for q, s := range newStats {
					curStats.setIndexStats(q, s)
				}
				p.setStats(dbName, curStats)
				err = ddb.SetStatisics(ctx, newMap.HashOf())
				if err != nil {
					sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
					continue
				}
			}
		}
	})
}

func newIdxMeta(ctx *sql.Context, curStats *DoltStats, doltTable *doltdb.Table, sqlIndex sql.Index, cols []string) (indexMeta, error) {
	var idx durable.Index
	var err error
	if strings.EqualFold(sqlIndex.ID(), "PRIMARY") {
		idx, err = doltTable.GetRowData(ctx)
	} else {
		idx, err = doltTable.GetIndexRowData(ctx, sqlIndex.ID())
	}
	if err != nil {
		return indexMeta{}, err
	}

	prollyMap := durable.ProllyMapFromIndex(idx)

	// get newest histogram target level hashes
	levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
	if err != nil {
		return indexMeta{}, err
	}

	var addrs []hash.Hash
	var preservedStats []DoltBucket
	var missingAddrs float64
	var missingChunks []tree.Node
	var missingOffsets [][]uint64
	var offset uint64
	for _, n := range levelNodes {
		// Compare the previous histogram chunks to the newest tree chunks.
		// Partition the newest chunks into 1) preserved or 2) missing.
		// Missing chunks will need to be scanned on a stats update, so
		// track the (start, end) ordinal offsets to simplify the read iter.
		treeCnt, err := n.TreeCount()
		if err != nil {
			return indexMeta{}, err
		}

		addrs = append(addrs, n.HashOf())
		if bucketIdx, ok := curStats.active[n.HashOf()]; !ok {
			missingChunks = append(missingChunks, n)
			missingOffsets = append(missingOffsets, []uint64{offset, offset + uint64(treeCnt)})
			missingAddrs++
		} else {
			preservedStats = append(preservedStats, curStats.Histogram[bucketIdx])
		}
		offset += uint64(treeCnt)
	}
	return indexMeta{
		qual:           curStats.Qual,
		cols:           cols,
		updateChunks:   missingChunks,
		updateOrdinals: missingOffsets,
		preexisting:    preservedStats,
		allAddrs:       addrs,
	}, nil
}
