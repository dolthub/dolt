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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

const asyncAutoRefreshStats = "async_auto_refresh_stats"

func (p *Provider) ConfigureAutoRefresh(ctxFactory func(ctx context.Context) (*sql.Context, error), dbName string, ddb *doltdb.DoltDB, prov sql.DatabaseProvider, bThreads *sql.BackgroundThreads, checkInterval time.Duration, updateThresh float64) error {
	// this is only called after initial statistics are finished loading
	// launch a thread that periodically checks freshness

	// retain handle to cancel on drop database
	// todo: add Cancel(name) to sql.BackgroundThreads interface
	dropDbCtx, dbStatsCancel := context.WithCancel(context.Background())
	p.autoRefreshCancel[dbName] = dbStatsCancel

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
				curStats, ok := p.dbStats[dbName]
				if !ok {
					curStats = newDbStats(dbName)
					p.dbStats[dbName] = curStats
				}

				newStats := make(map[sql.StatQualifier]*DoltStats)
				var deletedStats []sql.StatQualifier
				qualExists := make(map[sql.StatQualifier]bool)
				tableExistsAndSkipped := make(map[string]bool)

				sqlDb, err := prov.Database(sqlCtx, dbName)
				if err != nil {
					sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
					continue
				}

				// for each table
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

					if curStats.latestTableHashes[table] == tableHash {
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
						curStat := curStats.stats[qual]
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

						idxMeta, err := newIdxMeta(sqlCtx, curStat, dTab, index, curStat.Columns)
						if err != nil {
							sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
							continue
						}
						curCnt := float64(len(curStat.active))
						updateCnt := float64(len(idxMeta.updateChunks))
						deleteCnt := float64(len(curStat.active) - len(idxMeta.preexisting))
						sqlCtx.GetLogger().Debugf("statistics current: %d, new: %d, delete: %d", int(curCnt), int(updateCnt), int(deleteCnt))

						if curCnt == 0 || (deleteCnt+updateCnt)/curCnt > updateThresh {
							sqlCtx.GetLogger().Debugf("statistics updating: %s", idxMeta.qual)
							// mark index for updating
							idxMetas = append(idxMetas, idxMeta)
							// update lastest hash if we haven't already
							curStats.latestTableHashes[table] = tableHash
						}
					}
					// get new buckets for index chunks to update
					newTableStats, err := updateStats(sqlCtx, sqlTable, dTab, indexes, idxMetas)
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}

					// merge new chunks with preexisting chunks
					for _, idxMeta := range idxMetas {
						stat := newTableStats[idxMeta.qual]
						if stat != nil {
							newStats[idxMeta.qual] = mergeStatUpdates(stat, idxMeta)
						}
					}
				}

				for _, s := range curStats.stats {
					// table or index delete leaves hole in stats
					// this is separate from threshold check
					if !tableExistsAndSkipped[s.Qual.Table()] && !qualExists[s.Qual] {
						// only delete stats we've verified are deleted
						deletedStats = append(deletedStats, s.Qual)
					}
				}

				prevMap := curStats.currentMap
				if prevMap.KeyDesc().Count() == 0 {
					kd, vd := schema.StatsTableDoltSchema.GetMapDescriptors()
					prevMap, err = prolly.NewMapFromTuples(ctx, ddb.NodeStore(), kd, vd)
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}
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

				p.mu.Lock()
				p.dbStats[dbName].currentMap = newMap
				err = ddb.SetStatisics(ctx, newMap.HashOf())
				for q, s := range newStats {
					p.dbStats[dbName].stats[q] = s
				}
				p.mu.Unlock()
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

	// get histogram level hashes
	levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
	if err != nil {
		return indexMeta{}, err
	}

	var addrs []hash.Hash
	var missingAddrs float64
	var missingChunks []tree.Node
	var preservedStats []DoltBucket
	var missingOffsets [][]uint64
	var offset uint64
	for _, n := range levelNodes {
		// check if hash is in current list
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
