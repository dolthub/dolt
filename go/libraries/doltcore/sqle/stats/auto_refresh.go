package stats

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
	"time"
)

const asyncAutoRefreshStats = "async_auto_refresh_stats"

func (p *Provider) ConfigureAutoRefresh(bThreads *sql.BackgroundThreads, checkInterval time.Duration, updateThresh float64) error {
	// this is only called after initial statistics are finished loading
	// launch a thread that periodically checks freshness
	return bThreads.Add(asyncAutoRefreshStats, func(ctx context.Context) {
		sqlCtx := sql.NewContext(ctx)
		for now := range time.Tick(checkInterval) {
			sqlCtx.GetLogger().Debugf("starting statistics refresh check: %s", now.String())

			dSess := dsess.DSessFromSess(sqlCtx.Session)
			prov := dSess.Provider()

			var newStats map[sql.StatQualifier]*DoltStats

			for db, curStats := range p.dbStats {
				ddb, ok := dSess.GetDoltDB(sqlCtx, db)
				if !ok {
					sqlCtx.GetLogger().Debugf("statistics refresh error: database not found %s", db)
					continue
				}

				sqlDb, err := prov.Database(sqlCtx, db)
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
						continue
					}
					p.mu.Lock()
					curStats.latestTableHashes[table] = tableHash
					p.mu.Unlock()

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

					var idxMetas []indexMeta
					for _, index := range indexes {

						qual := sql.NewStatQualifier(db, table, strings.ToLower(index.ID()))
						curHist := curStats.stats[qual]

						var idx durable.Index
						var err error
						if strings.EqualFold(index.ID(), "PRIMARY") {
							idx, err = dTab.GetRowData(ctx)
						} else {
							idx, err = dTab.GetIndexRowData(ctx, index.ID())
						}
						if err != nil {
							sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
							continue
						}

						prollyMap := durable.ProllyMapFromIndex(idx)

						// get histogram level hashes
						levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
						if err != nil {
							sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
							continue
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
								sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
								continue
							}

							addrs = append(addrs, n.HashOf())
							if bucketIdx, ok := curStats.active[n.HashOf()]; !ok {
								missingChunks = append(missingChunks, n)
								missingOffsets = append(missingOffsets, []uint64{offset, offset + uint64(treeCnt)})
								missingAddrs++
							} else {
								preservedStats = append(preservedStats, curHist.Histogram[bucketIdx])
							}
							offset += uint64(treeCnt)
						}

						if missingAddrs/float64(len(curStats.active)) > updateThresh {
							// trigger refresh
							// todo indexMeta should have the missing chunks attached?
							// updateStats(sqlCtx, sqlTable, dTab, missingAddrs)
							idxMetas = append(idxMetas, indexMeta{
								db:             db,
								table:          table,
								index:          strings.ToLower(index.ID()),
								updateChunks:   missingChunks,
								updateOrdinals: missingOffsets,
								preexisting:    preservedStats,
								allAddrs:       addrs,
							})
						}
					}
					newTableStats, err := updateStats(sqlCtx, sqlTable, dTab, indexes, idxMetas)
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}

					// merge old and new buckets
					for qual, updateStats := range newTableStats {
						var oldHist []DoltBucket
						var bucketOrder []hash.Hash
						for _, idxMeta := range idxMetas {
							if strings.EqualFold(idxMeta.db, qual.Db()) && strings.EqualFold(idxMeta.table, qual.Table()) && strings.EqualFold(idxMeta.index, qual.Index()) {
								oldHist = idxMeta.preexisting
								bucketOrder = idxMeta.allAddrs
								break
							}
						}
						updateHist := updateStats.Histogram

						var mergeHist DoltHistogram
						var i, j int
						for _, chunkAddr := range bucketOrder {
							if i < len(oldHist) && oldHist[i].Chunk == chunkAddr {
								mergeHist = append(mergeHist, oldHist[i])
								i++
							} else if j < len(updateHist) && updateHist[j].Chunk == chunkAddr {
								mergeHist = append(mergeHist, updateHist[i])
								j++
							}
						}
						updateStats.Histogram = mergeHist
						// todo update counts
						//newStats[qual] =

						newStats[qual] = updateStats
					}
				}

				prevMap := p.dbStats[db].currentMap
				if prevMap.KeyDesc().Count() == 0 {
					kd, vd := schema.StatsTableDoltSchema.GetMapDescriptors()
					prevMap, err = prolly.NewMapFromTuples(ctx, ddb.NodeStore(), kd, vd)
					if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
						continue
					}
				}
				newMap, err := flushStats(sqlCtx, prevMap, newStats)
				if err != nil {
					sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
					continue
				}

				p.mu.Lock()
				p.dbStats[db].currentMap = newMap
				p.mu.Unlock()
			}
		}
	})
}
