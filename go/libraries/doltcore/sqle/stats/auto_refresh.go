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

			// Iterate all dbs, tables, indexes. Each db will collect
			// []indexMeta above refresh threshold. We read and process those
			// chunks' statistics. We merge updated chunks with precomputed
			// chunks. The full set of statistics for each database lands
			// 1) in the provider's most recent set of database statistics, and
			// 2) on disk in the database's statistics ref'd prolly.Map.
			for db, curStats := range p.dbStats {
				var newStats map[sql.StatQualifier]*DoltStats

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
						qual := sql.NewStatQualifier(db, table, strings.ToLower(index.ID()))
						curStat := curStats.stats[qual]
						idxMeta, err := newIdxMeta(sqlCtx, curStat, dTab, index, curStat.Columns)
						if err != nil {
							sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
							continue
						}
						if float64(len(idxMeta.updateChunks))/float64(len(curStat.active)) > updateThresh {
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
						newStats[idxMeta.qual] = mergeStatUpdates(stat, idxMeta)
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
				err = ddb.SetStatisics(ctx, newMap.HashOf())
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
		db:             curStats.Qual.Db(),
		table:          curStats.Qual.Table(),
		index:          curStats.Qual.Index(),
		qual:           curStats.Qual,
		cols:           cols,
		updateChunks:   missingChunks,
		updateOrdinals: missingOffsets,
		preexisting:    preservedStats,
		allAddrs:       addrs,
	}, nil
}
