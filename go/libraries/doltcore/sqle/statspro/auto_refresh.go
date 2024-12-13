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

package statspro

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	types2 "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const asyncAutoRefreshStats = "async_auto_refresh_stats"

func (p *Provider) InitAutoRefresh(ctxFactory func(ctx context.Context) (*sql.Context, error), dbName string, bThreads *sql.BackgroundThreads) error {
	_, threshold, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshThreshold)
	_, interval, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsAutoRefreshInterval)
	interval64, _, _ := types2.Int64.Convert(interval)
	intervalSec := time.Second * time.Duration(interval64.(int64))
	thresholdf64 := threshold.(float64)

	ctx, err := ctxFactory(context.Background())
	if err != nil {
		return err
	}

	branches := p.getStatsBranches(ctx)

	return p.InitAutoRefreshWithParams(ctxFactory, dbName, bThreads, intervalSec, thresholdf64, branches)
}

func (p *Provider) InitAutoRefreshWithParams(ctxFactory func(ctx context.Context) (*sql.Context, error), dbName string, bThreads *sql.BackgroundThreads, checkInterval time.Duration, updateThresh float64, branches []string) error {
	// this is only called after initial statistics are finished loading
	// launch a thread that periodically checks freshness

	p.mu.Lock()
	defer p.mu.Unlock()

	dropDbCtx, dbStatsCancel := context.WithCancel(context.Background())
	p.autoCtxCancelers[dbName] = dbStatsCancel

	return bThreads.Add(fmt.Sprintf("%s_%s", asyncAutoRefreshStats, dbName), func(ctx context.Context) {
		ticker := time.NewTicker(checkInterval + time.Nanosecond)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				select {
				case <-dropDbCtx.Done():
					ticker.Stop()
					return
				default:
				}

				sqlCtx, err := ctxFactory(ctx)
				if err != nil {
					return
				}

				dSess := dsess.DSessFromSess(sqlCtx.Session)
				ddb, ok := dSess.GetDoltDB(sqlCtx, dbName)
				if !ok {
					sqlCtx.GetLogger().Debugf("statistics refresh error: database not found %s", dbName)
					return
				}
				for _, branch := range branches {
					if br, ok, err := ddb.HasBranch(ctx, branch); ok {
						sqlCtx.GetLogger().Debugf("starting statistics refresh check for '%s': %s", dbName, time.Now().String())
						// update WORKING session references
						sqlDb, err := dSess.Provider().Database(sqlCtx, p.branchQualifiedDatabase(dbName, branch))
						if err != nil {
							sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
							return
						}

						if err := p.checkRefresh(sqlCtx, sqlDb, dbName, br, updateThresh); err != nil {
							sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
							return
						}
					} else if err != nil {
						sqlCtx.GetLogger().Debugf("statistics refresh error: branch check error %s", err.Error())
					} else {
						sqlCtx.GetLogger().Debugf("statistics refresh error: branch not found %s", br)
					}
				}
			}
		}
	})
}

func (p *Provider) checkRefresh(ctx *sql.Context, sqlDb sql.Database, dbName, branch string, updateThresh float64) error {
	if !p.TryLockForUpdate(branch, dbName, "") {
		return fmt.Errorf("database already being updated: %s/%s", branch, dbName)
	}
	defer p.UnlockTable(branch, dbName, "")

	// Iterate all dbs, tables, indexes. Each db will collect
	// []indexMeta above refresh threshold. We read and process those
	// chunks' statistics. We merge updated chunks with precomputed
	// chunks. The full set of statistics for each database lands
	// 1) in the provider's most recent set of database statistics, and
	// 2) on disk in the database's statistics ref'd prolly.Map.
	statDb, ok := p.getStatDb(dbName)
	if !ok {
		return sql.ErrDatabaseNotFound.New(dbName)
	}

	var deletedStats []sql.StatQualifier
	qualExists := make(map[sql.StatQualifier]bool)
	tableExistsAndSkipped := make(map[string]bool)

	tables, err := sqlDb.GetTableNames(ctx)
	if err != nil {
		return err
	}

	for _, table := range tables {
		if !p.TryLockForUpdate(branch, dbName, table) {
			ctx.GetLogger().Debugf("statistics refresh: table is already being updated: %s/%s.%s", branch, dbName, table)
			return fmt.Errorf("table already being updated: %s", table)
		}
		defer p.UnlockTable(branch, dbName, table)

		sqlTable, dTab, err := GetLatestTable(ctx, table, sqlDb)
		if err != nil {
			return err
		}

		tableHash, err := dTab.GetRowDataHash(ctx)
		if err != nil {
			return err
		}

		if statDb.GetTableHash(branch, table) == tableHash {
			// no data changes since last check
			tableExistsAndSkipped[table] = true
			ctx.GetLogger().Debugf("statistics refresh: table hash unchanged since last check: %s", tableHash)
			continue
		} else {
			ctx.GetLogger().Debugf("statistics refresh: new table hash: %s", tableHash)
		}

		schHash, err := dTab.GetSchemaHash(ctx)
		if err != nil {
			return err
		}

		var schemaName string
		if schTab, ok := sqlTable.(sql.DatabaseSchemaTable); ok {
			schemaName = strings.ToLower(schTab.DatabaseSchema().SchemaName())
		}

		if oldSchHash, err := statDb.GetSchemaHash(ctx, branch, table); oldSchHash.IsEmpty() {
			if err := statDb.SetSchemaHash(ctx, branch, table, schHash); err != nil {
				return err
			}
		} else if oldSchHash != schHash {
			ctx.GetLogger().Debugf("statistics refresh: detected table schema change: %s,%s/%s", dbName, table, branch)
			if err := statDb.SetSchemaHash(ctx, branch, table, schHash); err != nil {
				return err
			}
			stats, err := p.GetTableDoltStats(ctx, branch, dbName, schemaName, table)
			if err != nil {
				return err
			}
			for _, stat := range stats {
				statDb.DeleteStats(ctx, branch, stat.Qualifier())
			}
		} else if err != nil {
			return err
		}

		iat, ok := sqlTable.(sql.IndexAddressableTable)
		if !ok {
			return fmt.Errorf("table does not support indexes %s", table)
		}

		indexes, err := iat.GetIndexes(ctx)
		if err != nil {
			return err
		}

		// collect indexes and ranges to be updated
		var idxMetas []indexMeta
		for _, index := range indexes {
			qual := sql.NewStatQualifier(dbName, schemaName, table, strings.ToLower(index.ID()))
			qualExists[qual] = true
			curStat, ok := statDb.GetStat(branch, qual)
			if !ok {
				curStat = NewDoltStats()
				curStat.Statistic.Qual = qual

				cols := make([]string, len(index.Expressions()))
				tablePrefix := fmt.Sprintf("%s.", table)
				for i, c := range index.Expressions() {
					cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
				}
				curStat.Statistic.Cols = cols
			}
			ctx.GetLogger().Debugf("statistics refresh index: %s", qual.String())

			updateMeta, err := newIdxMeta(ctx, curStat, dTab, index, curStat.Columns())
			if err != nil {
				ctx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
				continue
			}
			curCnt := float64(len(curStat.Active))
			updateCnt := float64(len(updateMeta.newNodes))
			deleteCnt := float64(len(curStat.Active) - len(updateMeta.keepChunks))
			ctx.GetLogger().Debugf("statistics current: %d, new: %d, delete: %d", int(curCnt), int(updateCnt), int(deleteCnt))

			if curCnt == 0 || (deleteCnt+updateCnt)/curCnt > updateThresh {
				if curCnt == 0 && updateCnt == 0 {
					continue
				}
				ctx.GetLogger().Debugf("statistics updating: %s", updateMeta.qual)
				// mark index for updating
				idxMetas = append(idxMetas, updateMeta)
				// update latest hash if we haven't already
				statDb.SetTableHash(branch, table, tableHash)
			}
		}

		// get new buckets for index chunks to update
		newTableStats, err := createNewStatsBuckets(ctx, sqlTable, dTab, indexes, idxMetas)
		if err != nil {
			return err
		}

		// merge new chunks with preexisting chunks
		for _, updateMeta := range idxMetas {
			stat := newTableStats[updateMeta.qual]
			if stat != nil {
				var err error
				if _, ok := statDb.GetStat(branch, updateMeta.qual); !ok {
					err = statDb.SetStat(ctx, branch, updateMeta.qual, stat)
				} else {
					err = statDb.ReplaceChunks(ctx, branch, updateMeta.qual, updateMeta.allAddrs, updateMeta.dropChunks, stat.Hist)
				}
				if err != nil {
					return err
				}
				p.UpdateStatus(dbName, fmt.Sprintf("refreshed %s", dbName))
			}
		}
	}

	for _, q := range statDb.ListStatQuals(branch) {
		// table or index delete leaves hole in stats
		// this is separate from threshold check
		if !tableExistsAndSkipped[q.Table()] && !qualExists[q] {
			// only delete stats we've verified are deleted
			deletedStats = append(deletedStats, q)
		}
	}

	statDb.DeleteStats(ctx, branch, deletedStats...)

	if err := statDb.Flush(ctx, branch); err != nil {
		return err
	}

	return nil
}
