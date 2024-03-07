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

	dSess := dsess.DSessFromSess(ctx.Session)
	var branches []string
	if _, bs, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsBranches); bs == "" {
		defaultBranch, err := dSess.GetBranch()
		if err != nil {
			return err
		}
		branches = append(branches, defaultBranch)
	} else {
		for _, branch := range strings.Split(bs.(string), ",") {
			branches = append(branches, strings.TrimSpace(branch))
		}
	}
	return p.InitAutoRefreshWithParams(ctxFactory, dbName, bThreads, intervalSec, thresholdf64, branches)
}

func (p *Provider) InitAutoRefreshWithParams(ctxFactory func(ctx context.Context) (*sql.Context, error), dbName string, bThreads *sql.BackgroundThreads, checkInterval time.Duration, updateThresh float64, branches []string) error {
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
			case <-timer.C:
				func() {
					p.mu.Lock()
					defer p.mu.Unlock()
					select {
					case <-dropDbCtx.Done():
						timer.Stop()
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
					}
					for _, branch := range branches {
						if br, ok, err := ddb.HasBranch(ctx, branch); ok {
							sqlCtx.GetLogger().Debugf("starting statistics refresh check for '%s': %s", dbName, time.Now().String())
							if err := p.checkRefresh(sqlCtx, dbName, br, updateThresh); err != nil {
								sqlCtx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
								return
							}
						} else if err != nil {
							sqlCtx.GetLogger().Debugf("statistics refresh error: branch check error %s", err.Error())
						} else {
							sqlCtx.GetLogger().Debugf("statistics refresh error: branch not found %s", br)
						}
					}
				}()
				timer.Reset(checkInterval)
			}
		}
	})
}

func (p *Provider) checkRefresh(ctx *sql.Context, dbName, branch string, updateThresh float64) error {
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

	// important: update session references every loop
	dSess := dsess.DSessFromSess(ctx.Session)
	prov := dSess.Provider()

	// WORKING root database
	sqlDb, err := prov.Database(ctx, fmt.Sprintf("%s/%s", dbName, branch))
	if err != nil {
		return err
	}

	tables, err := sqlDb.GetTableNames(ctx)
	if err != nil {
		return err
	}

	for _, table := range tables {
		sqlTable, dTab, err := p.getLatestTable(ctx, table, dbName, branch)
		if err != nil {
			return err
		}

		tableHash, err := dTab.GetRowDataHash(ctx)
		if err != nil {
			return err
		}

		if statDb.GetLatestHash(branch, table) == tableHash {
			// no data changes since last check
			tableExistsAndSkipped[table] = true
			ctx.GetLogger().Debugf("statistics refresh: table hash unchanged since last check: %s", tableHash)
			continue
		} else {
			ctx.GetLogger().Debugf("statistics refresh: new table hash: %s", tableHash)
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
			qual := sql.NewStatQualifier(dbName, table, strings.ToLower(index.ID()))
			qualExists[qual] = true
			curStat, ok := statDb.GetStat(branch, qual)
			if !ok {
				curStat = NewDoltStats()
				curStat.Qual = qual

				cols := make([]string, len(index.Expressions()))
				tablePrefix := fmt.Sprintf("%s.", table)
				for i, c := range index.Expressions() {
					cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
				}
				curStat.Columns = cols
			}
			ctx.GetLogger().Debugf("statistics refresh index: %s", qual.String())

			updateMeta, err := newIdxMeta(ctx, curStat, dTab, index, curStat.Columns)
			if err != nil {
				ctx.GetLogger().Debugf("statistics refresh error: %s", err.Error())
				continue
			}
			curCnt := float64(len(curStat.Active))
			updateCnt := float64(len(updateMeta.newNodes))
			deleteCnt := float64(len(curStat.Active) - len(updateMeta.keepChunks))
			ctx.GetLogger().Debugf("statistics current: %d, new: %d, delete: %d", int(curCnt), int(updateCnt), int(deleteCnt))

			if curCnt == 0 || (deleteCnt+updateCnt)/curCnt > updateThresh {
				ctx.GetLogger().Debugf("statistics updating: %s", updateMeta.qual)
				// mark index for updating
				idxMetas = append(idxMetas, updateMeta)
				// update lastest hash if we haven't already
				statDb.SetLatestHash(branch, table, tableHash)
			}
		}

		// get new buckets for index chunks to update
		newTableStats, err := createNewStatsBuckets(ctx, sqlTable, dTab, indexes, idxMetas)
		if err != nil {
			return err
		}

		// merge new chunks with preexisting chunks
		// TODO move to put chunks
		for _, updateMeta := range idxMetas {
			stat := newTableStats[updateMeta.qual]
			if stat != nil {
				var err error
				if _, ok := statDb.GetStat(branch, updateMeta.qual); !ok {
					err = statDb.SetStat(ctx, branch, updateMeta.qual, stat)
				} else {
					err = statDb.ReplaceChunks(ctx, branch, updateMeta.qual, updateMeta.allAddrs, updateMeta.dropChunks, stat.Histogram)
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

	statDb.DeleteStats(branch, deletedStats...)

	if err := statDb.Flush(ctx, branch); err != nil {
		return err
	}

	return nil
}
