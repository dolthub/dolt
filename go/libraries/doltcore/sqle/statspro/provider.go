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
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"strings"
)

var _ sql.StatsProvider = (*StatsCoord)(nil)

func (sc *StatsCoord) GetTableStats(ctx *sql.Context, db string, table sql.Table) ([]sql.Statistic, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return nil, err
	}
	key := tableIndexesKey{
		db:     db,
		branch: branch,
		table:  table.Name(),
	}
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	st := sc.Stats[key]
	var ret []sql.Statistic
	for _, s := range st {
		ret = append(ret, s)
	}
	return ret, nil
}

func (sc *StatsCoord) RefreshTableStats(ctx *sql.Context, table sql.Table, dbName string) error {
	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return err
	}

	if branch == "" {
		branch = "main"
	}

	var sqlDb sqle.Database
	func() {
		sc.dbMu.Lock()
		defer sc.dbMu.Unlock()
		for _, db := range sc.dbs {
			if db.AliasedName() == dbName && db.Revision() == branch {
				sqlDb = db
				return
			}
		}
	}()

	if sqlDb.Name() == "" {
		return fmt.Errorf("qualified database not found: %s/%s", branch, dbName)
	}

	after := NewControl("finish analyze", func(sc *StatsCoord) error { return nil })
	analyze := NewAnalyzeJob(ctx, sqlDb, []string{table.String()}, after)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sc.Done:
		return fmt.Errorf("stat queue was interrupted")
	case sc.Jobs <- analyze:
	}

	// wait for finalize to finish before returning
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sc.Done:
		return fmt.Errorf("stat queue was interrupted")
	case <-after.done:
		return nil
	}
}

func (sc *StatsCoord) SetStats(ctx *sql.Context, s sql.Statistic) error {
	ss, ok := s.(*stats.Statistic)
	if !ok {
		return fmt.Errorf("expected *stats.Statistics, found %T", s)
	}
	key, err := sc.statsKey(ctx, ss.Qualifier().Db(), ss.Qualifier().Table())
	if err != nil {
		return err
	}
	sc.Stats[key] = sc.Stats[key][:0]
	sc.Stats[key] = append(sc.Stats[key], ss)
	return nil
}

func (sc *StatsCoord) GetStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) (sql.Statistic, bool) {
	key, err := sc.statsKey(ctx, qual.Database, qual.Table())
	if err != nil {
		return nil, false
	}
	for _, s := range sc.Stats[key] {
		if strings.EqualFold(s.Qualifier().Index(), qual.Index()) {
			return s, true
		}
	}
	return nil, false
}

func (sc *StatsCoord) DropStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) error {
	key, err := sc.statsKey(ctx, qual.Database, qual.Table())
	if err != nil {
		return err
	}
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	delete(sc.Stats, key)
	return nil
}

func (sc *StatsCoord) DropDbStats(ctx *sql.Context, dbName string, flush bool) error {
	var doSwap bool
	var newStorageTarget sqle.Database

	func() {
		sc.gcMu.Lock()
		defer sc.gcMu.Unlock()
		if sc.gcCancel != nil {
			sc.gcCancel()
			sc.gcCancel = nil
		}
	}()

	func() {
		sc.dbMu.Lock()
		defer sc.dbMu.Unlock()
		doSwap = strings.EqualFold(sc.statsEncapsulatingDb, dbName)
		for i := 0; i < len(sc.dbs); i++ {
			db := sc.dbs[i]
			if strings.EqualFold(db.AliasedName(), dbName) {
				sc.dbs = append(sc.dbs[:i], sc.dbs[i+1:]...)
				i--
			}
			if doSwap && newStorageTarget.Name() == "" {
				newStorageTarget = db
			}
		}
		delete(sc.Branches, dbName)
	}()

	if doSwap {
		// synchronously replace?
		// return early after swap and async the actual writes?
		fs, err := sc.pro.FileSystemForDatabase(newStorageTarget.AliasedName())
		if err != nil {
			return err
		}
		newKv, err := sc.initStorage(ctx, fs, newStorageTarget.Revision())
		if err != nil {
			return err
		}
		if pkv, ok := sc.kv.(*prollyStats); ok {
			newKv.mem = pkv.mem
		}
	} else {
		sc.setGc()
	}

	// stats lock is more contentious, do last
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	var deleteKeys []tableIndexesKey
	for k, _ := range sc.Stats {
		if strings.EqualFold(dbName, k.db) {
			deleteKeys = append(deleteKeys, k)
		}
	}
	for _, k := range deleteKeys {
		delete(sc.Stats, k)
	}

	return nil
}

func (sc *StatsCoord) statsKey(ctx *sql.Context, dbName, table string) (tableIndexesKey, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return tableIndexesKey{}, err
	}
	key := tableIndexesKey{
		db:     dbName,
		branch: branch,
		table:  table,
	}
	return key, nil
}

func (sc *StatsCoord) RowCount(ctx *sql.Context, dbName string, table sql.Table) (uint64, error) {
	key, err := sc.statsKey(ctx, dbName, table.Name())
	if err != nil {
		return 0, err
	}
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	for _, s := range sc.Stats[key] {
		if strings.EqualFold(s.Qualifier().Index(), "PRIMARY") {
			return s.RowCnt, nil
		}
	}
	return 0, nil
}

func (sc *StatsCoord) DataLength(ctx *sql.Context, dbName string, table sql.Table) (uint64, error) {
	key, err := sc.statsKey(ctx, dbName, table.Name())
	if err != nil {
		return 0, err
	}
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	for _, s := range sc.Stats[key] {
		if strings.EqualFold(s.Qualifier().Index(), "PRIMARY") {
			return s.RowCnt, nil
		}
	}
	return 0, nil
}
