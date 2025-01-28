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
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"log"
	"path"
	"path/filepath"
	"strconv"
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

	var sqlDb dsess.SqlDatabase
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

	if sqlDb == nil {
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
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
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
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
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

func (sc *StatsCoord) GetTableDoltStats(ctx *sql.Context, branch, db, schema, table string) ([]*stats.Statistic, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	log.Printf("get stat: %s/%s/%s\n", branch, db, table)
	key := tableIndexesKey{
		db:     db,
		branch: branch,
		table:  table,
		schema: schema,
	}
	for key, ss := range sc.Stats {
		log.Println("  stats exist " + key.String() + " " + strconv.Itoa(len(ss)))
	}
	return sc.Stats[key], nil
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
		doSwap = strings.EqualFold(sc.statsBackingDb, dbName)
		for i := 0; i < len(sc.dbs); i++ {
			db := sc.dbs[i]
			if strings.EqualFold(db.AliasedName(), dbName) {
				sc.dbs = append(sc.dbs[:i], sc.dbs[i+1:]...)
				i--
			}
		}
		delete(sc.Branches, dbName)
	}()

	if doSwap {
		if err := sc.rotateStorage(ctx); err != nil {
			return err
		}
	}

	sc.setGc()

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

func (sc *StatsCoord) CancelRefreshThread(dbName string) {
	sc.Drop(dbName)
}

func (sc *StatsCoord) StartRefreshThread(ctx *sql.Context, _ dsess.DoltDatabaseProvider, _ string, _ *env.DoltEnv, sqlDb dsess.SqlDatabase) error {
	<-sc.Add(ctx, sqlDb)
	return nil
}

func (sc *StatsCoord) ThreadStatus(string) string {
	return ""
}

func (sc *StatsCoord) Prune(ctx *sql.Context) error {
	done := make(chan struct{})
	sc.runGc(ctx, done)
	<-done
	return nil
}

func (sc *StatsCoord) Purge(ctx *sql.Context) error {
	return sc.rotateStorage(ctx)
}

func (sc *StatsCoord) rotateStorage(ctx *sql.Context) error {
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	if sc.statsBackingDb != "" {
		if err := sc.rm(sc.statsBackingDb); err != nil {
			return err
		}
	}

	var mem *memStats
	switch kv := sc.kv.(type) {
	case *prollyStats:
		mem = kv.mem
	case *memStats:
		mem = kv
	default:
		mem = NewMemStats()
	}

	if len(sc.dbs) == 0 {
		sc.kv = mem
		sc.statsBackingDb = ""
		return nil
	}

	newStorageTarget := sc.dbs[0]
	if err := sc.rm(newStorageTarget.AliasedName()); err != nil {
		return err
	}

	newKv, err := sc.initStorage(ctx, newStorageTarget)
	if err != nil {
		return err
	}

	newKv.mem = mem
	sc.kv = newKv
	sc.statsBackingDb = newStorageTarget.AliasedName()
	return nil
}

func (sc *StatsCoord) rm(db string) error {
	fs, err := sc.pro.FileSystemForDatabase(db)
	if err != nil {
		return err
	}

	//remove from filesystem
	statsFs, err := fs.WithWorkingDir(dbfactory.DoltStatsDir)
	if err != nil {
		return err
	}

	if ok, _ := statsFs.Exists(""); ok {
		if err := statsFs.Delete("", true); err != nil {
			return err
		}
	}

	dropDbLoc, err := statsFs.Abs("")
	if err != nil {
		return err
	}

	if err = dbfactory.DeleteFromSingletonCache(filepath.ToSlash(dropDbLoc + "/.dolt/noms")); err != nil {
		return err
	}
	return nil
}

func (sc *StatsCoord) initStorage(ctx *sql.Context, storageTarget dsess.SqlDatabase) (*prollyStats, error) {
	fs, err := sc.pro.FileSystemForDatabase(storageTarget.AliasedName())
	if err != nil {
		return nil, err
	}

	// assume access is protected by kvLock
	// get reference to target database
	params := make(map[string]interface{})
	params[dbfactory.GRPCDialProviderParam] = sc.dialPro

	var urlPath string
	u, err := earl.Parse(sc.pro.DbFactoryUrl())
	if u.Scheme == dbfactory.MemScheme {
		urlPath = path.Join(sc.pro.DbFactoryUrl(), dbfactory.DoltDataDir)
	} else if u.Scheme == dbfactory.FileScheme {
		urlPath = doltdb.LocalDirDoltDB
	}

	statsFs, err := fs.WithWorkingDir(dbfactory.DoltStatsDir)
	if err != nil {
		return nil, err
	}

	var dEnv *env.DoltEnv
	exists, isDir := statsFs.Exists("")
	if !exists {
		err := statsFs.MkDirs("")
		if err != nil {
			return nil, fmt.Errorf("unable to make directory '%s', cause: %s", dbfactory.DoltStatsDir, err.Error())
		}

		dEnv = env.Load(ctx, sc.hdp, statsFs, urlPath, "test")
		sess := dsess.DSessFromSess(ctx.Session)
		err = dEnv.InitRepo(ctx, types.Format_Default, sess.Username(), sess.Email(), storageTarget.AliasedName())
		if err != nil {
			return nil, err
		}
	} else if !isDir {
		return nil, fmt.Errorf("file exists where the dolt stats directory should be")
	} else {
		dEnv = env.LoadWithoutDB(ctx, sc.hdp, statsFs, "")
	}

	if dEnv.DoltDB == nil {
		ddb, err := doltdb.LoadDoltDBWithParams(ctx, types.Format_Default, urlPath, statsFs, params)
		if err != nil {
			return nil, err
		}

		dEnv.DoltDB = ddb
	}

	deaf := dEnv.DbEaFactory()

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}
	opts := editor.Options{
		Deaf:    deaf,
		Tempdir: tmpDir,
	}
	statsDb, err := sqle.NewDatabase(ctx, "stats", dEnv.DbData(), opts)
	if err != nil {
		return nil, err
	}
	return NewProllyStats(ctx, statsDb)
}

func (sc *StatsCoord) WaitForDbSync(ctx *sql.Context) error {
	// make a control job
	// wait until the control job done before returning
	j := NewControl("wait for sync", func(sc *StatsCoord) error { return nil })
	if err := sc.sendJobs(ctx, j); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case <-j.done:
	}
	return nil
}
