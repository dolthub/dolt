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
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
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

	var branch string
	if strings.Contains(dbName, "/") {
		parts := strings.Split(dbName, "/")
		if len(parts) == 2 {
			dbName = parts[0]
			branch = parts[1]
		}
	}
	if branch == "" {
		branch, err := dSess.GetBranch()
		if err != nil {
			return err
		}

		if branch == "" {
			branch = "main"
		}
	}

	db, err := sc.pro.Database(ctx, dbName)
	sqlDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), branch, branch+"/"+dbName)
	if err != nil {
		return err
	}

	tableKey, newTableStats, err := sc.updateTable(ctx, table.Name(), sqlDb)
	if err != nil {
		return err
	}

	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	sc.Stats[tableKey] = newTableStats
	return nil
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
	key := tableIndexesKey{
		db:     db,
		branch: branch,
		table:  table,
		schema: schema,
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
	return sc.sq.InterruptSync(ctx, func() {
		if strings.EqualFold(sc.statsBackingDb, dbName) {
			delete(sc.dbFs, dbName)
			if err := sc.rotateStorage(ctx); err != nil {
				sc.descError("drop rotateStorage", err)
			}
		}

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
	})
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

func (sc *StatsCoord) Init(ctx context.Context, dbs []dsess.SqlDatabase, keepStorage bool) error {
	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return err
	}
	for i, db := range dbs {
		if db, ok := db.(sqle.Database); ok { // exclude read replica dbs
			fs, err := sc.pro.FileSystemForDatabase(db.AliasedName())
			if err != nil {
				return err
			}
			sc.AddFs(db, fs)
			if i == 0 && !keepStorage {
				if err := sc.rotateStorage(sqlCtx); err != nil {
					return err
				}
			}
		}
	}
	sc.sq.Run(ctx)
	return nil
}

func (sc *StatsCoord) Purge(ctx *sql.Context) error {
	if err := sc.rotateStorage(ctx); err != nil {
		return err
	}
	if err := sc.kv.StartGc(ctx, 0); err != nil {
		return err
	}
	return sc.kv.FinishGc(nil)
}

func (sc *StatsCoord) rotateStorage(ctx *sql.Context) error {
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

	if len(sc.dbFs) == 0 {
		sc.kv = mem
		sc.statsBackingDb = ""
		return nil
	}

	var newStorageTarget string
	for db, _ := range sc.dbFs {
		newStorageTarget = db
		break
	}

	if err := sc.rm(newStorageTarget); err != nil {
		return err
	}

	newKv, err := sc.initStorage(ctx, newStorageTarget)
	if err != nil {
		return err
	}

	newKv.mem = mem
	sc.kv = newKv
	sc.statsBackingDb = newStorageTarget
	return nil
}

func (sc *StatsCoord) rm(db string) error {
	fs, ok := sc.dbFs[db]
	if !ok {
		return fmt.Errorf("failed to remove stats db: %s filesys not found", db)
	}

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

func (sc *StatsCoord) initStorage(ctx *sql.Context, storageTarget string) (*prollyStats, error) {
	fs, ok := sc.dbFs[strings.ToLower(storageTarget)]
	if !ok {
		return nil, fmt.Errorf("failed to remove stats db: %s filesys not found", storageTarget)
	}

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
		err = dEnv.InitRepo(ctx, types.Format_Default, sess.Username(), sess.Email(), storageTarget)
		if err != nil {
			return nil, err
		}
	} else if !isDir {
		return nil, fmt.Errorf("file exists where the dolt stats directory should be")
	} else {
		dEnv = env.LoadWithoutDB(ctx, sc.hdp, statsFs, "", doltversion.Version)
	}

	if err := dEnv.LoadDoltDBWithParams(ctx, types.Format_Default, urlPath, statsFs, params); err != nil {
		return nil, err
	}

	deaf := dEnv.DbEaFactory(ctx)

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}
	opts := editor.Options{
		Deaf:    deaf,
		Tempdir: tmpDir,
	}
	statsDb, err := sqle.NewDatabase(ctx, "stats", dEnv.DbData(ctx), opts)
	if err != nil {
		return nil, err
	}
	return NewProllyStats(ctx, statsDb)
}

func (sc *StatsCoord) WaitForDbSync(ctx *sql.Context) error {
	// wait for the current partial + one full cycle to complete
	for _ = range 2 {
		done := sc.getCycleWaiter()
		select {
		case <-done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

func (sc *StatsCoord) Gc(ctx *sql.Context) error {
	sc.sq.InterruptAsync(func() {
		sc.doGc = true
	})
	return sc.WaitForDbSync(ctx)
}
