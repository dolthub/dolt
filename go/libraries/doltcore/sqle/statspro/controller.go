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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro/jobqueue"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/sirupsen/logrus"
	"log"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

var _ sql.StatsProvider = (*StatsController)(nil)

type ctxFactory func(ctx context.Context) (*sql.Context, error)

type tableIndexesKey struct {
	db     string
	branch string
	table  string
	schema string
}

func (k tableIndexesKey) String() string {
	return k.db + "/" + k.branch + "/" + k.table
}

type StatsController struct {
	logger         *logrus.Logger
	pro            *sqle.DoltDatabaseProvider
	statsBackingDb filesys.Filesys
	dialPro        dbfactory.GRPCDialProvider
	hdp            env.HomeDirProvider

	dbFs map[string]filesys.Filesys

	// ctxGen lets us fetch the most recent working root
	ctxGen ctxFactory

	sq *jobqueue.SerialQueue

	activeCtxCancel context.CancelFunc
	listeners       *listenMsg

	JobInterval time.Duration
	gcInterval  time.Duration
	memOnly     bool
	enableGc    bool
	doGc        bool
	Debug       bool
	closed      chan struct{}

	// kv is a content-addressed cache of histogram objects:
	// buckets, first bounds, and schema-specific statistic
	// templates.
	kv StatsKv

	// Stats tracks table statistics accessible to sessions.
	statsMu sync.Mutex
	Stats   *rootStats
	genCnt  atomic.Uint64
	gcCnt   int
}

type rootStats struct {
	hashes          map[tableIndexesKey]hash.Hash
	stats           map[tableIndexesKey][]*stats.Statistic
	DbCnt           int `json:"dbCnt"`
	BucketWrites    int `json:"bucketWrites""`
	TablesProcessed int `json:"tablesProcessed""`
	TablesSkipped   int `json:"tablesSkipped""`
}

func newRootStats() *rootStats {
	return &rootStats{
		hashes: make(map[tableIndexesKey]hash.Hash),
		stats:  make(map[tableIndexesKey][]*stats.Statistic),
	}
}

func (rs *rootStats) String() string {
	str, _ := json.Marshal(rs)
	return string(str)
}

func NewStatsController(pro *sqle.DoltDatabaseProvider, ctxGen ctxFactory, logger *logrus.Logger, dEnv *env.DoltEnv) *StatsController {
	sq := jobqueue.NewSerialQueue().WithErrorCb(func(err error) {
		logger.Error(err)
	})
	return &StatsController{
		statsMu:     sync.Mutex{},
		logger:      logger,
		JobInterval: 500 * time.Millisecond,
		gcInterval:  24 * time.Hour,
		sq:          sq,
		Stats:       newRootStats(),
		dbFs:        make(map[string]filesys.Filesys),
		closed:      make(chan struct{}),
		kv:          NewMemStats(),
		pro:         pro,
		hdp:         dEnv.GetUserHomeDir,
		dialPro:     env.NewGRPCDialProviderFromDoltEnv(dEnv),
		ctxGen:      ctxGen,
		genCnt:      atomic.Uint64{},
	}
}

func (sc *StatsController) SetMemOnly(v bool) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	sc.memOnly = v
}

func (sc *StatsController) SetEnableGc(v bool) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	sc.enableGc = v
}

func (sc *StatsController) setDoGc() {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	sc.doGc = true
}

func (sc *StatsController) gcIsSet() bool {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	return sc.doGc
}

// SetTimers can only be called after Init
func (sc *StatsController) SetTimers(job, gc int64) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	sc.sq.NewRateLimit(time.Duration(max(1, job)))
	sc.gcInterval = time.Duration(gc)
}

func (sc *StatsController) AddFs(ctx *sql.Context, db dsess.SqlDatabase, fs filesys.Filesys, rotateOk bool) error {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()

	firstDb := len(sc.dbFs) == 0
	sc.dbFs[db.AliasedName()] = fs
	if rotateOk && firstDb {
		return sc.lockedRotateStorage(ctx)
	}
	return nil
}

func (sc *StatsController) Info(ctx context.Context) (dprocedures.StatsInfo, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()

	// don't use protected access / deadlock
	cachedBucketCnt := sc.kv.Len()
	storageCnt, err := sc.kv.Flush(ctx)
	if err != nil {
		return dprocedures.StatsInfo{}, err
	}

	var cachedBoundCnt int
	var cachedTemplateCnt int
	var backing string
	switch kv := sc.kv.(type) {
	case *memStats:
		cachedBoundCnt = len(kv.bounds)
		cachedTemplateCnt = len(kv.templates)
		backing = "mem"
	case *prollyStats:
		cachedBoundCnt = len(kv.mem.bounds)
		cachedTemplateCnt = len(kv.mem.templates)
		backing, _ = sc.statsBackingDb.Abs("")
	}
	backingParts := strings.Split(backing, "/")
	return dprocedures.StatsInfo{
		DbCnt:             sc.Stats.DbCnt,
		Active:            sc.activeCtxCancel != nil,
		CachedBucketCnt:   cachedBucketCnt,
		StorageBucketCnt:  storageCnt,
		CachedBoundCnt:    cachedBoundCnt,
		CachedTemplateCnt: cachedTemplateCnt,
		StatCnt:           len(sc.Stats.stats),
		GenCnt:            int(sc.genCnt.Load()),
		GcCnt:             sc.gcCnt,
		Backing:           backingParts[len(backingParts)-1],
	}, nil
}

func (sc *StatsController) descError(d string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	if sc.Debug {
		log.Println("stats error: ", err.Error())
	}
	sc.logger.Errorf("stats error; job detail: %s; verbose: %s", d, err)
}

func (sc *StatsController) GetTableStats(ctx *sql.Context, db string, table sql.Table) ([]sql.Statistic, error) {
	key, err := sc.statsKey(ctx, db, table.Name())
	if err != nil {
		return nil, err
	}
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	if sc.Stats == nil {
		return nil, nil
	}
	st := sc.Stats.stats[key]
	var ret []sql.Statistic
	for _, s := range st {
		ret = append(ret, s)
	}
	return ret, nil
}

func (sc *StatsController) AnalyzeTable(ctx *sql.Context, table sql.Table, dbName string) (err error) {
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
		var err error
		branch, err = dSess.GetBranch()
		if err != nil {
			return err
		}

		if branch == "" {
			branch = env.DefaultInitBranch
		}
	}

	db, err := sc.pro.Database(ctx, dbName)
	sqlDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), branch, branch+"/"+dbName)
	if err != nil {
		return err
	}

	newStats := newRootStats()
	err = sc.updateTable(ctx, newStats, table.Name(), sqlDb, nil)
	if err != nil {
		return err
	}

	sc.statsMu.Lock()
	for k, v := range newStats.stats {
		sc.Stats.stats[k] = v
		sc.Stats.hashes[k] = newStats.hashes[k]
	}
	sc.statsMu.Unlock()

	return err
}

func (sc *StatsController) SetStats(ctx *sql.Context, s sql.Statistic) error {
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

	// not efficient, but this is only used for testing
	var newStats []*stats.Statistic
	for _, ss := range sc.Stats.stats[key] {
		if !strings.EqualFold(ss.Qualifier().Index(), s.Qualifier().Index()) {
			newStats = append(newStats, ss)
		}
	}
	newStats = append(newStats, ss)
	sc.Stats.stats[key] = newStats
	return nil
}

func (sc *StatsController) GetStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) (sql.Statistic, bool) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	key, err := sc.statsKey(ctx, qual.Database, qual.Table())
	if err != nil {
		return nil, false
	}
	for _, s := range sc.Stats.stats[key] {
		if strings.EqualFold(s.Qualifier().Index(), qual.Index()) {
			return s, true
		}
	}
	return nil, false
}

func (sc *StatsController) GetTableDoltStats(ctx *sql.Context, branch, db, schema, table string) ([]*stats.Statistic, error) {
	key := tableIndexesKey{
		db:     strings.ToLower(db),
		branch: strings.ToLower(branch),
		table:  strings.ToLower(table),
		schema: strings.ToLower(schema),
	}
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	if sc.Stats == nil {
		return nil, nil
	}
	return sc.Stats.stats[key], nil
}

func (sc *StatsController) DropStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) error {
	key, err := sc.statsKey(ctx, qual.Database, qual.Table())
	if err != nil {
		return err
	}
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	delete(sc.Stats.stats, key)
	return nil
}

func (sc *StatsController) DropDbStats(ctx *sql.Context, dbName string, flush bool) error {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	log.Println("drop statsdb", dbName)

	dbFs := sc.dbFs[dbName]
	delete(sc.dbFs, dbName)
	if sc.statsBackingDb == dbFs {
		// don't wait to see if the thread context is invalidated
		func() {
			sc.statsMu.Unlock()
			sc.Restart()
			defer sc.statsMu.Lock()
		}()
		if err := sc.lockedRotateStorage(ctx); err != nil {
			return err
		}
	}

	var deleteKeys []tableIndexesKey
	for k, _ := range sc.Stats.stats {
		if strings.EqualFold(dbName, k.db) {
			deleteKeys = append(deleteKeys, k)
		}
	}
	for _, k := range deleteKeys {
		delete(sc.Stats.stats, k)
	}
	return nil
}

func (sc *StatsController) statsKey(ctx *sql.Context, dbName, table string) (tableIndexesKey, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return tableIndexesKey{}, err
	}
	key := tableIndexesKey{
		db:     strings.ToLower(dbName),
		branch: strings.ToLower(branch),
		table:  strings.ToLower(table),
	}
	return key, nil
}

func (sc *StatsController) RowCount(ctx *sql.Context, dbName string, table sql.Table) (uint64, error) {
	key, err := sc.statsKey(ctx, dbName, table.Name())
	if err != nil {
		return 0, err
	}
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	for _, s := range sc.Stats.stats[key] {
		if strings.EqualFold(s.Qualifier().Index(), "PRIMARY") {
			return s.RowCnt, nil
		}
	}
	return 0, nil
}

func (sc *StatsController) DataLength(ctx *sql.Context, dbName string, table sql.Table) (uint64, error) {
	key, err := sc.statsKey(ctx, dbName, table.Name())
	if err != nil {
		return 0, err
	}
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	for _, s := range sc.Stats.stats[key] {
		if strings.EqualFold(s.Qualifier().Index(), "PRIMARY") {
			return s.RowCnt, nil
		}
	}
	return 0, nil
}

func (sc *StatsController) Purge(ctx *sql.Context) error {
	genStart := sc.genCnt.Load()
	newKv := NewMemStats()
	newKv.gcGen = genStart
	newStats := newRootStats()
	if ok, err := sc.trySwapStats(ctx, genStart, newStats, newKv); !ok {
		return fmt.Errorf("failed to purge stats")
	} else if err != nil {
		return err
	}
	return nil
}

func (sc *StatsController) rotateStorage(ctx context.Context) error {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	return sc.lockedRotateStorage(ctx)
}

func (sc *StatsController) lockedRotateStorage(ctx context.Context) error {
	if sc.memOnly {
		return nil
	}
	//log.Println("rotate storage")
	if sc.statsBackingDb != nil {
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
		sc.statsBackingDb = nil
		return nil
	}

	var newStorageTarget filesys.Filesys
	for _, dbFs := range sc.dbFs {
		newStorageTarget = dbFs
		if newStorageTarget == sc.statsBackingDb {
			// prefer continuity when possible
			break
		}
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

func (sc *StatsController) rm(fs filesys.Filesys) error {
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

	//log.Println("rm", dropDbLoc)

	if err = dbfactory.DeleteFromSingletonCache(filepath.ToSlash(dropDbLoc + "/.dolt/noms")); err != nil {
		return err
	}
	return nil
}

func (sc *StatsController) initStorage(ctx context.Context, fs filesys.Filesys) (*prollyStats, error) {
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
		err = dEnv.InitRepo(ctx, types.Format_Default, "stats", "stats@stats.com", env.DefaultInitBranch)
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
	m, err := dEnv.DbData(ctx).Ddb.GetStatistics(ctx)
	if err == nil {
		// use preexisting map
		kd, vd := m.Descriptors()
		return &prollyStats{
			mu:     sync.Mutex{},
			destDb: statsDb,
			kb:     val.NewTupleBuilder(kd),
			vb:     val.NewTupleBuilder(vd),
			m:      m.Mutate(),
			mem:    NewMemStats(),
		}, nil
	}
	return NewProllyStats(ctx, statsDb)
}
