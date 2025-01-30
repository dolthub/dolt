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
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type StatsDbController struct {
	ch       chan StatsJob
	destDb   dsess.SqlDatabase
	sourceDb dsess.SqlDatabase
	// qualified db ->
	branches map[string]BranchDb
	dirty    sql.FastIntSet
}

type BranchDb struct {
	db           string
	branch       string
	tableHashes  map[string]hash.Hash
	schemaHashes map[string]hash.Hash
}

type StatsJob interface {
	Finish()
	String() string
}

var _ StatsJob = (*ReadJob)(nil)
var _ StatsJob = (*SeedDbTablesJob)(nil)
var _ StatsJob = (*ControlJob)(nil)
var _ StatsJob = (*FinalizeJob)(nil)

func NewSeedJob(sqlDb dsess.SqlDatabase) SeedDbTablesJob {
	return SeedDbTablesJob{
		sqlDb:  sqlDb,
		tables: nil,
		done:   make(chan struct{}),
	}
}

type tableStatsInfo struct {
	name        string
	schHash     hash.Hash
	idxRoots    []hash.Hash
	bucketCount int
}

type SeedDbTablesJob struct {
	sqlDb  dsess.SqlDatabase
	tables []tableStatsInfo
	done   chan struct{}
}

func (j SeedDbTablesJob) Finish() {
	close(j.done)
}

func (j SeedDbTablesJob) String() string {
	b := strings.Builder{}
	b.WriteString("seed db: ")
	b.WriteString(j.sqlDb.RevisionQualifiedName())
	b.WriteString("[")

	var sep = ""
	for _, ti := range j.tables {
		b.WriteString(sep)
		b.WriteString("(" + ti.name + ": " + ti.schHash.String()[:5] + ")")
	}
	b.WriteString("]")

	return b.String()
}

func NewAnalyzeJob(ctx *sql.Context, sqlDb dsess.SqlDatabase, tables []string, after ControlJob) AnalyzeJob {
	return AnalyzeJob{ctx: ctx, sqlDb: sqlDb, tables: tables, after: after, done: make(chan struct{})}
}

type AnalyzeJob struct {
	ctx    *sql.Context
	sqlDb  dsess.SqlDatabase
	tables []string
	after  ControlJob
	done   chan struct{}
}

func (j AnalyzeJob) String() string {
	return "analyze: [" + strings.Join(j.tables, ", ") + "]"
}

func (j AnalyzeJob) Finish() {
	close(j.done)
	return
}

type ReadJob struct {
	// |ctx|/|db| track a specific working set
	ctx      *sql.Context
	db       dsess.SqlDatabase
	table    string
	key      templateCacheKey
	template stats.Statistic
	m        prolly.Map
	first    bool
	nodes    []tree.Node
	ordinals []updateOrdinal
	colCnt   int
	done     chan struct{}
}

func (j ReadJob) Finish() {
	close(j.done)
}

func (j ReadJob) String() string {
	b := strings.Builder{}
	b.WriteString("read: " + j.db.RevisionQualifiedName() + "/" + j.table + ": ")
	sep := ""
	for i, o := range j.ordinals {
		b.WriteString(fmt.Sprintf("%s[%s:%d-%d]", sep, j.nodes[i].HashOf().String()[:5], o.start, o.stop))
		sep = ", "
	}
	return b.String()
}

type finalizeStruct struct {
	buckets []hash.Hash
	tupB    *val.TupleBuilder
}

type FinalizeJob struct {
	tableKey    tableIndexesKey
	keepIndexes map[sql.StatQualifier]bool
	editIndexes map[templateCacheKey]finalizeStruct
	done        chan struct{}
}

func (j FinalizeJob) Finish() {
	close(j.done)
}

func (j FinalizeJob) String() string {
	b := strings.Builder{}
	b.WriteString("finalize " + j.tableKey.String())
	b.WriteString(": ")
	sep := ""
	for idx, fs := range j.editIndexes {
		b.WriteString(fmt.Sprintf("%s(%s: ", sep, idx.idxName))
		sep = ""
		for _, h := range fs.buckets {
			b.WriteString(fmt.Sprintf("%s%s", sep, h.String()[:5]))
			sep = ", "
		}
		b.WriteString(")")
		sep = ", "
	}
	return b.String()
}

func NewControl(desc string, cb func(sc *StatsCoord) error) ControlJob {
	return ControlJob{cb: cb, desc: desc, done: make(chan struct{})}
}

type ControlJob struct {
	cb   func(sc *StatsCoord) error
	desc string
	done chan struct{}
}

func (j ControlJob) Finish() {
	close(j.done)
}

func (j ControlJob) String() string {
	return "ControlJob: " + j.desc
}

type ctxFactory func(ctx context.Context) (*sql.Context, error)

func NewStatsCoord(pro *sqle.DoltDatabaseProvider, ctxGen ctxFactory, logger *logrus.Logger, threads *sql.BackgroundThreads, dEnv *env.DoltEnv) *StatsCoord {
	done := make(chan struct{})
	close(done)
	kv := NewMemStats()
	return &StatsCoord{
		dbMu:           &sync.Mutex{},
		statsMu:        &sync.Mutex{},
		logger:         logger,
		Jobs:           make(chan StatsJob, 1024),
		Done:           done,
		Interrupts:     make(chan ControlJob, 1),
		JobInterval:    50 * time.Millisecond,
		gcInterval:     24 * time.Hour,
		branchInterval: 24 * time.Hour,
		enableGc:       atomic.Bool{},
		bucketCap:      kv.Cap(),
		Stats:          make(map[tableIndexesKey][]*stats.Statistic),
		Branches:       make(map[string][]ref.DoltRef),
		threads:        threads,
		kv:             kv,
		pro:            pro,
		hdp:            dEnv.GetUserHomeDir,
		dialPro:        env.NewGRPCDialProviderFromDoltEnv(dEnv),
		ctxGen:         ctxGen,
	}
}

func (sc *StatsCoord) SetMemOnly(v bool) {
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	sc.memOnly = v
}

func (sc *StatsCoord) SetEnableGc(v bool) {
	sc.enableGc.Store(v)
}

func (sc *StatsCoord) SetTimers(job, gc, branch int64) {
	sc.JobInterval = time.Duration(job) * time.Millisecond
	sc.gcInterval = time.Duration(gc) * time.Millisecond
	sc.branchInterval = time.Duration(branch) * time.Millisecond
}

type tableIndexesKey struct {
	db     string
	branch string
	table  string
	schema string
}

func (k tableIndexesKey) String() string {
	return k.db + "/" + k.branch + "/" + k.table
}

type StatsCoord struct {
	logger      *logrus.Logger
	JobInterval time.Duration
	threads     *sql.BackgroundThreads
	pro         *sqle.DoltDatabaseProvider
	memOnly     bool
	// ctxGen lets us fetch the most recent working root
	ctxGen ctxFactory

	// XXX: do not hold the |dbMu| while accessing |pro|
	dbMu           *sync.Mutex
	dbs            []dsess.SqlDatabase
	branchInterval time.Duration
	ddlGuard       bool

	kv StatsKv

	statsBackingDb string
	cancelSwitch   context.CancelFunc
	dialPro        dbfactory.GRPCDialProvider
	hdp            env.HomeDirProvider

	readCounter atomic.Int32

	activeGc   atomic.Bool
	doGc       atomic.Bool
	enableGc   atomic.Bool
	gcInterval time.Duration
	gcDone     chan struct{}
	gcMu       sync.Mutex
	gcCancel   context.CancelFunc

	doBranchCheck atomic.Bool
	doCapCheck    atomic.Bool
	bucketCnt     atomic.Int64
	bucketCap     int64

	Jobs       chan StatsJob
	Interrupts chan ControlJob
	Done       chan struct{}

	Branches map[string][]ref.DoltRef

	statsMu *sync.Mutex
	// Stats tracks table statistics accessible to sessions.
	Stats map[tableIndexesKey][]*stats.Statistic
}

func (sc *StatsCoord) Stop() {
	select {
	case <-sc.Done:
	default:
		close(sc.Done)
	}
}

func (sc *StatsCoord) Restart(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sc.Done:
	default:
		sc.Stop()
	}

	sc.Done = make(chan struct{})
	return sc.threads.Add("stats", func(ctx context.Context) {
		sc.run(ctx)
	})
}

func (sc *StatsCoord) Close() {
	sc.Stop()
	return
}

func (sc *StatsCoord) cancelGc() {
	sc.gcMu.Lock()
	defer sc.gcMu.Unlock()
	if sc.gcCancel != nil {
		sc.gcCancel()
	}
}

func (sc *StatsCoord) Add(ctx *sql.Context, db dsess.SqlDatabase, branch ref.DoltRef, fs filesys.Filesys) chan struct{} {
	db, err := sqle.RevisionDbForBranch(ctx, db, branch.GetPath(), branch.GetPath()+"/"+db.AliasedName())
	if err != nil {
		sc.error(ControlJob{desc: "add db"}, err)
		ret := make(chan struct{})
		close(ret)
		return ret
	}

	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	sc.ddlGuard = true

	sc.Branches[db.AliasedName()] = append(sc.Branches[db.AliasedName()], ref.NewBranchRef(db.Revision()))
	sc.dbs = append(sc.dbs, db)
	ret := sc.Seed(ctx, db)

	if len(sc.dbs) == 1 {
		sc.statsBackingDb = db.AliasedName()
		var mem *memStats
		switch kv := sc.kv.(type) {
		case *memStats:
			mem = kv
		case *prollyStats:
			mem = kv.mem
		default:
			mem = NewMemStats()
			return ret
		}
		if sc.memOnly {
			return ret
		}
		newKv, err := sc.initStorage(ctx, db, fs)
		if err != nil {
			sc.error(ControlJob{desc: "add db"}, err)
			close(ret)
			return ret
		}
		newKv.mem = mem
		sc.kv = newKv
	}

	return ret
}

func (sc *StatsCoord) Drop(dbName string) {
	// deprecated
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	sc.ddlGuard = true

	for i, db := range sc.dbs {
		if strings.EqualFold(db.Name(), dbName) {
			sc.dbs = append(sc.dbs[:i], sc.dbs[i+1:]...)
			return
		}
	}
}

type StatsInfo struct {
	DbCnt   int
	ReadCnt int
	Active  bool
	JobCnt  int
}

func (sc *StatsCoord) Info() StatsInfo {
	sc.dbMu.Lock()
	dbCnt := len(sc.dbs)
	defer sc.dbMu.Unlock()

	return StatsInfo{
		DbCnt:   dbCnt,
		ReadCnt: int(sc.readCounter.Load()),
		Active:  true,
		JobCnt:  len(sc.Jobs),
	}
}

// event loop must be stopped
func (sc *StatsCoord) flushQueue(ctx context.Context) ([]StatsJob, error) {
	select {
	case <-sc.Done:
	default:
		return nil, fmt.Errorf("cannot read queue while event loop is active")
		// inactive event loop cannot be interrupted, discard
	}
	var ret []StatsJob
	for _ = range len(sc.Jobs) {
		select {
		case <-ctx.Done():
			return nil, nil
		case j, ok := <-sc.Jobs:
			if !ok {
				return nil, nil
			}
			ret = append(ret, j)
		}
	}
	return ret, nil
}

func (sc *StatsCoord) Seed(ctx *sql.Context, sqlDb dsess.SqlDatabase) chan struct{} {
	j := NewSeedJob(sqlDb)
	sc.Jobs <- j
	return j.done
}

func (sc *StatsCoord) Control(desc string, cb func(sc *StatsCoord) error) chan struct{} {
	j := NewControl(desc, cb)
	sc.Jobs <- j
	return j.done
}

func (sc *StatsCoord) Interrupt(desc string, cb func(sc *StatsCoord) error) chan struct{} {
	j := NewControl(desc, cb)
	sc.Interrupts <- j
	return j.done
}

func (sc *StatsCoord) error(j StatsJob, err error) {
	fmt.Println(err.Error())
	sc.logger.Errorf("stats error; job detail: %s; verbose: %s", j.String(), err)
}

// statsRunner operates on stats jobs
func (sc *StatsCoord) run(ctx context.Context) error {
	jobTimer := time.NewTimer(0)
	gcTicker := time.NewTicker(sc.gcInterval)
	branchTicker := time.NewTicker(sc.branchInterval)

	for {
		// sequentially test:
		// (1) ctx done/thread canceled
		// (2) GC check
		// (3) branch check
		// (4) cap check
		// (4) job and other tickers
		select {
		case <-sc.Done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if sc.doGc.Swap(false) {
			if err := sc.runGc(ctx, make(chan struct{})); err != nil {
				if err != nil {
					sc.error(ControlJob{desc: "gc"}, err)
				}
			}
		}

		if sc.doBranchCheck.Swap(false) {
			j := ControlJob{desc: "branch update"}
			newJobs, err := sc.updateBranches(ctx)
			if err != nil {
				sc.error(ControlJob{desc: "branches update"}, err)
			}
			err = sc.sendJobs(ctx, newJobs...)
			if err != nil {
				sc.error(j, err)
			}
		}

		select {
		case <-sc.Done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case j, ok := <-sc.Interrupts:
			if !ok {
				return nil
			}
			if err := j.cb(sc); err != nil {
				sc.error(j, err)
				continue
			}
		default:
		}

		select {
		case <-sc.Done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-jobTimer.C:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case j, ok := <-sc.Jobs:
				if !ok {
					return nil
				}
				//log.Println("execute: ", j.String())
				newJobs, err := sc.executeJob(ctx, j)
				if err != nil {
					sc.error(j, err)
				}
				err = sc.sendJobs(ctx, newJobs...)
				if err != nil {
					sc.error(j, err)
				}
				j.Finish()
			default:
			}
		case <-gcTicker.C:
			sc.setGc()
		case <-branchTicker.C:
			sc.doBranchCheck.Store(true)
		}
		jobTimer.Reset(sc.JobInterval)
	}
}

func (sc *StatsCoord) sendJobs(ctx context.Context, jobs ...StatsJob) error {
	for i := 0; i < len(jobs); i++ {
		j := jobs[i]
		if j == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sc.Jobs <- j:
			//log.Println("send ", j.String())
			if _, ok := j.(ReadJob); ok {
				sc.readCounter.Add(1)
			}
		default:
			sc.doubleChannelSize(ctx)
			i--
		}
	}
	return nil
}

func (sc *StatsCoord) executeJob(ctx context.Context, j StatsJob) ([]StatsJob, error) {
	switch j := j.(type) {
	case SeedDbTablesJob:
		return sc.seedDbTables(ctx, j)
	case ReadJob:
		sc.readCounter.Add(-1)
		return sc.readChunks(ctx, j)
	case FinalizeJob:
		return sc.finalizeUpdate(ctx, j)
	case ControlJob:
		if err := j.cb(sc); err != nil {
			sc.error(j, err)
		}
	case AnalyzeJob:
		return sc.runAnalyze(ctx, j)
	default:
	}
	return nil, nil
}

func (sc *StatsCoord) doubleChannelSize(ctx context.Context) {
	var restart bool
	select {
	case <-sc.Done:
	default:
		sc.Stop()
		restart = true
	}
	close(sc.Jobs)
	ch := make(chan StatsJob, cap(sc.Jobs)*2)
	for j := range sc.Jobs {
		ch <- j
	}
	sc.Jobs = ch
	if restart {
		sc.Restart(ctx)
	}
}

func (sc *StatsCoord) dropTableJob(sqlDb dsess.SqlDatabase, tableName string) StatsJob {
	return FinalizeJob{
		tableKey: tableIndexesKey{
			db:     sqlDb.AliasedName(),
			branch: sqlDb.Revision(),
			table:  tableName,
		},
		editIndexes: nil,
		done:        make(chan struct{}),
	}
}

func (sc *StatsCoord) dropBranchJob(dbName string, branch string) ControlJob {
	return ControlJob{
		desc: "drop branch",
		cb: func(sc *StatsCoord) error {
			sc.statsMu.Lock()
			defer sc.statsMu.Unlock()
			var deleteKeys []tableIndexesKey
			for k, _ := range sc.Stats {
				if strings.EqualFold(dbName, k.db) && strings.EqualFold(branch, k.branch) {
					deleteKeys = append(deleteKeys, k)
				}
			}
			for _, k := range deleteKeys {
				delete(sc.Stats, k)
			}
			return nil
		},
		done: make(chan struct{}),
	}
}

func (sc *StatsCoord) readChunks(ctx context.Context, j ReadJob) ([]StatsJob, error) {
	// check if chunk already in cache
	// if no, see if on disk and we just need to load
	// otherwise perform read to create the bucket, write to disk, update mem ref
	prollyMap := j.m
	updater := newBucketBuilder(sql.StatQualifier{}, j.colCnt, prollyMap.KeyDesc().PrefixDesc(j.colCnt))
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc())

	// all kv puts are guarded by |gcMu| to avoid concurrent
	// GC with stale data discarding some or all state
	sc.gcMu.Lock()
	defer sc.gcMu.Unlock()

	if j.first {
		ctx, err := sc.ctxGen(ctx)
		if err != nil {
			return nil, err
		}

		sc.kv.PutTemplate(j.key, j.template)

		firstNodeHash := j.nodes[0].HashOf()
		if _, ok := sc.kv.GetBound(firstNodeHash); !ok {
			firstRow, err := firstRowForIndex(ctx, prollyMap, val.NewTupleBuilder(prollyMap.KeyDesc()))
			if err != nil {
				if err != nil {
					return nil, err
				}
			}
			fmt.Printf("%s bound %s: %v\n", j.table, firstNodeHash.String()[:5], firstRow)
			sc.kv.PutBound(firstNodeHash, firstRow)
		}
	}
	for i, n := range j.nodes {
		// each node is a bucket
		updater.newBucket()

		// we read exclusive range [node first key, next node first key)
		start, stop := j.ordinals[i].start, j.ordinals[i].stop
		iter, err := j.m.IterOrdinalRange(ctx, start, stop)
		if err != nil {
			return nil, err
		}
		for {
			// stats key will be a prefix of the index key
			keyBytes, _, err := iter.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}
			// build full key
			for i := range keyBuilder.Desc.Types {
				keyBuilder.PutRaw(i, keyBytes.GetField(i))
			}

			updater.add(keyBuilder.BuildPrefixNoRecycle(prollyMap.Pool(), updater.prefixLen))
			keyBuilder.Recycle()
		}

		// finalize the aggregation
		log.Println("read/put chunk ", n.HashOf().String()[:5])
		bucket, err := updater.finalize(ctx, prollyMap.NodeStore())
		if err != nil {
			return nil, err
		}
		err = sc.kv.PutBucket(ctx, n.HashOf(), bucket, val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(j.colCnt)))
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (sc *StatsCoord) runAnalyze(_ context.Context, j AnalyzeJob) ([]StatsJob, error) {
	var ret []StatsJob
	for _, tableName := range j.tables {
		readJobs, _, err := sc.readJobsForTable(j.ctx, j.sqlDb, tableStatsInfo{name: tableName})
		if err != nil {
			return nil, err
		}
		ret = append(ret, readJobs...)
	}
	if j.after.done != nil {
		ret = append(ret, j.after)
	}
	return ret, nil
}

func (sc *StatsCoord) finalizeUpdate(ctx context.Context, j FinalizeJob) ([]StatsJob, error) {
	if len(j.editIndexes) == 0 {
		// delete table
		sc.statsMu.Lock()
		delete(sc.Stats, j.tableKey)
		sc.statsMu.Unlock()
		return nil, nil
	}

	var newStats []*stats.Statistic
	for _, s := range sc.Stats[j.tableKey] {
		if ok := j.keepIndexes[s.Qual]; ok {
			newStats = append(newStats, s)
		}
	}
	for key, fs := range j.editIndexes {
		log.Println("finalize " + j.tableKey.String() + " " + key.String())
		template, ok := sc.kv.GetTemplate(key)
		if !ok {
			return nil, fmt.Errorf(" missing template dependency for table: %s", key)
		}
		template.Qual = sql.NewStatQualifier(j.tableKey.db, "", j.tableKey.table, key.idxName)

		for i, bh := range fs.buckets {
			if i == 0 {
				bnd, ok := sc.kv.GetBound(bh)
				if !ok {
					log.Println("chunks: ", fs.buckets)
					return nil, fmt.Errorf("missing read job bound dependency for chunk %s: %s", key, bh)
				}
				template.LowerBnd = bnd[:fs.tupB.Desc.Count()]
			}
			// accumulate counts
			if b, ok, err := sc.kv.GetBucket(ctx, bh, fs.tupB); err != nil {
				return nil, err
			} else if !ok {
				log.Println("need chunks: ", fs.buckets)
				return nil, fmt.Errorf("missing read job bucket dependency for chunk: %s", bh)
			} else {
				template.RowCnt += b.RowCnt
				template.DistinctCnt += b.DistinctCnt
				template.NullCnt += b.NullCnt
				template.Hist = append(template.Hist, b)
			}
		}
		newStats = append(newStats, &template)
	}

	// protected swap
	sc.statsMu.Lock()
	sc.Stats[j.tableKey] = newStats
	log.Println("stat cnt: ", len(sc.Stats), len(newStats))
	sc.statsMu.Unlock()

	return nil, nil
}

type dbBranchKey struct {
	db     string
	branch string
}

func (sc *StatsCoord) updateBranches(ctx context.Context) ([]StatsJob, error) {
	log.Println("run branch update")
	j := ControlJob{desc: "branch update"}
	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return nil, err
	}

	var ret []StatsJob
	newBranches := make(map[string][]ref.DoltRef)
	var newDbs []dsess.SqlDatabase

	sc.dbMu.Lock()
	sc.ddlGuard = false
	dbBranches := make(map[string][]ref.DoltRef)
	for k, v := range sc.Branches {
		dbBranches[k] = v
	}
	dbs := make([]dsess.SqlDatabase, len(sc.dbs))
	copy(dbs, sc.dbs)
	sc.dbMu.Unlock()

	for dbName, branches := range dbBranches {
		var sqlDb dsess.SqlDatabase
		for _, db := range dbs {
			if strings.EqualFold(db.AliasedName(), dbName) {
				sqlDb = db
				break
			}
		}

		if sqlDb == nil {
			sc.error(j, fmt.Errorf("database in branches list is not tracked: %s", dbName))
			continue
		}

		dSess := dsess.DSessFromSess(sqlCtx.Session)
		dbd, ok := dSess.GetDbData(sqlCtx, dbName)
		if !ok {
			sc.error(j, fmt.Errorf("database in branches list does not exist: %s", dbName))
			continue
		}
		curBranches, err := dbd.Ddb.GetBranches(ctx)
		if err != nil {
			sc.error(j, err)
			continue
		}

		newBranches[sqlDb.AliasedName()] = curBranches

		i := 0
		k := 0
		for i < len(branches) && k < len(curBranches) {
			br := curBranches[k]
			switch strings.Compare(branches[i].GetPath(), curBranches[k].GetPath()) {
			case 0:
				sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
				if err != nil {
					sc.error(j, err)
					continue
				}

				newDbs = append(newDbs, sqlDb)
				i++
				k++
			case -1:
				//sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, branches[i].GetPath(), branches[i].GetPath()+"/"+dbName)
				//if err != nil {
				//	sc.error(j, err)
				//	continue
				//}
				//
				//dropDbs[dbBranchKey{sqlDb.AliasedName(), sqlDb.Revision()}] = true
				i++
			case +1:
				// add
				sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
				if err != nil {
					sc.error(j, err)
					continue
				}

				newDbs = append(newDbs, sqlDb)
				ret = append(ret, NewSeedJob(sqlDb))
				k++
			}
		}
		for k < len(curBranches) {
			br := curBranches[k]
			sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
			if err != nil {
				sc.error(j, err)
				continue
			}

			newDbs = append(newDbs, sqlDb)
			ret = append(ret, NewSeedJob(sqlDb))
			k++
		}
		//for i < len(branches) {
		//	sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, branches[i].GetPath(), branches[i].GetPath()+"/"+dbName)
		//	if err != nil {
		//		sc.error(j, err)
		//		continue
		//	}
		//
		//	dropDbs[dbBranchKey{sqlDb.AliasedName(), sqlDb.Revision()}] = true
		//	i++
		//}
	}

	sc.dbMu.Lock()

	if sc.ddlGuard {
		// ddl interrupted branch refresh
		sc.dbMu.Unlock()
		return sc.updateBranches(ctx)
	}
	defer sc.dbMu.Unlock()

	sc.Branches = newBranches
	sc.dbs = newDbs

	var statKeys = make(map[dbBranchKey]bool)
	for _, db := range sc.dbs {
		statKeys[dbBranchKey{db.AliasedName(), db.Revision()}] = true
	}

	newStats := make(map[tableIndexesKey][]*stats.Statistic)
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	for k, s := range sc.Stats {
		if statKeys[dbBranchKey{db: k.db, branch: k.branch}] {
			newStats[k] = s
		}
	}
	sc.Stats = newStats
	return ret, nil
}

func (sc *StatsCoord) countBuckets() int {
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	var cnt int
	for _, ss := range sc.Stats {
		cnt += len(ss)
	}
	return cnt
}

func (sc *StatsCoord) setGc() {
	if sc.enableGc.Load() {
		sc.doGc.Store(true)
	}
}

func (sc *StatsCoord) runGc(ctx context.Context, done chan struct{}) error {
	log.Println("run GC")

	sc.doGc.Store(false)
	if !sc.enableGc.Load() {
		close(done)
		return nil
	}

	sc.gcMu.Lock()
	defer sc.gcMu.Unlock()

	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return err
	}

	if err := sc.kv.StartGc(ctx, int(sc.bucketCap)); err != nil {
		return err
	}

	// can't take |dbMu| and provider lock
	sc.dbMu.Lock()
	dbs := make([]dsess.SqlDatabase, len(sc.dbs))
	copy(dbs, sc.dbs)
	sc.ddlGuard = true
	sc.dbMu.Unlock()

	var bucketCnt int
	for _, db := range dbs {
		j := NewGcMarkJob(db)
		cnt, err := sc.gcMark(sqlCtx, j)
		if sql.ErrDatabaseNotFound.Is(err) {
			// concurrent delete
			continue
		} else if err != nil {
			return err
		}
		bucketCnt += cnt
	}

	sc.bucketCnt.Store(int64(bucketCnt))
	sc.bucketCap = sc.kv.Cap()
	sc.kv.FinishGc()
	sc.activeGc.Store(false)

	return nil
}
