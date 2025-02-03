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
	"strconv"
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
	sqlDb       dsess.SqlDatabase
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
		Interrupts:     make(chan ControlJob, 1024),
		JobInterval:    50 * time.Millisecond,
		gcInterval:     24 * time.Hour,
		branchInterval: 24 * time.Hour,
		enableGc:       atomic.Bool{},
		bucketCap:      kv.Cap(),
		Stats:          make(map[tableIndexesKey][]*stats.Statistic),
		Branches:       make(map[string][]ref.DoltRef),
		dbFs:           make(map[string]filesys.Filesys),
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
	logger         *logrus.Logger
	threads        *sql.BackgroundThreads
	pro            *sqle.DoltDatabaseProvider
	statsBackingDb string
	dialPro        dbfactory.GRPCDialProvider
	hdp            env.HomeDirProvider
	// ctxGen lets us fetch the most recent working root
	ctxGen ctxFactory

	JobInterval    time.Duration
	gcInterval     time.Duration
	branchInterval time.Duration
	memOnly        bool
	Debug          bool

	Jobs chan StatsJob
	// Interrupts skip the job queue and are processed first,
	// but has a fixed size and will block
	Interrupts chan ControlJob
	Done       chan struct{}

	// XXX: do not hold the |dbMu| while accessing |pro|
	dbMu *sync.Mutex
	// dbs is a list of branch-qualified databases.
	dbs  []dsess.SqlDatabase
	dbFs map[string]filesys.Filesys
	// Branches lists the branches tracked for each database.
	// Should track |dbs|.
	Branches map[string][]ref.DoltRef

	// kv is a content-addressed cache of histogram objects:
	// buckets, first bounds, and schema-specific statistic
	// templates.
	kv StatsKv

	statsMu *sync.Mutex
	// Stats tracks table statistics accessible to sessions.
	Stats map[tableIndexesKey][]*stats.Statistic

	branchCounter atomic.Uint64
	gcCounter     atomic.Uint64

	readCounter atomic.Int32

	delayGc     atomic.Bool
	delayBranch atomic.Bool

	doGc     atomic.Bool
	enableGc atomic.Bool
	gcMu     sync.Mutex
	gcCancel context.CancelFunc

	// ddlGuard is a compare and swap that lets |updateBranches|
	// safe and nonblocking
	ddlGuard      bool
	doBranchCheck atomic.Bool
	doCapCheck    atomic.Bool
	bucketCnt     atomic.Int64
	bucketCap     int64
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
		j := NewControl("stop thread", func(sc *StatsCoord) error {
			sc.Stop()
			return nil
		})
		sc.Interrupts <- j
		select {
		case <-ctx.Done():
		case <-j.done:
		}
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

func (sc *StatsCoord) Add(ctx *sql.Context, db dsess.SqlDatabase, branch ref.DoltRef, fs filesys.Filesys) (chan struct{}, error) {
	db, err := sqle.RevisionDbForBranch(ctx, db, branch.GetPath(), branch.GetPath()+"/"+db.AliasedName())
	if err != nil {
		sc.error(ControlJob{desc: "add db"}, err)
		ret := make(chan struct{})
		close(ret)
		return ret, nil
	}

	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	sc.ddlGuard = true

	sc.Branches[db.AliasedName()] = append(sc.Branches[db.AliasedName()], ref.NewBranchRef(db.Revision()))
	sc.dbs = append(sc.dbs, db)
	sc.dbFs[db.AliasedName()] = fs
	ret, err := sc.Seed(ctx, db)
	if err != nil {
		return nil, err
	}

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
			return ret, nil
		}
		if sc.memOnly {
			return ret, nil
		}
		newKv, err := sc.initStorage(ctx, db)
		if err != nil {
			sc.error(ControlJob{desc: "add db"}, err)
			close(ret)
			return ret, nil
		}
		newKv.mem = mem
		sc.kv = newKv
	}

	return ret, nil
}

func (sc *StatsCoord) Drop(dbName string) {
	// todo: deprecate
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
	DbCnt         int
	ReadCnt       int
	Active        bool
	JobCnt        int
	GcCounter     int
	BranchCounter int
}

func (sc *StatsCoord) Info() StatsInfo {
	sc.dbMu.Lock()
	dbCnt := len(sc.dbs)
	defer sc.dbMu.Unlock()

	return StatsInfo{
		DbCnt:         dbCnt,
		ReadCnt:       int(sc.readCounter.Load()),
		Active:        true,
		JobCnt:        len(sc.Jobs),
		GcCounter:     int(sc.gcCounter.Load()),
		BranchCounter: int(sc.branchCounter.Load()),
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

func (sc *StatsCoord) Seed(ctx context.Context, sqlDb dsess.SqlDatabase) (chan struct{}, error) {
	j := NewSeedJob(sqlDb)
	//sc.Jobs <- j
	if err := sc.unsafeAsyncSend(ctx, j); err != nil {
		return nil, err
	}
	return j.done, nil
}

func (sc *StatsCoord) Control(ctx context.Context, desc string, cb func(sc *StatsCoord) error) (chan struct{}, error) {
	j := NewControl(desc, cb)
	if err := sc.unsafeAsyncSend(ctx, j); err != nil {
		return nil, err
	}
	return j.done, nil
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
			j := ControlJob{desc: "branches update"}
			newJobs, err := sc.updateBranches(ctx)
			if err != nil {
				sc.error(j, err)
			}
			err = sc.sendJobs(ctx, newJobs...)
			if err != nil {
				sc.error(j, err)
			}
			continue
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
			if sc.Debug {
				log.Println("stats interrupt job: ", j.String())
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
		case j, ok := <-sc.Interrupts:
			if !ok {
				return nil
			}
			if sc.Debug {
				log.Println("stats interrupt job: ", j.String())
			}
			if err := j.cb(sc); err != nil {
				sc.error(j, err)
				continue
			}
		case <-jobTimer.C:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case j, ok := <-sc.Jobs:
				if !ok {
					return nil
				}
				if sc.Debug {
					log.Println("stats execute: ", j.String())
				}
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
	// jobs can double and access is concurrent
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()

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
	close(sc.Jobs)
	ch := make(chan StatsJob, cap(sc.Jobs)*2)
	for j := range sc.Jobs {
		ch <- j
	}
	sc.Jobs = ch
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
			if sc.Debug {
				log.Printf("put bound: %s | %s: %v\n", j.table, firstNodeHash.String()[:5], firstRow)
			}
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
		//log.Println("read/put chunk ", n.HashOf().String()[:5])
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
		if len(fs.buckets) == 0 {
			continue
		}

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

	// We cannot mutex protect concurrent db drops
	// and finalization. We need to check afterward
	// whether there was a db/stats race. We check
	// separately for database and branch deletes.

	sc.dbMu.Lock()
	sc.ddlGuard = false
	sc.dbMu.Unlock()

	sc.statsMu.Lock()
	sc.Stats[j.tableKey] = newStats
	sc.statsMu.Unlock()

	sc.dbMu.Lock()
	if sc.ddlGuard {
		sqlCtx, err := sc.ctxGen(ctx)
		if err != nil {
			return nil, err
		}

		if _, err := j.sqlDb.GetRoot(sqlCtx); err != nil {
			sc.statsMu.Lock()
			delete(sc.Stats, j.tableKey)
			sc.statsMu.Unlock()
		}
	}
	sc.dbMu.Unlock()

	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := j.sqlDb.GetRoot(sqlCtx); err != nil {
		sc.statsMu.Lock()
		delete(sc.Stats, j.tableKey)
		sc.statsMu.Unlock()
	}

	return nil, nil
}

type dbBranchKey struct {
	db     string
	branch string
}

func (sc *StatsCoord) updateBranches(ctx context.Context) ([]StatsJob, error) {
	if sc.delayBranch.Swap(true) {
		return nil, nil
	}

	if sc.Debug {
		log.Println("stats branch check number: ", strconv.Itoa(int(sc.branchCounter.Load())))
	}
	sc.branchCounter.Add(1)

	j := ControlJob{desc: "branch update"}
	sqlCtx, err := sc.ctxGen(ctx)
	if err != nil {
		return nil, err
	}

	newBranches := make(map[string][]ref.DoltRef)
	var newDbs []dsess.SqlDatabase

	// Currenrtly, updateBranches is sensitive to concurrent
	// add/drop database. We used |ddlGuard| as a compare and
	// swap check after collecting new dbs, branches, and stats.
	// A failed guard check retries.
	// If this were incrementally adding/deleting, |ddlGuard| would
	// be unnecessary, but more complex and maybe more blocking.
	sc.dbMu.Lock()
	sc.ddlGuard = false
	dbBranches := make(map[string][]ref.DoltRef)
	for k, v := range sc.Branches {
		dbBranches[k] = v
	}
	dbs := make([]dsess.SqlDatabase, len(sc.dbs))
	copy(dbs, sc.dbs)
	sc.dbMu.Unlock()

	{
		// filter for branches that haven't been deleted
		var w int
		for i := 0; i < len(dbs); i++ {
			if _, err := dbs[i].GetRoot(sqlCtx); err != nil {
				continue
			}
			dbs[w] = dbs[i]
			w++
		}

		dbs = dbs[:w]
	}

	var ret []StatsJob
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

		// check if db still valid
		dSess := dsess.DSessFromSess(sqlCtx.Session)
		dbd, ok := dSess.GetDbData(sqlCtx, sqlDb.AliasedName())
		if !ok {
			sc.error(j, fmt.Errorf("database in branches list does not exist: %s", dbName))
			continue
		}
		curBranches, err := dbd.Ddb.GetBranches(sqlCtx)
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
				i++
				k++
				sqlDb, err := sqle.RevisionDbForBranch(sqlCtx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
				if err != nil {
					sc.error(j, err)
					continue
				}
				newDbs = append(newDbs, sqlDb)
			case -1:
				i++
			case +1:
				// add
				k++
				sqlDb, err := sqle.RevisionDbForBranch(sqlCtx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
				if err != nil {
					sc.error(j, err)
					continue
				}
				_, err = sqlDb.GetRoot(sqlCtx)
				if err != nil {
					continue
				}

				newDbs = append(newDbs, sqlDb)
				ret = append(ret, NewSeedJob(sqlDb))
			}
		}
		for k < len(curBranches) {
			br := curBranches[k]
			k++
			sqlDb, err := sqle.RevisionDbForBranch(sqlCtx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
			if err != nil {
				sc.error(j, err)
				continue
			}

			newDbs = append(newDbs, sqlDb)
			ret = append(ret, NewSeedJob(sqlDb))
		}
	}

	sc.dbMu.Lock()

	if sc.ddlGuard {
		// ddl interrupted branch refresh
		sc.dbMu.Unlock()
		return sc.updateBranches(ctx)
	}

	sc.Branches = newBranches
	sc.dbs = newDbs

	var statKeys = make(map[dbBranchKey]bool)
	for _, db := range sc.dbs {
		statKeys[dbBranchKey{db.AliasedName(), db.Revision()}] = true
	}
	sc.dbMu.Unlock()

	newStats := make(map[tableIndexesKey][]*stats.Statistic)
	sc.statsMu.Lock()
	for k, s := range sc.Stats {
		if statKeys[dbBranchKey{db: k.db, branch: k.branch}] {
			newStats[k] = s
		}
	}
	sc.Stats = newStats
	sc.statsMu.Unlock()

	// Avoid branch checks starving the loop, only re-enable after
	// letting a block of other work through.
	ret = append(ret, NewControl("re-enable branch check", func(sc *StatsCoord) error {
		sc.delayBranch.Store(false)
		return nil
	}))

	return ret, nil
}

func (sc *StatsCoord) setGc() {
	if sc.enableGc.Load() {
		sc.doGc.Store(true)
	}
}
