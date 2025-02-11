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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
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

// todo refactor so we can count buckets globally
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
	idxLen   int
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
		if b.Len() > 100 {
			b.WriteString("...")
			break
		}
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
			if b.Len() > 20 {
				b.WriteString("...")
				break
			}
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

// NewStop lets caller block until run thread exits
func NewStop() StopJob {
	return StopJob{done: make(chan struct{})}
}

type StopJob struct {
	done chan struct{}
}

func (j StopJob) Finish() {
	close(j.done)
}

func (j StopJob) String() string {
	return "StopJob"
}

type ctxFactory func(ctx context.Context) (*sql.Context, error)

func NewStatsCoord(pro *sqle.DoltDatabaseProvider, ctxGen ctxFactory, logger *logrus.Logger, threads *sql.BackgroundThreads, dEnv *env.DoltEnv) *StatsCoord {
	done := make(chan struct{})
	close(done)
	kv := NewMemStats()
	return &StatsCoord{
		dbMu:           &sync.Mutex{},
		stopMu:         &sync.Mutex{},
		statsMu:        &sync.Mutex{},
		logger:         logger,
		Jobs:           make(chan StatsJob, 1024),
		Done:           done,
		Interrupts:     make(chan StatsJob, 1024),
		JobInterval:    500 * time.Millisecond,
		gcInterval:     24 * time.Hour,
		branchInterval: 24 * time.Hour,
		enableGc:       atomic.Bool{},
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
	sc.JobInterval = time.Duration(job)
	sc.gcInterval = time.Duration(gc)
	sc.branchInterval = time.Duration(branch)
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
	Interrupts chan StatsJob
	Done       chan struct{}
	stopMu     *sync.Mutex

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

	// Stats tracks table statistics accessible to sessions.
	Stats   map[tableIndexesKey][]*stats.Statistic
	statsMu *sync.Mutex

	branchCounter atomic.Uint64
	gcCounter     atomic.Uint64

	readCounter atomic.Int32

	doGc         atomic.Bool
	enableGc     atomic.Bool
	enableBrSync atomic.Bool
	gcMu         sync.Mutex

	// ddlGuard is a compare and swap that lets |updateBranches|
	// safe and nonblocking
	ddlGuard     bool
	doBranchSync atomic.Bool
	doCapCheck   atomic.Bool
	seedCnt      atomic.Int64
}

// Stop blocks until |sc.Done| is closed and the |run| thread exits.
func (sc *StatsCoord) Stop(ctx context.Context) error {
	sc.stopMu.Lock()
	defer sc.stopMu.Unlock()
	return sc.lockedStop(ctx)
}

func (sc *StatsCoord) lockedStop(ctx context.Context) error {
	select {
	case <-sc.Done:
		return nil
	default:
	}
	j := NewStop()
	if err := sc.unsafeAsyncSend(ctx, j); err != nil {
		close(j.done)
		return err
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-j.done:
		return nil
	}
	return nil
}

func (sc *StatsCoord) Restart(ctx context.Context) error {
	sc.stopMu.Lock()
	defer sc.stopMu.Unlock()
	return sc.lockedRestart(ctx)
}

func (sc *StatsCoord) lockedRestart(ctx context.Context) error {
	if err := sc.lockedStop(ctx); err != nil {
		return err
	}
	sc.Done = make(chan struct{})
	if err := sc.threads.Add("stats", func(ctx context.Context) {
		sc.run(ctx)
	}); err != nil {
		return err
	}

	return nil
}

func (sc *StatsCoord) Close() {
	select {
	case <-sc.Done:
	default:
		close(sc.Done)
	}
	return
}

func (sc *StatsCoord) Add(ctx *sql.Context, db dsess.SqlDatabase, branch ref.DoltRef, fs filesys.Filesys, keepStorage bool) (chan struct{}, error) {
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

	if len(sc.dbs) == 1 && !keepStorage {
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

func (sc *StatsCoord) Info(ctx context.Context) (dprocedures.StatsInfo, error) {
	sc.dbMu.Lock()
	dbCnt := len(sc.dbs)
	cachedBucketCnt := sc.kv.Len()
	var cachedBoundCnt int
	var cachedTemplateCnt int
	switch kv := sc.kv.(type) {
	case *memStats:
		cachedBoundCnt = len(kv.bounds)
		cachedTemplateCnt = len(kv.templates)
	case *prollyStats:
		cachedBoundCnt = len(kv.mem.bounds)
		cachedTemplateCnt = len(kv.mem.templates)
	}
	defer sc.dbMu.Unlock()

	sc.statsMu.Lock()
	statCnt := len(sc.Stats)
	defer sc.statsMu.Unlock()

	storageCnt, err := sc.kv.Flush(ctx)
	if err != nil {
		return dprocedures.StatsInfo{}, err
	}
	var active bool
	select {
	case <-sc.Done:
	default:
		active = true
	}

	return dprocedures.StatsInfo{
		DbCnt:             dbCnt,
		ReadCnt:           int(sc.readCounter.Load()),
		Active:            active,
		DbSeedCnt:         int(sc.seedCnt.Load()),
		CachedBucketCnt:   cachedBucketCnt,
		StorageBucketCnt:  storageCnt,
		CachedBoundCnt:    cachedBoundCnt,
		CachedTemplateCnt: cachedTemplateCnt,
		StatCnt:           statCnt,
		GcCounter:         int(sc.gcCounter.Load()),
		SyncCounter:       int(sc.branchCounter.Load()),
	}, nil
}

// captureFlushQueue is a debug method that lets us inspect and
// restore the job queue
func (sc *StatsCoord) captureFlushQueue(ctx context.Context) ([]StatsJob, error) {
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
	if err := sc.unsafeAsyncSend(ctx, j); err != nil {
		return nil, err
	}
	sc.seedCnt.Add(1)
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
	if sc.Debug {
		log.Println("stats error: ", err.Error())
	}
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
		// (4) interrupt queue
		// (5) job and other tickers
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

		if sc.doBranchSync.Swap(false) {
			j := ControlJob{desc: "branches update"}
			newJobs, err := sc.runBranchSync(ctx, make(chan struct{}))
			if err != nil {
				sc.error(j, err)
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
			if sc.Debug {
				log.Println("stats interrupt job: ", j.String())
			}
			if _, ok := j.(StopJob); ok {
				defer j.Finish()
				defer close(sc.Done)
				return nil
			}
			err := sc.executeJob(ctx, j)
			if err != nil {
				sc.error(j, err)
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
			if _, ok := j.(StopJob); ok {
				defer j.Finish()
				defer close(sc.Done)
				return nil
			}
			err := sc.executeJob(ctx, j)
			if err != nil {
				sc.error(j, err)
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
					log.Println("stats execute job: ", j.String())
				}
				if _, ok := j.(StopJob); ok {
					defer j.Finish()
					defer close(sc.Done)
					return nil
				}
				err := sc.executeJob(ctx, j)
				if err != nil {
					sc.error(j, err)
				}
			default:
			}
		case <-gcTicker.C:
			sc.setGc()
		case <-branchTicker.C:
			sc.doBranchSync.Store(true)
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

func (sc *StatsCoord) executeJob(ctx context.Context, j StatsJob) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			err = fmt.Errorf("stats job %s panicked: %s", j.String(), r)
		}
	}()
	var newJobs []StatsJob
	switch j := j.(type) {
	case SeedDbTablesJob:
		newJobs, err = sc.seedDbTables(ctx, j)
	case ReadJob:
		sc.readCounter.Add(-1)
		newJobs, err = sc.readChunks(ctx, j)
	case FinalizeJob:
		newJobs, err = sc.finalizeUpdate(ctx, j)
	case ControlJob:
		if err := j.cb(sc); err != nil {
			sc.error(j, err)
		}
	case AnalyzeJob:
		newJobs, err = sc.runAnalyze(ctx, j)
	default:
		return fmt.Errorf("unknown job type: %T", j)
	}
	if err != nil {
		return err
	}
	err = sc.sendJobs(ctx, newJobs...)
	if err != nil {
		sc.error(j, err)
	}
	j.Finish()
	return nil
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
	updater := newBucketBuilder(sql.StatQualifier{}, j.idxLen, prollyMap.KeyDesc())
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(j.idxLen))

	// all kv puts are guarded by |gcMu| to avoid concurrent
	// GC with stale data discarding some or all state
	sc.gcMu.Lock()
	defer sc.gcMu.Unlock()

	if j.first {
		sc.kv.PutTemplate(j.key, j.template)

		firstNodeHash := j.nodes[0].HashOf()
		if _, ok := sc.kv.GetBound(firstNodeHash, j.idxLen); !ok {
			firstRow, err := firstRowForIndex(j.ctx, prollyMap, keyBuilder)
			if err != nil {
				if err != nil {
					return nil, err
				}
			}
			if sc.Debug {
				log.Printf("put bound: %s | %s: %v\n", j.table, firstNodeHash.String()[:5], firstRow)
			}
			sc.kv.PutBound(firstNodeHash, firstRow, j.idxLen)
		}
	}

	for i, n := range j.nodes {
		if _, ok, err := sc.kv.GetBucket(ctx, n.HashOf(), keyBuilder); err != nil {
			return nil, err
		} else if ok {
			// concurrent reads overestimate shared buckets
			continue
		}
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
		bucket, err := updater.finalize(ctx, prollyMap.NodeStore())
		if err != nil {
			return nil, err
		}
		err = sc.kv.PutBucket(ctx, n.HashOf(), bucket, keyBuilder)
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
				bnd, ok := sc.kv.GetBound(bh, fs.tupB.Desc.Count())
				if !ok {
					return nil, fmt.Errorf("missing read job bound dependency for chunk %s: %s/%d", key, bh, fs.tupB.Desc.Count())
				}
				template.LowerBnd = bnd
			}
			// accumulate counts
			if b, ok, err := sc.kv.GetBucket(ctx, bh, fs.tupB); err != nil {
				return nil, err
			} else if !ok {
				return nil, fmt.Errorf("missing read job bucket dependency for chunk: %s/%d", bh, fs.tupB.Desc.Count())
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

func (sc *StatsCoord) runBranchSync(ctx context.Context, done chan struct{}) ([]StatsJob, error) {
	if !sc.enableBrSync.Swap(false) {
		close(done)
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

	// Currently, updateBranches is sensitive to concurrent
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
				sc.seedCnt.Add(1)
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
			sc.seedCnt.Add(1)
		}
	}

	sc.dbMu.Lock()

	if sc.ddlGuard {
		// ddl interrupted branch refresh
		sc.dbMu.Unlock()
		return sc.runBranchSync(ctx, done)
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
		sc.enableBrSync.Store(true)
		close(done)
		return nil
	}))

	return ret, nil
}

func (sc *StatsCoord) setGc() {
	if sc.enableGc.Load() {
		sc.doGc.Store(true)
	}
}
