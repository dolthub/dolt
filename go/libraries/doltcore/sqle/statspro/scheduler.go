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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/sirupsen/logrus"
	"io"
	"path"
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

type StatsJobType uint8

const (
	StatsJobLoad StatsJobType = iota
	StatsJobAnalyze
	StatsJobUpdate
	StatsJobInterrupt
)

type StatsJob interface {
	JobType() StatsJobType
	Finish()
	String() string
}

var _ StatsJob = (*ReadJob)(nil)
var _ StatsJob = (*GCJob)(nil)
var _ StatsJob = (*SeedDbTablesJob)(nil)
var _ StatsJob = (*ControlJob)(nil)

func NewSeedJob(ctx *sql.Context, sqlDb sqle.Database) SeedDbTablesJob {
	return SeedDbTablesJob{
		ctx:    ctx,
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
	ctx    *sql.Context
	sqlDb  sqle.Database
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

		b.WriteString("]")
	}
	return b.String()
}

func (j SeedDbTablesJob) JobType() StatsJobType {
	//TODO implement me
	panic("implement me")
}

func NewGCJob() GCJob {
	return GCJob{done: make(chan struct{})}
}

type GCJob struct {
	// centralized bucket collector needs to be GC'd periodically
	// how do we trigger? schema change, table change, db change, bucket count threshold
	ctx  *sql.Context
	done chan struct{}
}

func (j GCJob) String() string {
	return "gc"
}

func (j GCJob) JobType() StatsJobType {
	//TODO implement me
	panic("implement me")
}

func (j GCJob) Finish() {
	close(j.done)
	return
}

func NewAnalyzeJob(ctx *sql.Context, sqlDb sqle.Database, tables []string, after ControlJob) AnalyzeJob {
	return AnalyzeJob{ctx: ctx, sqlDb: sqlDb, tables: tables, after: after, done: make(chan struct{})}
}

type AnalyzeJob struct {
	ctx    *sql.Context
	sqlDb  sqle.Database
	tables []string
	after  ControlJob
	done   chan struct{}
}

func (j AnalyzeJob) String() string {
	//TODO implement me
	panic("implement me")
}

func (j AnalyzeJob) JobType() StatsJobType {
	//TODO implement me
	panic("implement me")
}

func (j AnalyzeJob) Finish() {
	close(j.done)
	return
}

type ReadJob struct {
	ctx      *sql.Context
	db       sqle.Database
	table    string
	m        prolly.Map
	nodes    []tree.Node
	ordinals []updateOrdinal
	done     chan struct{}
}

func (j ReadJob) Finish() {
	close(j.done)
}

func (j ReadJob) JobType() StatsJobType {
	//TODO implement me
	panic("implement me")
}

func (j ReadJob) String() string {
	b := strings.Builder{}
	b.WriteString("read: " + j.db.RevisionQualifiedName() + "/" + j.table + ": ")
	sep := ""
	for _, o := range j.ordinals {
		b.WriteString(fmt.Sprintf("%s[%d-%d]", sep, o.start, o.stop))
		sep = ", "
	}
	return b.String()
}

type FinalizeJob struct {
	tableKey tableIndexesKey
	indexes  map[templateCacheKey][]hash.Hash
	done     chan struct{}
}

func (j FinalizeJob) Finish() {
	close(j.done)
}

func (j FinalizeJob) JobType() StatsJobType {
	//TODO implement me
	panic("implement me")
}

func (j FinalizeJob) String() string {
	b := strings.Builder{}
	b.WriteString("finalize " + j.tableKey.String())
	b.WriteString(": ")
	sep := ""
	for idx, hashes := range j.indexes {
		b.WriteString(fmt.Sprintf("%s(%s: ", sep, idx.idxName))
		sep = ""
		for _, h := range hashes {
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

func (j ControlJob) JobType() StatsJobType {
	return StatsJobInterrupt
}

func (j ControlJob) String() string {
	return "ControlJob: " + j.desc
}

func NewStatsCoord(sleep time.Duration, kv StatsKv, logger *logrus.Logger, threads *sql.BackgroundThreads, dEnv *env.DoltEnv) *StatsCoord {
	done := make(chan struct{})
	close(done)
	return &StatsCoord{
		dbMu:           &sync.Mutex{},
		statsMu:        &sync.Mutex{},
		logger:         logger,
		Jobs:           make(chan StatsJob, 1024),
		Done:           done,
		Interrupts:     make(chan ControlJob, 1),
		JobInterval:    sleep,
		gcInterval:     24 * time.Hour,
		branchInterval: 24 * time.Hour,
		capInterval:    1 * time.Minute,
		bucketCap:      defaultBucketSize,
		Stats:          make(map[tableIndexesKey][]*stats.Statistic),
		Branches:       make(map[string][]ref.DoltRef),
		threads:        threads,
		kv:             kv,
		hdp:            dEnv.GetUserHomeDir,
		dialPro:        env.NewGRPCDialProviderFromDoltEnv(dEnv),
	}
}

type tableIndexesKey struct {
	db     string
	branch string
	table  string
}

func (k tableIndexesKey) String() string {
	return k.db + "/" + k.branch + "/" + k.table
}

type StatsCoord struct {
	logger      *logrus.Logger
	JobInterval time.Duration
	threads     *sql.BackgroundThreads
	pro         *sqle.DoltDatabaseProvider

	dbMu           *sync.Mutex
	dbs            []sqle.Database
	branchInterval time.Duration
	capInterval    time.Duration

	kv StatsKv

	statsBackingDb string
	cancelSwitch   context.CancelFunc
	dialPro        dbfactory.GRPCDialProvider
	hdp            env.HomeDirProvider

	readCounter atomic.Int32

	activeGc   atomic.Bool
	doGc       atomic.Bool
	disableGc  atomic.Bool
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
	close(sc.Done)
}

func (sc *StatsCoord) Restart(ctx *sql.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sc.Done:
	default:
		sc.Stop()
	}

	sc.Done = make(chan struct{})
	return sc.threads.Add("stats", func(_ context.Context) {
		sc.run(ctx)
	})
}

func (sc *StatsCoord) Close() {
	sc.Stop()
	return
}

func (sc *StatsCoord) Add(ctx *sql.Context, db sqle.Database) chan struct{} {
	dSess := dsess.DSessFromSess(ctx.Session)
	dbd, ok := dSess.GetDbData(ctx, db.AliasedName())
	if !ok {
		sc.error(ControlJob{desc: "add db"}, fmt.Errorf("database in branches list does not exist: %s", db.AliasedName()))
		ret := make(chan struct{})
		close(ret)
		return ret
	}
	curBranches, err := dbd.Ddb.GetBranches(ctx)
	if err != nil {
		sc.error(ControlJob{desc: "add db"}, err)
		ret := make(chan struct{})
		close(ret)
		return ret
	}

	ret := sc.Seed(ctx, db)

	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	sc.dbs = append(sc.dbs, db)
	sc.Branches[db.AliasedName()] = curBranches
	if len(sc.dbs) == 1 {
		sc.statsBackingDb = db.AliasedName()
		var mem *memStats
		switch kv := sc.kv.(type) {
		case *memStats:
			mem = kv
		case *prollyStats:
			mem = kv.mem
		default:
			mem, err = NewMemStats(defaultBucketSize)
			if err != nil {
				sc.error(ControlJob{desc: "add db"}, err)
			}
			close(ret)
			return ret
		}
		newKv, err := NewProllyStats(ctx, db)
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

	var active bool
	select {
	case _, ok := <-sc.Interrupts:
		active = ok
	default:
		active = true
	}
	return StatsInfo{
		DbCnt:   dbCnt,
		ReadCnt: int(sc.readCounter.Load()),
		Active:  active,
		JobCnt:  len(sc.Jobs),
	}
}

func (sc *StatsCoord) putBucket(ctx context.Context, h hash.Hash, b *stats.Bucket, tupB *val.TupleBuilder) error {
	return sc.kv.PutBucket(ctx, h, b, tupB)
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

// TODO sendJobs
func (sc *StatsCoord) Seed(ctx *sql.Context, sqlDb sqle.Database) chan struct{} {
	j := NewSeedJob(ctx, sqlDb)
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

func GcSweep(ctx *sql.Context) ControlJob {
	return NewControl("finish GC", func(sc *StatsCoord) error {
		sc.gcMu.Lock()
		defer sc.gcMu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
			sc.kv.FinishGc()
			sc.bucketCnt.Store(int64(sc.kv.Len()))
			sc.activeGc.Store(false)
			close(sc.gcDone)
			sc.gcCancel = nil
			return nil
		}
	})
}

func (sc *StatsCoord) error(j StatsJob, err error) {
	fmt.Println(err.Error())
	sc.logger.Debugf("stats error; job detail: %s; verbose: %s", j.String(), err)
}

// statsRunner operates on stats jobs
func (sc *StatsCoord) run(ctx *sql.Context) error {
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
			j := sc.startGcMark(ctx, make(chan struct{}))
			err := sc.sendJobs(ctx, j)
			if err != nil {
				sc.error(j, err)
			}
		}

		if sc.doBranchCheck.Swap(false) {
			j := ControlJob{desc: "branch update"}
			newJobs, err := sc.updateBranches(ctx, j)
			if err != nil {
				sc.error(ControlJob{desc: "branches update"}, err)
			}
			err = sc.sendJobs(ctx, newJobs...)
			if err != nil {
				sc.error(j, err)
			}
		}

		select {
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

func (sc *StatsCoord) sendJobs(ctx *sql.Context, jobs ...StatsJob) error {
	for i := 0; i < len(jobs); i++ {
		j := jobs[i]
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

func (sc *StatsCoord) executeJob(ctx *sql.Context, j StatsJob) ([]StatsJob, error) {
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

func (sc *StatsCoord) doubleChannelSize(ctx *sql.Context) {
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

func (sc *StatsCoord) runOneInterrupt(ctx *sql.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case j, ok := <-sc.Interrupts:
		if !ok {
			return nil
		}
		if err := j.cb(sc); err != nil {
			return err
		}
	default:
	}
	return nil
}

func (sc *StatsCoord) seedDbTables(_ context.Context, j SeedDbTablesJob) ([]StatsJob, error) {
	// get list of tables, get list of indexes, partition index ranges into ordinal blocks
	// return list of IO jobs for table/index/ordinal blocks
	tableNames, err := j.sqlDb.GetTableNames(j.ctx)
	if err != nil {
		if errors.Is(err, doltdb.ErrBranchNotFound) {
			return []StatsJob{sc.dropBranchJob(j.sqlDb.AliasedName(), j.sqlDb.Revision())}, nil
		}
		return nil, err
	}

	var newTableInfo []tableStatsInfo
	var ret []StatsJob

	var bucketDiff int

	i := 0
	k := 0
	for i < len(tableNames) && k < len(j.tables) {
		var jobs []StatsJob
		var ti tableStatsInfo
		switch strings.Compare(tableNames[i], j.tables[k].name) {
		case 0:
			// continue
			jobs, ti, err = sc.readJobsForTable(j.ctx, j.sqlDb, j.tables[k])
			bucketDiff += ti.bucketCount - j.tables[k].bucketCount
			i++
			k++
		case -1:
			// new table
			jobs, ti, err = sc.readJobsForTable(j.ctx, j.sqlDb, tableStatsInfo{name: tableNames[i]})
			bucketDiff += ti.bucketCount
			i++
		case +1:
			// dropped table
			jobs = append(jobs, sc.dropTableJob(j.sqlDb, j.tables[k].name))
			bucketDiff -= j.tables[k].bucketCount
			k++
		}
		if err != nil {
			return nil, err
		}
		if ti.name != "" {
			newTableInfo = append(newTableInfo, ti)
		}
		ret = append(ret, jobs...)
	}
	for i < len(tableNames) {
		jobs, ti, err := sc.readJobsForTable(j.ctx, j.sqlDb, tableStatsInfo{name: tableNames[i]})
		if err != nil {
			return nil, err
		}
		bucketDiff += ti.bucketCount
		newTableInfo = append(newTableInfo, ti)
		ret = append(ret, jobs...)
		i++
	}

	for k < len(j.tables) {
		ret = append(ret, sc.dropTableJob(j.sqlDb, j.tables[k].name))
		bucketDiff -= j.tables[k].bucketCount
		k++
	}

	sc.bucketCnt.Add(int64(bucketDiff))

	for sc.bucketCnt.Load() > sc.bucketCap {
		sc.bucketCap *= 2
		sc.doGc.Store(true)
	}

	// retry again after finishing planned work
	ret = append(ret, SeedDbTablesJob{tables: newTableInfo, sqlDb: j.sqlDb, ctx: j.ctx, done: make(chan struct{})})
	return ret, nil
}

func (sc *StatsCoord) readJobsForTable(ctx *sql.Context, sqlDb sqle.Database, tableInfo tableStatsInfo) ([]StatsJob, tableStatsInfo, error) {
	var ret []StatsJob
	var bucketCnt int
	sqlTable, dTab, err := GetLatestTable(ctx, tableInfo.name, sqlDb)
	if err != nil {
		return nil, tableStatsInfo{}, err
	}
	indexes, err := sqlTable.GetIndexes(ctx)
	if err != nil {
		return nil, tableStatsInfo{}, err
	}

	schHashKey, _, err := sqlTable.IndexCacheKey(ctx)
	if err != nil {
		return nil, tableStatsInfo{}, err
	}

	schemaChanged := !tableInfo.schHash.Equal(schHashKey.Hash)
	if schemaChanged {
		sc.setGc()
	}

	var dataChanged bool
	var isNewData bool
	var newIdxRoots []hash.Hash

	fullIndexBuckets := make(map[templateCacheKey][]hash.Hash)
	for i, sqlIdx := range indexes {
		var idx durable.Index
		var err error
		if strings.EqualFold(sqlIdx.ID(), "PRIMARY") {
			idx, err = dTab.GetRowData(ctx)
		} else {
			idx, err = dTab.GetIndexRowData(ctx, sqlIdx.ID())
		}
		if err != nil {
			return nil, tableStatsInfo{}, err
		}

		if err := sc.cacheTemplate(ctx, sqlTable, sqlIdx); err != nil {
			sc.logger.Debugf("stats collection failed to generate a statistic template: %s.%s.%s:%T; %s", sqlDb.RevisionQualifiedName(), tableInfo.name, sqlIdx, sqlIdx, err)
			continue
		}

		prollyMap := durable.ProllyMapFromIndex(idx)

		idxRoot := prollyMap.Node().HashOf()
		newIdxRoots = append(newIdxRoots, idxRoot)

		levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
		if err != nil {
			return nil, tableStatsInfo{}, err
		}

		bucketCnt += len(levelNodes)

		if i < len(tableInfo.idxRoots) && idxRoot.Equal(tableInfo.idxRoots[i]) && !schemaChanged && !sc.activeGc.Load() {
			continue
		}
		dataChanged = true

		indexKey := templateCacheKey{h: schHashKey.Hash, idxName: sqlIdx.ID()}
		for _, n := range levelNodes {
			fullIndexBuckets[indexKey] = append(fullIndexBuckets[indexKey], n.HashOf())
		}

		readJobs, err := sc.partitionStatReadJobs(ctx, sqlDb, tableInfo.name, levelNodes, prollyMap)
		if err != nil {
			return nil, tableStatsInfo{}, err
		}
		ret = append(ret, readJobs...)
		isNewData = isNewData || len(readJobs) > 0
	}
	if isNewData || schemaChanged || dataChanged {
		// if there are any reads to perform, we follow those reads with a table finalize
		ret = append(ret, FinalizeJob{
			tableKey: tableIndexesKey{
				db:     sqlDb.AliasedName(),
				branch: sqlDb.Revision(),
				table:  tableInfo.name,
			},
			indexes: fullIndexBuckets,
			done:    make(chan struct{}),
		})
	}

	return ret, tableStatsInfo{name: tableInfo.name, schHash: schHashKey.Hash, idxRoots: newIdxRoots, bucketCount: bucketCnt}, nil
}

func (sc *StatsCoord) dropTableJob(sqlDb sqle.Database, tableName string) StatsJob {
	return FinalizeJob{
		tableKey: tableIndexesKey{
			db:     sqlDb.AliasedName(),
			branch: sqlDb.Revision(),
			table:  tableName,
		},
		indexes: nil,
		done:    make(chan struct{}),
	}
}

func (sc *StatsCoord) dropBranchJob(dbName string, branch string) ControlJob {
	return ControlJob{
		desc: "drop branch",
		cb: func(sc *StatsCoord) error {
			sc.dbMu.Lock()
			defer sc.dbMu.Unlock()
			curRefs := sc.Branches[branch]
			for i, ref := range curRefs {
				if strings.EqualFold(ref.GetPath(), branch) {
					sc.Branches[branch] = append(curRefs[:i], curRefs[:i+1]...)
					break
				}
			}
			for i, db := range sc.dbs {
				if strings.EqualFold(db.Revision(), branch) && strings.EqualFold(db.AliasedName(), dbName) {
					sc.dbs = append(sc.dbs[:i], sc.dbs[1+1:]...)
					break
				}
			}

			// stats lock is more contentious, do last
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

type templateCacheKey struct {
	h       hash.Hash
	idxName string
}

func (k templateCacheKey) String() string {
	return k.idxName + "/" + k.h.String()
}

func (sc *StatsCoord) cacheTemplate(ctx *sql.Context, sqlTable *sqle.DoltTable, sqlIdx sql.Index) error {
	schHash, _, err := sqlTable.IndexCacheKey(ctx)
	key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
	if _, ok := sc.kv.GetTemplate(key); ok {
		return nil
	}
	fds, colset, err := stats.IndexFds(sqlTable.Name(), sqlTable.Schema(), sqlIdx)
	if err != nil {
		return err
	}

	var class sql.IndexClass
	switch {
	case sqlIdx.IsSpatial():
		class = sql.IndexClassSpatial
	case sqlIdx.IsFullText():
		class = sql.IndexClassFulltext
	default:
		class = sql.IndexClassDefault
	}

	var types []sql.Type
	for _, cet := range sqlIdx.ColumnExpressionTypes() {
		types = append(types, cet.Type)
	}

	tablePrefix := sqlTable.Name() + "."
	cols := make([]string, len(sqlIdx.Expressions()))
	for i, c := range sqlIdx.Expressions() {
		cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
	}

	sc.kv.PutTemplate(key, stats.Statistic{
		Cols:     nil,
		Typs:     types,
		IdxClass: uint8(class),
		Fds:      fds,
		Colset:   colset,
	})
	return nil
}

func (sc *StatsCoord) readChunks(ctx context.Context, j ReadJob) ([]StatsJob, error) {
	// check if chunk already in cache
	// if no, see if on disk and we just need to load
	// otherwise perform read to create the bucket, write to disk, update mem ref
	prollyMap := j.m
	updater := newBucketBuilder(sql.StatQualifier{}, prollyMap.KeyDesc().Count(), prollyMap.KeyDesc())
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc())

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
		bucket, err := updater.finalize(ctx, prollyMap.NodeStore())
		if err != nil {
			return nil, err
		}
		err = sc.kv.PutBucket(ctx, n.HashOf(), bucket, val.NewTupleBuilder(prollyMap.KeyDesc()))
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (sc *StatsCoord) finalizeUpdate(ctx context.Context, j FinalizeJob) ([]StatsJob, error) {
	if len(j.indexes) == 0 {
		// delete table
		sc.statsMu.Lock()
		delete(sc.Stats, j.tableKey)
		sc.statsMu.Unlock()
		return nil, nil
	}

	var newStats []*stats.Statistic
	for key, bucketHashes := range j.indexes {
		template, ok := sc.kv.GetTemplate(key)
		if !ok {
			return nil, fmt.Errorf(" missing template dependency for table: %s", key)
		}
		template.Qual = sql.NewStatQualifier(j.tableKey.db, "", j.tableKey.table, key.idxName)

		for i, bh := range bucketHashes {
			if i == 0 {
				var ok bool
				template.LowerBnd, ok = sc.kv.GetBound(bh)
				if !ok {
					return nil, fmt.Errorf("missing read job bucket dependency for chunk: %s", bh)
				}
			}
			// accumulate counts
			if b, ok, err := sc.kv.GetBucket(ctx, bh, nil); err != nil {
				return nil, err
			} else if !ok {
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
	sc.statsMu.Unlock()

	return nil, nil
}

// delete table, delete index
func (sc *StatsCoord) gc(ctx *sql.Context) error {
	return nil
	//sc.dbMu.Lock()
	//newStorage := sc.statsEncapsulatingDb
	//newKv, err := sc.kv.NewEmpty(ctx)
	//if err != nil {
	//	return err
	//}
	//sc.dbMu.Unlock()
	//return sc.gcWithStorageSwap(ctx, newStorage, newKv)
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

func (sc *StatsCoord) updateBranches(ctx *sql.Context, j ControlJob) ([]StatsJob, error) {
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	var ret []StatsJob
	newBranches := make(map[string][]ref.DoltRef)
	var newDbs []sqle.Database
	for dbName, branches := range sc.Branches {
		var sqlDb sqle.Database
		for _, db := range sc.dbs {
			if strings.EqualFold(db.AliasedName(), dbName) {
				sqlDb = db
				break
			}
		}

		if sqlDb.Name() == "" {
			sc.error(j, fmt.Errorf("database in branches list is not tracked: %s", dbName))
			continue
		}

		dSess := dsess.DSessFromSess(ctx.Session)
		dbd, ok := dSess.GetDbData(ctx, dbName)
		if !ok {
			sc.error(j, fmt.Errorf("database in branches list does not exist: %s", dbName))
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
				newDbs = append(newDbs, sqlDb.(sqle.Database))
				i++
				k++
			case -1:
				//ret = append(ret, sc.dropBranchJob(ctx, dbName, branches[i]))
				i++
			case +1:
				// add
				sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
				if err != nil {
					sc.error(j, err)
					continue
				}

				newDbs = append(newDbs, sqlDb.(sqle.Database))
				ret = append(ret, NewSeedJob(ctx, sqlDb.(sqle.Database)))
				k++
			}
		}
		if k < len(curBranches) {
			br := curBranches[k]
			sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
			if err != nil {
				sc.error(j, err)
				continue
			}

			newDbs = append(newDbs, sqlDb.(sqle.Database))
			ret = append(ret, NewSeedJob(ctx, sqlDb.(sqle.Database)))
			k++
		}
	}
	sc.Branches = newBranches
	sc.dbs = newDbs
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

func (sc *StatsCoord) initStorage(ctx *sql.Context, fs filesys.Filesys, defaultBranch string) (*prollyStats, error) {
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

		dEnv = env.Load(context.Background(), sc.hdp, statsFs, urlPath, "test")
		sess := dsess.DSessFromSess(ctx.Session)
		err = dEnv.InitRepo(ctx, types.Format_Default, sess.Username(), sess.Email(), defaultBranch)
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

func (sc *StatsCoord) setGc() {
	if !sc.disableGc.Load() {
		sc.doGc.Store(true)
	}
}

func (sc *StatsCoord) startGcMark(ctx *sql.Context, done chan struct{}) StatsJob {
	sc.doGc.Store(false)
	if sc.disableGc.Load() {
		close(done)
		return nil
	}
	sc.gcMu.Lock()
	defer sc.gcMu.Unlock()
	if sc.activeGc.Swap(true) {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-sc.gcDone:
				close(done)
			}
		}()
		return nil
	}

	subCtx, cancel := context.WithCancel(ctx)
	sc.gcCancel = cancel

	sc.kv.StartGc(ctx, int(sc.bucketCap))

	sc.gcDone = make(chan struct{})
	go func(ctx context.Context) {
		defer close(done)
		select {
		case <-ctx.Done():
			close(sc.gcDone)
			return
		case <-sc.gcDone:
		}
	}(subCtx)
	return GcSweep(ctx)
}
