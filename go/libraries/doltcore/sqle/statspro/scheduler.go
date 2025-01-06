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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/sirupsen/logrus"
	"io"
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
	name     string
	schHash  hash.Hash
	idxRoots []hash.Hash
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
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
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

func NewStatsCoord(sleep time.Duration, logger *logrus.Logger, threads *sql.BackgroundThreads) *StatsCoord {
	return &StatsCoord{
		dbMu:            &sync.Mutex{},
		statsMu:         &sync.Mutex{},
		logger:          logger,
		Jobs:            make(chan StatsJob, 1024),
		Done:            make(chan struct{}),
		Interrupts:      make(chan ControlJob),
		SleepMult:       sleep,
		gcInterval:      24 * time.Hour,
		BucketCache:     make(map[hash.Hash]*stats.Bucket),
		LowerBoundCache: make(map[hash.Hash]sql.Row),
		TemplateCache:   make(map[templateCacheKey]stats.Statistic),
		Stats:           make(map[tableIndexesKey][]*stats.Statistic),
		threads:         threads,
	}
}

type tableIndexesKey struct {
	db     string
	branch string
	table  string
}

type StatsCoord struct {
	logger    *logrus.Logger
	SleepMult time.Duration
	threads   *sql.BackgroundThreads

	dbMu *sync.Mutex
	dbs  []sqle.Database

	readCounter atomic.Int32
	doGc        atomic.Bool
	disableGc   atomic.Bool
	gcInterval  time.Duration

	Jobs       chan StatsJob
	Interrupts chan ControlJob
	Done       chan struct{}

	// BucketCache are in-memory stats buckets, always tracked
	// on disk
	BucketCache map[hash.Hash]*stats.Bucket
	// LowerBoundCache saves lower bounds for first buckets
	LowerBoundCache map[hash.Hash]sql.Row
	// TemplateCache saves statistic templates based on table
	// schema + index name
	TemplateCache map[templateCacheKey]stats.Statistic

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
	sc.dbMu.Lock()
	sc.dbs = append(sc.dbs, db)
	sc.dbMu.Unlock()
	return sc.Seed(ctx, db)
}

func (sc *StatsCoord) Drop(dbName string) {
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

func (sc *StatsCoord) putBucket(h hash.Hash, b *stats.Bucket) {
	sc.BucketCache[h] = b
}

func (sc *StatsCoord) putFirstRow(h hash.Hash, r sql.Row) {
	sc.LowerBoundCache[h] = r
}

// event loop must be stopped
func (sc *StatsCoord) flushQueue(ctx context.Context) ([]StatsJob, error) {
	select {
	case _, ok := <-sc.Interrupts:
		if ok {
			return nil, fmt.Errorf("cannot read queue while event loop is active")
		}
		// inactive event loop cannot be interrupted, discard
	default:
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

func (sc *StatsCoord) error(j StatsJob, err error) {
	sc.logger.Debugf("stats error; job detail: %s; verbose: %s", j.String(), err)
}

// statsRunner operates on stats jobs
func (sc *StatsCoord) run(ctx *sql.Context) error {
	var start time.Time
	jobTimer := time.NewTimer(0)
	gcTicker := time.NewTicker(sc.gcInterval)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-jobTimer.C:
		case <-gcTicker.C:
			if sc.doGc.Load() {
				if err := sc.gc(ctx); err != nil {
					sc.error(GCJob{}, err)
				}
				sc.doGc.Store(false)
			}
		case <-sc.Done:
			return nil
		case j, ok := <-sc.Interrupts:
			if !ok {
				return nil
			}
			if err := j.cb(sc); err != nil {
				sc.error(j, err)
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sc.Done:
			return nil
		case j, ok := <-sc.Jobs:
			if !ok {
				return nil
			}
			start = time.Now()
			newJobs, err := sc.executeJob(ctx, j)
			if err != nil {
				sc.error(j, err)
			}
			err = sc.sendJobs(ctx, newJobs)
			if err != nil {
				sc.error(j, err)
			}
			j.Finish()
		}
		jobTimer.Reset(time.Since(start) * sc.SleepMult)
	}
}

func (sc *StatsCoord) sendJobs(ctx *sql.Context, jobs []StatsJob) error {
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
	sc.Stop()
	ch := make(chan StatsJob, cap(sc.Jobs)*2)
	for j := range sc.Jobs {
		ch <- j
	}
	sc.Jobs = ch
	sc.Restart(ctx)
}

func (sc *StatsCoord) seedDbTables(ctx context.Context, j SeedDbTablesJob) ([]StatsJob, error) {
	// get list of tables, get list of indexes, partition index ranges into ordinal blocks
	// return list of IO jobs for table/index/ordinal blocks
	tableNames, err := j.sqlDb.GetTableNames(j.ctx)
	if err != nil {
		return nil, err
	}

	var newTableInfo []tableStatsInfo
	var ret []StatsJob

	i := 0
	k := 0
	for i < len(tableNames) && k < len(j.tables) {
		var jobs []StatsJob
		var ti tableStatsInfo
		switch strings.Compare(tableNames[i], j.tables[k].name) {
		case 0:
			// continue
			jobs, ti, err = sc.readJobsForTable(j.ctx, j.sqlDb, j.tables[k])
			i++
			k++
		case -1:
			// new table
			jobs, ti, err = sc.readJobsForTable(j.ctx, j.sqlDb, tableStatsInfo{name: tableNames[i]})
			i++
		case +1:
			// dropped table
			jobs = append(jobs, sc.dropTableJob(j.sqlDb, j.tables[k].name))
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
		newTableInfo = append(newTableInfo, ti)
		ret = append(ret, jobs...)
		i++
	}

	for k < len(j.tables) {
		ret = append(ret, sc.dropTableJob(j.sqlDb, j.tables[k].name))
		k++
	}

	// retry again after finishing planned work
	ret = append(ret, SeedDbTablesJob{tables: newTableInfo, sqlDb: j.sqlDb, ctx: j.ctx, done: make(chan struct{})})
	return ret, nil
}

func (sc *StatsCoord) readJobsForTable(ctx *sql.Context, sqlDb sqle.Database, tableInfo tableStatsInfo) ([]StatsJob, tableStatsInfo, error) {
	var ret []StatsJob
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
		sc.doGc.Store(true)
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
		if i < len(tableInfo.idxRoots) && idxRoot.Equal(tableInfo.idxRoots[i]) && !schemaChanged {
			continue
		}
		dataChanged = true

		levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
		if err != nil {
			return nil, tableStatsInfo{}, err
		}

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

	return ret, tableStatsInfo{name: tableInfo.name, schHash: schHashKey.Hash, idxRoots: newIdxRoots}, nil
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
	if _, ok := sc.TemplateCache[key]; ok {
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

	sc.TemplateCache[key] = stats.Statistic{
		Cols:     nil,
		Typs:     types,
		IdxClass: uint8(class),
		Fds:      fds,
		Colset:   colset,
	}
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
		sc.putBucket(n.HashOf(), bucket)
	}
	return nil, nil
}

func (sc *StatsCoord) finalizeUpdate(_ context.Context, j FinalizeJob) ([]StatsJob, error) {
	if len(j.indexes) == 0 {
		// delete table
		sc.statsMu.Lock()
		delete(sc.Stats, j.tableKey)
		sc.statsMu.Unlock()
		return nil, nil
	}

	var newStats []*stats.Statistic
	for key, bucketHashes := range j.indexes {
		template, ok := sc.TemplateCache[key]
		if !ok {
			return nil, fmt.Errorf("failed to finalize update, missing template dependency for table: %s", key)
		}
		template.Qual = sql.NewStatQualifier(j.tableKey.db, "", j.tableKey.table, key.idxName)

		for i, bh := range bucketHashes {
			if i == 0 {
				var ok bool
				template.LowerBnd, ok = sc.LowerBoundCache[bh]
				if !ok {
					return nil, fmt.Errorf("failed to finalize update, missing read job bucket dependency for chunk: %s", bh)
				}
			}
			// accumulate counts
			if b, ok := sc.BucketCache[bh]; !ok {
				return nil, fmt.Errorf("failed to finalize update, missing read job bucket dependency for chunk: %s", bh)
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
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()

	newBucketCache := make(map[hash.Hash]*stats.Bucket)
	newLowerBoundCache := make(map[hash.Hash]sql.Row)
	newTemplateCache := make(map[templateCacheKey]stats.Statistic)

	for _, sqlDb := range sc.dbs {
		tableNames, err := sqlDb.GetTableNames(ctx)
		if err != nil {
			return err
		}
		for _, table := range tableNames {
			sqlTable, dTab, err := GetLatestTable(ctx, table, sqlDb)
			print(dTab)
			if err != nil {
				return err
			}
			indexes, err := sqlTable.GetIndexes(ctx)
			if err != nil {
				return err
			}
			for _, sqlIdx := range indexes {
				var idx durable.Index
				var err error
				if strings.EqualFold(sqlIdx.ID(), "PRIMARY") {
					idx, err = dTab.GetRowData(ctx)
				} else {
					idx, err = dTab.GetIndexRowData(ctx, sqlIdx.ID())
				}
				if err != nil {
					return err
				}

				schHash, _, err := sqlTable.IndexCacheKey(ctx)
				key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
				if t, ok := sc.TemplateCache[key]; ok {
					newTemplateCache[key] = t
				}

				prollyMap := durable.ProllyMapFromIndex(idx)

				levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
				if err != nil {
					return err
				}

				if r, ok := sc.LowerBoundCache[levelNodes[0].HashOf()]; ok {
					newLowerBoundCache[levelNodes[0].HashOf()] = r
				}
				for _, node := range levelNodes {
					if b, ok := sc.BucketCache[node.HashOf()]; ok {
						newBucketCache[node.HashOf()] = b
					}
				}

			}
		}
	}

	sc.BucketCache = newBucketCache
	sc.TemplateCache = newTemplateCache
	sc.LowerBoundCache = newLowerBoundCache

	return nil
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
