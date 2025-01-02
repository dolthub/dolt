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
	Done()
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

type SeedDbTablesJob struct {
	ctx    *sql.Context
	sqlDb  sqle.Database
	tables []string
	done   chan struct{}
}

func (j SeedDbTablesJob) Done() {
	close(j.done)
}

func (j SeedDbTablesJob) String() string {
	return "seed db: " + j.sqlDb.RevisionQualifiedName() + "[" + strings.Join(j.tables, ", ") + "]"
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
	//TODO implement me
	panic("implement me")
}

func (j GCJob) JobType() StatsJobType {
	//TODO implement me
	panic("implement me")
}

func (j GCJob) Done() {
	close(j.done)
	return
}

type ReadJob struct {
	ctx      *sql.Context
	db       dsess.SqlDatabase
	table    string
	m        prolly.Map
	nodes    []tree.Node
	ordinals []updateOrdinal
	done     chan struct{}
}

func (j ReadJob) Done() {
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

func (j FinalizeJob) Done() {
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

func (j ControlJob) Done() {
	close(j.done)
}

func (j ControlJob) JobType() StatsJobType {
	return StatsJobInterrupt
}

func (j ControlJob) String() string {
	return "ControlJob: " + j.desc
}

func NewStatsCoord(sleep time.Duration, logger *logrus.Logger) *StatsCoord {
	return &StatsCoord{
		dbMu:            &sync.Mutex{},
		statsMu:         &sync.Mutex{},
		logger:          logger,
		Jobs:            make(chan StatsJob, 1024),
		SleepMult:       sleep,
		BucketCache:     make(map[hash.Hash]*stats.Bucket),
		LowerBoundCache: make(map[hash.Hash]sql.Row),
		TemplateCache:   make(map[templateCacheKey]stats.Statistic),
		Stats:           make(map[tableIndexesKey][]*stats.Statistic),
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

	dbMu *sync.Mutex
	dbs  []dsess.SqlDatabase

	Jobs       chan StatsJob
	Interrupts chan ControlJob

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
	close(sc.Interrupts)
}

func (sc *StatsCoord) Start(ctx context.Context) {
	sc.Interrupts = make(chan ControlJob)
	// todo put into background threads
	go sc.run(ctx)
}

func (sc *StatsCoord) Close() {
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

func (sc *StatsCoord) putBucket(h hash.Hash, b *stats.Bucket) {
	sc.BucketCache[h] = b
}

func (sc *StatsCoord) putFirstRow(h hash.Hash, r sql.Row) {
	sc.LowerBoundCache[h] = r
}

func (sc *StatsCoord) putStatistic(h hash.Hash, r sql.Row) {
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
func (sc *StatsCoord) run(ctx context.Context) error {
	var err error
	var newJobs []StatsJob
	start := time.Now()
	ticker := time.NewTicker(time.Nanosecond)

	queuedCnt := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
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
		case j, ok := <-sc.Jobs:
			if !ok {
				return nil
			}
			queuedCnt--
			start = time.Now()
			switch j := j.(type) {
			case SeedDbTablesJob:
				newJobs, err = sc.seedDbTables(ctx, j)
			case ReadJob:
				newJobs, err = sc.readChunks(ctx, j)
			case FinalizeJob:
				newJobs, err = sc.finalizeUpdate(ctx, j)
			case GCJob:
				newJobs, err = sc.gc(ctx, j)
			case ControlJob:
				if err := j.cb(sc); err != nil {
					sc.error(j, err)
				}
			default:
			}
			for _, j := range newJobs {
				sc.Jobs <- j
				queuedCnt++
			}
			newJobs = nil

			j.Done()

			if err != nil {
				sc.error(j, err)
			}
		}
		ticker.Reset(time.Since(start) * sc.SleepMult)
	}
}

func (sc *StatsCoord) seedDbTables(ctx context.Context, j SeedDbTablesJob) ([]StatsJob, error) {
	// get list of tables, get list of indexes, partition index ranges into ordinal blocks
	// return list of IO jobs for table/index/ordinal blocks
	tableNames, err := j.sqlDb.GetTableNames(j.ctx)
	if err != nil {
		return nil, err
	}
	i := 0
	k := 0
	var deleted bool
	for i < len(tableNames) && k < len(j.tables) {
		switch strings.Compare(tableNames[i], j.tables[k]) {
		case 0:
			i++
			k++
		case -1:
			i++
		case +1:
			k++
			deleted = true
		}
	}
	if !deleted && k < len(j.tables) {
		k++
		deleted = true
	}

	var ret []StatsJob

	if deleted {
		ret = append(ret, NewGCJob())
	}

	for _, table := range tableNames {
		sqlTable, dTab, err := GetLatestTable(j.ctx, table, j.sqlDb)
		if err != nil {
			return nil, err
		}
		indexes, err := sqlTable.GetIndexes(j.ctx)
		if err != nil {
			return nil, err
		}

		schHashKey, _, err := sqlTable.IndexCacheKey(j.ctx)
		if err != nil {
			return nil, err
		}

		var isReadJobs bool
		fullIndexBuckets := make(map[templateCacheKey][]hash.Hash)
		for _, sqlIdx := range indexes {
			var idx durable.Index
			var err error
			if strings.EqualFold(sqlIdx.ID(), "PRIMARY") {
				idx, err = dTab.GetRowData(ctx)
			} else {
				idx, err = dTab.GetIndexRowData(ctx, sqlIdx.ID())
			}
			if err != nil {
				return nil, err
			}

			if err := sc.cacheTemplate(j.ctx, sqlTable, sqlIdx); err != nil {
				sc.logger.Debugf("stats collection failed to generate a statistic template: %s.%s.%s:%T; %s", j.sqlDb.RevisionQualifiedName(), table, sqlIdx, sqlIdx, err)
				continue
			}

			prollyMap := durable.ProllyMapFromIndex(idx)

			levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
			if err != nil {
				return nil, err
			}

			indexKey := templateCacheKey{h: schHashKey.Hash, idxName: sqlIdx.ID()}
			for _, n := range levelNodes {
				fullIndexBuckets[indexKey] = append(fullIndexBuckets[indexKey], n.HashOf())
			}

			readJobs, err := sc.partitionStatReadJobs(j.ctx, j.sqlDb, table, levelNodes, prollyMap)
			if err != nil {
				return nil, err
			}
			ret = append(ret, readJobs...)
			isReadJobs = isReadJobs || len(readJobs) > 0
		}
		if isReadJobs {
			// if there are any reads to perform, we follow those reads with a table finalize
			ret = append(ret, FinalizeJob{
				tableKey: tableIndexesKey{
					db:     j.sqlDb.AliasedName(),
					branch: j.sqlDb.Revision(),
					table:  table,
				},
				indexes: fullIndexBuckets,
				done:    make(chan struct{}),
			})
		}
	}
	// retry again after finishing planned work
	ret = append(ret, SeedDbTablesJob{tables: tableNames, sqlDb: j.sqlDb, ctx: j.ctx, done: make(chan struct{})})
	return ret, nil
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
		if i == 0 {
			firstRow, err := firstRowForIndex(j.ctx, prollyMap, keyBuilder, prollyMap.KeyDesc().Count())
			if err != nil {
				return nil, err
			}
			sc.putFirstRow(j.nodes[0].HashOf(), firstRow)
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
		sc.putBucket(n.HashOf(), bucket)
	}
	return nil, nil
}

func (sc *StatsCoord) finalizeUpdate(_ context.Context, j FinalizeJob) ([]StatsJob, error) {

	if len(j.indexes) == 0 {
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
func (sc *StatsCoord) gc(ctx context.Context, j GCJob) ([]StatsJob, error) {
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()

	newBucketCache := make(map[hash.Hash]*stats.Bucket)
	newLowerBoundCache := make(map[hash.Hash]sql.Row)
	newTemplateCache := make(map[templateCacheKey]stats.Statistic)

	for _, sqlDb := range sc.dbs {
		tableNames, err := sqlDb.GetTableNames(j.ctx)
		if err != nil {
			return nil, err
		}
		for _, table := range tableNames {
			sqlTable, dTab, err := GetLatestTable(j.ctx, table, sqlDb)
			print(dTab)
			if err != nil {
				return nil, err
			}
			indexes, err := sqlTable.GetIndexes(j.ctx)
			if err != nil {
				return nil, err
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
					return nil, err
				}

				schHash, _, err := sqlTable.IndexCacheKey(j.ctx)
				key := templateCacheKey{h: schHash.Hash, idxName: sqlIdx.ID()}
				if t, ok := sc.TemplateCache[key]; ok {
					newTemplateCache[key] = t
				}

				prollyMap := durable.ProllyMapFromIndex(idx)

				levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
				if err != nil {
					return nil, err
				}

				readJobs, err := sc.partitionStatReadJobs(j.ctx, sqlDb, table, levelNodes, prollyMap)
				if err != nil {
					return nil, err
				}

				for _, read := range readJobs {
					for _, node := range read.(ReadJob).nodes {
						if b, ok := sc.BucketCache[node.HashOf()]; ok {
							newBucketCache[node.HashOf()] = b
							if r, ok := sc.LowerBoundCache[node.HashOf()]; ok {
								newLowerBoundCache[node.HashOf()] = r
							}
						}
					}
				}

			}
		}
	}

	sc.BucketCache = newBucketCache

	return nil, nil
}
