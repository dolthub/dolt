package statspro

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/sirupsen/logrus"
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

func NewSeedJob(ctx *sql.Context, sqlDb dsess.SqlDatabase) SeedDbTablesJob {
	return SeedDbTablesJob{
		ctx:    ctx,
		sqlDb:  sqlDb,
		tables: nil,
		done:   make(chan struct{}),
	}
}

type SeedDbTablesJob struct {
	ctx    *sql.Context
	sqlDb  dsess.SqlDatabase
	tables []string
	done   chan struct{}
}

func (j SeedDbTablesJob) Done() {
	close(j.done)
}

func (j SeedDbTablesJob) String() string {
	//TODO implement me
	panic("implement me")
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
	db     dsess.SqlDatabase
	branch string
	table  string
	m      prolly.Map
	nodes  []tree.Node
	done   chan struct{}
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
	indexes map[hash.Hash][]hash.Hash
	done    chan struct{}
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
		logger:      logger,
		Jobs:        make(chan StatsJob, 1024),
		SleepMult:   sleep,
		BucketCache: make(map[hash.Hash]*stats.Bucket),
		StatsState:  make(map[hash.Hash][]*stats.Bucket),
	}
}

type StatsCoord struct {
	dbMu       *sync.Mutex
	cacheMu    *sync.Mutex
	dbs        []dsess.SqlDatabase
	logger     *logrus.Logger
	Jobs       chan StatsJob
	Interrupts chan ControlJob
	SleepMult  time.Duration
	// bucketCache are stats buckets on disk
	BucketCache map[hash.Hash]*stats.Bucket
	// statsState maps index hash to a list of bucket pointers
	// important for branches with common indexes to share pointers
	StatsState map[hash.Hash][]*stats.Bucket
}

func (sc *StatsCoord) Stop() {
	close(sc.Interrupts)
}

func (sc *StatsCoord) Start() {
	sc.Interrupts = make(chan ControlJob)

}

func (sc *StatsCoord) Close() {
	return
}

func (sc *StatsCoord) Add(ctx *sql.Context, db dsess.SqlDatabase) chan struct{} {
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

// event loop must be stopped
func (sc *StatsCoord) flushQueue(ctx context.Context) ([]StatsJob, error) {
	select {
	case _, ok := <-sc.Interrupts:
		if !ok {
			return nil, fmt.Errorf("cannot read queue while event loop is active")
		}
		// inactive event loop cannot be interrupted, discard
	default:
	}
	var ret []StatsJob
	select {
	case <-ctx.Done():
		return nil, nil
	case j, ok := <-sc.Jobs:
		if !ok {
			return nil, nil
		}
		ret = append(ret, j)
	}
	return ret, nil
}

func (sc *StatsCoord) Seed(ctx *sql.Context, sqlDb dsess.SqlDatabase) chan struct{} {
	j := NewSeedJob(ctx, sqlDb)
	sc.Jobs <- j
	return j.done
}

func (sc *StatsCoord) Interrupt(desc string, cb func(sc *StatsCoord) error) chan struct{} {
	j := NewControl(desc, cb)
	sc.Interrupts <- j
	return j.done
}

func (sc *StatsCoord) error(j StatsJob, err error) {
	sc.logger.Debugf("stats error; job detail: %s; verbose: %w", j.String(), err)
}

// statsRunner operates on stats jobs
func (sc *StatsCoord) run(ctx context.Context) error {
	var err error
	var newJobs []StatsJob
	start := time.Now()
	ticker := time.NewTicker(0)

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
				newJobs, err = seedDbTables(ctx, sc.logger, j)
			case ReadJob:
				newJobs, err = readChunks(ctx, j)
			case FinalizeJob:
				newJobs, err = finalizeUpdate(ctx, j)
			case GCJob:
				newJobs, err = gc(ctx, sc, j)
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

			j.Done()

			if err != nil {
				sc.error(j, err)
			}
		}
		ticker.Reset(time.Since(start) * sc.SleepMult)
	}
}

func seedDbTables(ctx context.Context, logger *logrus.Logger, j SeedDbTablesJob) ([]StatsJob, error) {
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
		print(dTab)
		if err != nil {
			return nil, err
		}
		iat, ok := sqlTable.(sql.IndexAddressableTable)
		if !ok {
			logger.Debugf("stats collection expected table to be indexable: %s.%s", j.sqlDb.RevisionQualifiedName(), table)
			continue
		}
		indexes, err := iat.GetIndexes(j.ctx)
		if err != nil {
			return nil, err
		}
		for _, idx := range indexes {
			readJobs, err := partitionStatReadJobs(ctx, dTab, idx)
			if err != nil {
				return nil, err
			}
			ret = append(ret, readJobs...)
		}
	}
	return ret, nil
}

func readChunks(ctx context.Context, j ReadJob) ([]StatsJob, error) {
	// check if chunk already in cache
	// if no, see if on disk and we just need to load
	// otherwise perform read to create the bucket, write to disk, update mem ref

	return nil, nil
}

func finalizeUpdate(ctx context.Context, j FinalizeJob) ([]StatsJob, error) {
	// update shared data structure now that buckets should exist
	// read through the hashes, get bucket references, update provider
	return nil, nil
}

// delete table, delete index
func gc(ctx context.Context, sc *StatsCoord, j GCJob) ([]StatsJob, error) {
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()

	newBucketCache := make(map[hash.Hash]*stats.Bucket)

	for _, sqlDb := range sc.dbs {

		// TODO: loop through all branches

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
			iat, ok := sqlTable.(sql.IndexAddressableTable)
			if !ok {
				sc.error(j, fmt.Errorf("stats collection expected table to be indexable: %s.%s", sqlDb.RevisionQualifiedName(), table))
				continue
			}
			indexes, err := iat.GetIndexes(j.ctx)
			if err != nil {
				return nil, err
			}
			for _, idx := range indexes {
				readJobs, err := partitionStatReadJobs(ctx, dTab, idx)
				if err != nil {
					return nil, err
				}
				for _, read := range readJobs {
					for _, node := range read.(ReadJob).nodes {
						if b, ok := sc.BucketCache[node.HashOf()]; ok {
							newBucketCache[node.HashOf()] = b
						}
					}
				}

			}
		}
	}

	sc.cacheMu.Lock()
	defer sc.cacheMu.Unlock()
	sc.BucketCache = newBucketCache

	return nil, nil
}
