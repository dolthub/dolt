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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro/jobqueue"
	"github.com/dolthub/dolt/go/store/hash"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type ctxFactory func(ctx context.Context) (*sql.Context, error)

func NewStatsCoord(ctx context.Context, pro *sqle.DoltDatabaseProvider, ctxGen ctxFactory, logger *logrus.Logger, threads *sql.BackgroundThreads, dEnv *env.DoltEnv) *StatsCoord {
	done := make(chan struct{})
	close(done)
	kv := NewMemStats()
	sq := jobqueue.NewSerialQueue()
	go func() {
		sq.Run(ctx)
	}()
	return &StatsCoord{
		statsMu:        &sync.Mutex{},
		logger:         logger,
		JobInterval:    500 * time.Millisecond,
		gcInterval:     24 * time.Hour,
		branchInterval: 24 * time.Hour,
		sq:             sq,
		Stats:          newRootStats(),
		fsMu:           &sync.Mutex{},
		dbFs:           make(map[string]filesys.Filesys),
		threads:        threads,
		issuerDone:     done,
		cycleMu:        &sync.Mutex{},
		kv:             kv,
		pro:            pro,
		hdp:            dEnv.GetUserHomeDir,
		dialPro:        env.NewGRPCDialProviderFromDoltEnv(dEnv),
		ctxGen:         ctxGen,
	}
}

func (sc *StatsCoord) SetMemOnly(v bool) {
	sc.memOnly = v
}

func (sc *StatsCoord) SetEnableGc(v bool) {
	sc.enableGc = v
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

	fsMu *sync.Mutex
	dbFs map[string]filesys.Filesys

	// ctxGen lets us fetch the most recent working root
	ctxGen ctxFactory

	cycleMu     *sync.Mutex
	cycleCtx    context.Context
	cycleCancel context.CancelFunc
	sq          *jobqueue.SerialQueue

	issuerDone chan struct{}

	JobInterval    time.Duration
	gcInterval     time.Duration
	branchInterval time.Duration
	memOnly        bool
	enableGc       bool
	doGc           atomic.Bool
	Debug          bool

	// kv is a content-addressed cache of histogram objects:
	// buckets, first bounds, and schema-specific statistic
	// templates.
	kv StatsKv

	// Stats tracks table statistics accessible to sessions.
	statsMu *sync.Mutex
	Stats   *rootStats
	gcCnt   atomic.Uint64
}

type rootStats struct {
	h     hash.Hash
	dbCnt int
	stats map[tableIndexesKey][]*stats.Statistic
	gcCnt int
}

func newRootStats() *rootStats {
	return &rootStats{
		h:     hash.Hash{},
		dbCnt: 0,
		stats: make(map[tableIndexesKey][]*stats.Statistic),
		gcCnt: 0,
	}
}

// Stop stops the sender thread and then pauses the queue
func (sc *StatsCoord) Stop(ctx context.Context) error {
	return sc.sq.InterruptSync(ctx, func() {
		sc.cancelSender()
		select {
		case <-ctx.Done():
			return
		case <-sc.issuerDone:
			return
		}
	})
	if err := sc.sq.Pause(); err != nil {
		return err
	}
	return nil
}

// Restart continues the queue and blocks until sender is running
func (sc *StatsCoord) Restart(ctx context.Context) error {
	sc.sq.Start()
	return sc.sq.InterruptSync(ctx, func() {
		sc.cancelSender()
		select {
		case <-ctx.Done():
			return
		case <-sc.issuerDone:
		}
		go func() {
			sc.runIssuer(ctx)
		}()
	})
}

func (sc *StatsCoord) Close() {
	sc.sq.Stop()
	sc.cancelSender()
	return
}

func (sc *StatsCoord) AddFs(ctx *sql.Context, db dsess.SqlDatabase, fs filesys.Filesys) error {
	sc.fsMu.Lock()
	firstDb := len(sc.dbFs) == 0
	sc.dbFs[db.AliasedName()] = fs
	sc.fsMu.Unlock()
	if firstDb && !sc.memOnly {
		return sc.rotateStorage(ctx)
	}
	return nil
}

func (sc *StatsCoord) Info(ctx context.Context) (dprocedures.StatsInfo, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()

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

	statCnt := len(sc.Stats.stats)

	storageCnt, err := sc.kv.Flush(ctx)
	if err != nil {
		return dprocedures.StatsInfo{}, err
	}
	var active bool
	select {
	case <-sc.issuerDone:
	default:
		active = true
	}

	return dprocedures.StatsInfo{
		DbCnt:             sc.Stats.dbCnt,
		Active:            active,
		CachedBucketCnt:   cachedBucketCnt,
		StorageBucketCnt:  storageCnt,
		CachedBoundCnt:    cachedBoundCnt,
		CachedTemplateCnt: cachedTemplateCnt,
		StatCnt:           statCnt,
		GcCounter:         sc.Stats.gcCnt,
	}, nil
}

func (sc *StatsCoord) descError(d string, err error) {
	if sc.Debug {
		log.Println("stats error: ", err.Error())
	}
	sc.logger.Errorf("stats error; job detail: %s; verbose: %s", d, err)
}
