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
	"log"
	"sync"
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

func NewStatsCoord(pro *sqle.DoltDatabaseProvider, ctxGen ctxFactory, logger *logrus.Logger, threads *sql.BackgroundThreads, dEnv *env.DoltEnv) *StatsCoord {
	done := make(chan struct{})
	close(done)
	kv := NewMemStats()
	return &StatsCoord{
		statsMu:        &sync.Mutex{},
		logger:         logger,
		JobInterval:    500 * time.Millisecond,
		gcInterval:     24 * time.Hour,
		branchInterval: 24 * time.Hour,
		Stats:          make(map[tableIndexesKey][]*stats.Statistic),
		dbFs:           make(map[string]filesys.Filesys),
		threads:        threads,
		senderDone:     done,
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
	dbFs           map[string]filesys.Filesys

	// ctxGen lets us fetch the most recent working root
	ctxGen ctxFactory

	cycleMu     *sync.Mutex
	cycleCtx    context.Context
	cycleCancel context.CancelFunc
	sq          *jobqueue.SerialQueue

	senderDone chan struct{}

	JobInterval    time.Duration
	gcInterval     time.Duration
	branchInterval time.Duration
	memOnly        bool
	enableGc       bool
	doGc           bool
	Debug          bool

	// kv is a content-addressed cache of histogram objects:
	// buckets, first bounds, and schema-specific statistic
	// templates.
	kv StatsKv

	// Stats tracks table statistics accessible to sessions.
	Stats   map[tableIndexesKey][]*stats.Statistic
	statsMu *sync.Mutex

	dbCnt int
	gcCnt int
}

// Stop pauses the queue and blocks until sender thread exits.
func (sc *StatsCoord) Stop(ctx context.Context) error {
	if err := sc.sq.Pause(); err != nil {
		return err
	}
	sc.cancelSender()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-sc.senderDone:
		return nil
	}
}

// Restart continues the queue and blocks until sender is running
func (sc *StatsCoord) Restart(ctx context.Context) error {
	if err := sc.Stop(ctx); err != nil {
		return err
	}
	sc.sq.Start()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc.runSender(ctx)
	}()
	wg.Wait()
	return nil
}

func (sc *StatsCoord) Close() {
	sc.sq.Stop()
	sc.cancelSender()
	return
}

func (sc *StatsCoord) AddFs(db dsess.SqlDatabase, fs filesys.Filesys) {
	sc.dbFs[db.AliasedName()] = fs
	return
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

	statCnt := len(sc.Stats)

	storageCnt, err := sc.kv.Flush(ctx)
	if err != nil {
		return dprocedures.StatsInfo{}, err
	}
	var active bool
	select {
	case <-sc.senderDone:
	default:
		active = true
	}

	return dprocedures.StatsInfo{
		DbCnt:             sc.dbCnt,
		Active:            active,
		CachedBucketCnt:   cachedBucketCnt,
		StorageBucketCnt:  storageCnt,
		CachedBoundCnt:    cachedBoundCnt,
		CachedTemplateCnt: cachedTemplateCnt,
		StatCnt:           statCnt,
		GcCounter:         sc.gcCnt,
	}, nil
}

func (sc *StatsCoord) descError(d string, err error) {
	if sc.Debug {
		log.Println("stats error: ", err.Error())
	}
	sc.logger.Errorf("stats error; job detail: %s; verbose: %s", d, err)
}
