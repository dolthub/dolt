// Copyright 2023 Dolthub, Inc.
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
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

var ErrFailedToLoad = errors.New("failed to load statistics")

type indexMeta struct {
	qual     sql.StatQualifier
	cols     []string
	newNodes []tree.Node
	// updateOrdinals are [start, stop] tuples for each update chunk
	updateOrdinals []updateOrdinal
	keepChunks     []DoltBucket
	dropChunks     []DoltBucket
	allAddrs       []hash.Hash
}

type updateOrdinal struct {
	start, stop uint64
}

func NewProvider(pro *sqle.DoltDatabaseProvider, sf StatsFactory) *Provider {
	return &Provider{
		pro:       pro,
		sf:        sf,
		mu:        &sync.Mutex{},
		statDbs:   make(map[string]Database),
		cancelers: make(map[string]context.CancelFunc),
		status:    make(map[string]string),
	}
}

// Provider is the engine interface for reading and writing index statistics.
// Each database has its own statistics table that all tables/indexes in a db
// share.
type Provider struct {
	mu        *sync.Mutex
	pro       *sqle.DoltDatabaseProvider
	sf        StatsFactory
	statDbs   map[string]Database
	cancelers map[string]context.CancelFunc
	starter   sqle.InitDatabaseHook
	status    map[string]string
}

// each database has one statistics table that is a collection of the
// table stats in the database
type dbToStats struct {
	mu                *sync.Mutex
	dbName            string
	stats             map[sql.StatQualifier]*DoltStats
	statsDatabase     Database
	latestTableHashes map[string]hash.Hash
}

func newDbStats(dbName string) *dbToStats {
	return &dbToStats{
		mu:                &sync.Mutex{},
		dbName:            dbName,
		stats:             make(map[sql.StatQualifier]*DoltStats),
		latestTableHashes: make(map[string]hash.Hash),
	}
}

var _ sql.StatsProvider = (*Provider)(nil)

func (p *Provider) StartRefreshThread(ctx *sql.Context, pro dsess.DoltDatabaseProvider, name string, env *env.DoltEnv, db dsess.SqlDatabase) error {
	err := p.starter(ctx, pro.(*sqle.DoltDatabaseProvider), name, env, db)
	p.mu.Lock()
	defer p.mu.Unlock()

	if err != nil {
		p.UpdateStatus(name, fmt.Sprintf("error restarting thread %s: %s", name, err.Error()))
		return err
	}
	p.UpdateStatus(name, fmt.Sprintf("restarted thread: %s", name))
	return nil
}

func (p *Provider) SetStarter(hook sqle.InitDatabaseHook) {
	p.starter = hook
}

func (p *Provider) CancelRefreshThread(dbName string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cancel, ok := p.cancelers[dbName]; ok {
		cancel()
		p.UpdateStatus(dbName, fmt.Sprintf("cancelled thread: %s", dbName))
	}
}

func (p *Provider) ThreadStatus(dbName string) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if msg, ok := p.status[dbName]; ok {
		return msg
	}
	return "no active stats thread"
}

func (p *Provider) GetTableStats(ctx *sql.Context, db, table string) ([]sql.Statistic, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return nil, nil
	}

	return p.GetTableDoltStats(ctx, branch, db, table)
}

func (p *Provider) GetTableDoltStats(ctx *sql.Context, branch, db, table string) ([]sql.Statistic, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	statDb, ok := p.getStatDb(db)
	if !ok || statDb == nil {
		return nil, nil
	}

	if branch == "" {
		dSess := dsess.DSessFromSess(ctx.Session)
		var err error
		branch, err = dSess.GetBranch()
		if err != nil {
			return nil, nil
		}
	}

	var ret []sql.Statistic
	for _, qual := range statDb.ListStatQuals(branch) {
		if strings.EqualFold(db, qual.Database) && strings.EqualFold(table, qual.Tab) {
			stat, _ := statDb.GetStat(branch, qual)
			ret = append(ret, stat.toSql())
		}
	}

	return ret, nil
}

func (p *Provider) setStatDb(name string, db Database) {
	p.statDbs[name] = db
}

func (p *Provider) getStatDb(name string) (Database, bool) {
	statDb, ok := p.statDbs[strings.ToLower(name)]
	return statDb, ok
}

func (p *Provider) SetStats(ctx *sql.Context, s sql.Statistic) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	statDb, ok := p.getStatDb(s.Qualifier().Db())
	if !ok {
		return nil
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return nil
	}

	doltStat, err := DoltStatsFromSql(s)
	if err != nil {
		return err
	}

	p.UpdateStatus(s.Qualifier().Db(), fmt.Sprintf("refreshed %s", s.Qualifier().Db()))

	return statDb.SetStat(ctx, branch, s.Qualifier(), doltStat)
}

func (p *Provider) getQualStats(ctx *sql.Context, qual sql.StatQualifier) (*DoltStats, bool) {
	statDb, ok := p.getStatDb(qual.Db())
	if !ok {
		return nil, false
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return nil, false
	}

	return statDb.GetStat(branch, qual)
}

func (p *Provider) GetStats(ctx *sql.Context, qual sql.StatQualifier, _ []string) (sql.Statistic, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	stat, ok := p.getQualStats(ctx, qual)
	if !ok {
		return nil, false
	}
	return stat.toSql(), true
}

func (p *Provider) DropDbStats(ctx *sql.Context, db string, flush bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	statDb, ok := p.getStatDb(db)
	if !ok {
		return nil
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return err
	}

	// remove provider access
	if err := statDb.DeleteBranchStats(ctx, branch, flush); err != nil {
		return nil
	}

	p.status[db] = "dropped"

	return nil
}

func (p *Provider) DropStats(ctx *sql.Context, qual sql.StatQualifier, _ []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	statDb, ok := p.getStatDb(qual.Db())
	if !ok {
		return nil
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return nil
	}

	if _, ok := statDb.GetStat(branch, qual); ok {
		statDb.DeleteStats(branch, qual)
		p.UpdateStatus(qual.Db(), fmt.Sprintf("dropped statisic: %s", qual.String()))
	}

	return nil
}

func (p *Provider) UpdateStatus(db string, msg string) {
	p.status[db] = msg
}

func (p *Provider) RowCount(ctx *sql.Context, db, table string) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	statDb, ok := p.getStatDb(db)
	if !ok {
		return 0, sql.ErrDatabaseNotFound.New(db)
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return 0, err
	}

	priStats, ok := statDb.GetStat(branch, sql.NewStatQualifier(db, table, "primary"))
	if !ok {
		return 0, nil
	}

	return priStats.RowCount, nil
}

func (p *Provider) DataLength(ctx *sql.Context, db, table string) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	statDb, ok := p.getStatDb(db)
	if !ok {
		return 0, sql.ErrDatabaseNotFound.New(db)
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return 0, err
	}

	priStats, ok := statDb.GetStat(branch, sql.NewStatQualifier(db, table, "primary"))
	if !ok {
		return 0, nil
	}

	return priStats.AvgSize, nil
}
