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

package stats

import (
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
)

var ErrFailedToLoad = errors.New("failed to load statistics")

type DoltStats struct {
	level         int
	chunks        []hash.Hash
	active        map[hash.Hash]int
	RowCount      uint64
	DistinctCount uint64
	NullCount     uint64
	AvgSize       uint64
	Qual          sql.StatQualifier
	CreatedAt     time.Time
	Histogram     DoltHistogram
	Columns       []string
	Types         []sql.Type
	IdxClass      uint8
	LowerBound    sql.Row
	fds           *sql.FuncDepSet
	colSet        sql.ColSet
}

func DoltStatsFromSql(stat sql.Statistic) (*DoltStats, error) {
	hist, err := DoltHistFromSql(stat.Histogram(), stat.Types())
	if err != nil {
		return nil, err
	}
	return &DoltStats{
		Qual:          stat.Qualifier(),
		RowCount:      stat.RowCount(),
		DistinctCount: stat.DistinctCount(),
		NullCount:     stat.NullCount(),
		AvgSize:       stat.AvgSize(),
		CreatedAt:     stat.CreatedAt(),
		Histogram:     hist,
		Columns:       stat.Columns(),
		Types:         stat.Types(),
		IdxClass:      uint8(stat.IndexClass()),
		LowerBound:    stat.LowerBound(),
		fds:           stat.FuncDeps(),
		colSet:        stat.ColSet(),
	}, nil
}

func (s *DoltStats) updateActive() {
	newActive := make(map[hash.Hash]int)
	for i, hash := range s.chunks {
		newActive[hash] = i
	}
	s.active = newActive
}

func (s *DoltStats) updateCounts() {
	var newDistinct uint64
	var newRows uint64
	var newNulls uint64
	for _, b := range s.Histogram {
		newDistinct += b.DistinctCount
		newRows += b.RowCount
		newNulls += b.NullCount
	}
	s.RowCount = newRows
	s.DistinctCount = newDistinct
	s.NullCount = newNulls
}

func (s *DoltStats) toSql() sql.Statistic {
	typStrs := make([]string, len(s.Types))
	for i, typ := range s.Types {
		typStrs[i] = typ.String()
	}
	stat := stats.NewStatistic(s.RowCount, s.DistinctCount, s.NullCount, s.AvgSize, s.CreatedAt, s.Qual, s.Columns, s.Types, s.Histogram.toSql(), sql.IndexClass(s.IdxClass), s.LowerBound)
	return stat.WithColSet(s.colSet).WithFuncDeps(s.fds)
}

type DoltHistogram []DoltBucket

type DoltBucket struct {
	Chunk         hash.Hash
	RowCount      uint64
	DistinctCount uint64
	NullCount     uint64
	CreatedAt     time.Time
	Mcvs          []sql.Row
	McvCount      []uint64
	BoundCount    uint64
	UpperBound    sql.Row
}

func DoltHistFromSql(hist sql.Histogram, types []sql.Type) (DoltHistogram, error) {
	ret := make([]DoltBucket, len(hist))
	var err error
	for i, b := range hist {
		upperBound := make(sql.Row, len(b.UpperBound()))
		for i, v := range b.UpperBound() {
			upperBound[i], _, err = types[i].Convert(v)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %v to type %s", v, types[i].String())
			}
		}
		mcvs := make([]sql.Row, len(b.Mcvs()))
		for i, mcv := range b.Mcvs() {
			for _, v := range mcv {
				conv, _, err := types[i].Convert(v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert %v to type %s", v, types[i].String())
				}
				mcvs[i] = append(mcvs[i], conv)
			}
		}
		ret[i] = DoltBucket{
			RowCount:      b.RowCount(),
			DistinctCount: b.DistinctCount(),
			NullCount:     b.NullCount(),
			Mcvs:          mcvs,
			McvCount:      b.McvCounts(),
			BoundCount:    b.BoundCount(),
			UpperBound:    upperBound,
		}
	}
	return ret, nil
}

func (s DoltHistogram) toSql() []*stats.Bucket {
	ret := make([]*stats.Bucket, len(s))
	for i, b := range s {
		upperBound := make([]interface{}, len(b.UpperBound))
		copy(upperBound, b.UpperBound)
		ret[i] = stats.NewHistogramBucket(b.RowCount, b.DistinctCount, b.NullCount, b.BoundCount, upperBound, b.McvCount, b.Mcvs)
	}
	return ret
}

type indexMeta struct {
	db           string
	table        string
	index        string
	qual         sql.StatQualifier
	cols         []string
	updateChunks []tree.Node
	// [start, stop] ordinals for each chunk for update
	updateOrdinals [][]uint64
	preexisting    []DoltBucket
	allAddrs       []hash.Hash
}

func NewProvider() *Provider {
	return &Provider{
		mu:      &sync.Mutex{},
		dbStats: make(map[string]*dbStats),
	}
}

// Provider is the engine interface for reading and writing index statistics.
// Each database has its own statistics table that all tables/indexes in a db
// share.
type Provider struct {
	mu             *sync.Mutex
	latestRootAddr hash.Hash
	dbStats        map[string]*dbStats
}

// each database has one statistics table that is a collection of the
// table stats in the database
type dbStats struct {
	db                string
	stats             map[sql.StatQualifier]*DoltStats
	currentMap        prolly.Map
	latestRoot        *doltdb.RootValue
	latestTableHashes map[string]hash.Hash
}

var _ sql.StatsProvider = (*Provider)(nil)

// Init scans the statistics tables, populating the |stats| attribute.
// Statistics are not available for reading until we've finished loading.
func (p *Provider) Load(ctx *sql.Context, dbs []dsess.SqlDatabase) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, db := range dbs {
		// set map keys so concurrent orthogonal writes are OK
		p.dbStats[strings.ToLower(db.Name())] = &dbStats{db: strings.ToLower(db.Name()), stats: make(map[sql.StatQualifier]*DoltStats)}
	}
	eg, ctx := ctx.NewErrgroup()
	for _, db := range dbs {
		// copy closure variables
		dbName := strings.ToLower(db.Name())
		db := db
		eg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					if str, ok := r.(fmt.Stringer); ok {
						err = fmt.Errorf("%w: %s", ErrFailedToLoad, str.String())
					} else {
						err = fmt.Errorf("%w: %v", ErrFailedToLoad, r)
					}

					return
				}
			}()

			m, err := db.DbData().Ddb.GetStatistics(ctx)
			if errors.Is(err, doltdb.ErrNoStatistics) {
				return nil
			} else if err != nil {
				return err
			}
			stats, err := loadStats(ctx, db, m)
			if errors.Is(err, dtables.ErrIncompatibleVersion) {
				ctx.Warn(0, err.Error())
				return nil
			} else if err != nil {
				return err
			}
			p.dbStats[dbName] = stats
			return nil
		})
	}
	return eg.Wait()
}

func (p *Provider) GetTableStats(ctx *sql.Context, db, table string) ([]sql.Statistic, error) {
	var ret []sql.Statistic
	if dbStats := p.dbStats[strings.ToLower(db)]; dbStats != nil {
		for qual, stat := range p.dbStats[strings.ToLower(db)].stats {
			if strings.EqualFold(db, qual.Database) && strings.EqualFold(table, qual.Tab) {
				ret = append(ret, stat.toSql())
			}
		}
	}
	return ret, nil
}

func (p *Provider) SetStats(ctx *sql.Context, stats sql.Statistic) error {
	doltStats, err := DoltStatsFromSql(stats)
	if err != nil {
		return err
	}
	dbName := strings.ToLower(stats.Qualifier().Database)
	if _, ok := p.dbStats[dbName]; !ok {
		p.dbStats[dbName] = &dbStats{db: dbName, stats: make(map[sql.StatQualifier]*DoltStats)}
	}
	p.dbStats[dbName].stats[stats.Qualifier()] = doltStats
	return nil
}

func (p *Provider) GetStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) (sql.Statistic, bool) {
	if dbStats := p.dbStats[strings.ToLower(qual.Database)]; dbStats != nil {
		if s, ok := p.dbStats[strings.ToLower(qual.Database)].stats[qual]; ok {
			return s.toSql(), true
		}
	}
	return nil, false
}

func (p *Provider) DropStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) error {
	if dbStats := p.dbStats[strings.ToLower(qual.Database)]; dbStats != nil {
		delete(p.dbStats[strings.ToLower(qual.Database)].stats, qual)
	}
	return nil
}

func (p *Provider) RowCount(ctx *sql.Context, db, table string) (uint64, error) {
	var cnt uint64
	if dbStats := p.dbStats[strings.ToLower(db)]; dbStats != nil {
		for qual, s := range p.dbStats[strings.ToLower(db)].stats {
			if strings.EqualFold(db, qual.Database) && strings.EqualFold(table, qual.Table()) {
				if s.RowCount > cnt {
					cnt = s.RowCount
				}
			}
		}
	}
	return cnt, nil
}

func (p *Provider) DataLength(_ *sql.Context, db, table string) (uint64, error) {
	var avgSize uint64
	for meta, s := range p.dbStats[strings.ToLower(db)].stats {
		if strings.EqualFold(db, meta.Database) && strings.EqualFold(table, meta.Table()) {
			if s.AvgSize > avgSize {
				avgSize = s.AvgSize
			}
		}
	}
	return 0, nil
}

func (p *Provider) RefreshTableStats(ctx *sql.Context, table sql.Table, db string) error {
	tableName := strings.ToLower(table.Name())
	dbName := strings.ToLower(db)

	iat, ok := table.(sql.IndexAddressableTable)
	if !ok {
		return nil
	}
	indexes, err := iat.GetIndexes(ctx)
	if err != nil {
		return err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	prov := dSess.Provider()
	sqlDb, err := prov.Database(ctx, dbName)
	if err != nil {
		return err
	}
	sqlTable, ok, err := sqlDb.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("error creating statistics for table: %s; table not found", tableName)
	}

	var dTab *doltdb.Table
	switch t := sqlTable.(type) {
	case *sqle.AlterableDoltTable:
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.WritableDoltTable:
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.DoltTable:
		dTab, err = t.DoltTable(ctx)
	default:
		return fmt.Errorf("failed to unwrap dolt table from type: %T", sqlTable)
	}
	if err != nil {
		return err
	}

	curStats, ok := p.dbStats[dbName]
	if !ok {
		curStats = &dbStats{db: dbName, stats: make(map[sql.StatQualifier]*DoltStats)}
		p.dbStats[dbName] = curStats
	}

	tablePrefix := fmt.Sprintf("%s.", tableName)
	var idxMetas []indexMeta
	for _, idx := range indexes {
		cols := make([]string, len(idx.Expressions()))
		for i, c := range idx.Expressions() {
			cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
		}

		qual := sql.NewStatQualifier(db, table.Name(), strings.ToLower(idx.ID()))
		curStat, ok := curStats.stats[qual]
		if !ok {
			curStat = &DoltStats{Qual: qual, active: make(map[hash.Hash]int)}
		}
		idxMeta, err := newIdxMeta(ctx, curStat, dTab, idx, cols)
		if err != nil {
			return err
		}
		idxMetas = append(idxMetas, idxMeta)
	}

	newTableStats, err := updateStats(ctx, sqlTable, dTab, indexes, idxMetas)
	if err != nil {
		return err
	}

	if _, ok := p.dbStats[dbName]; !ok {
		p.dbStats[dbName] = &dbStats{db: strings.ToLower(db), stats: make(map[sql.StatQualifier]*DoltStats)}
	}

	// merge new chunks with preexisting chunks
	newStats := make(map[sql.StatQualifier]*DoltStats)
	for _, idxMeta := range idxMetas {
		stat := newTableStats[idxMeta.qual]
		newStats[idxMeta.qual] = mergeStatUpdates(stat, idxMeta)
	}

	prevMap := p.dbStats[dbName].currentMap
	if prevMap.KeyDesc().Count() == 0 {
		ddb, ok := dSess.GetDoltDB(ctx, dbName)
		if !ok {
			return fmt.Errorf("database not found in session for stats update: %s", db)
		}
		kd, vd := schema.StatsTableDoltSchema.GetMapDescriptors()
		prevMap, err = prolly.NewMapFromTuples(ctx, ddb.NodeStore(), kd, vd)
		if err != nil {
			return err
		}
	}
	newMap, err := flushStats(ctx, prevMap, newStats)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.dbStats[dbName].currentMap = newMap
	for k, v := range newStats {
		p.dbStats[dbName].stats[k] = v
	}
	defer p.mu.Unlock()

	ddb, ok := dSess.GetDoltDB(ctx, dbName)
	if !ok {
		return sql.ErrDatabaseNotFound.New(dbName)
	}

	return ddb.SetStatisics(ctx, newMap.HashOf())
}
