// Copyright 2024 Dolthub, Inc.
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

package statsnoms

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

func NewNomsStatsFactory(dialPro dbfactory.GRPCDialProvider) *NomsStatsFactory {
	return &NomsStatsFactory{dialPro: dialPro}
}

type NomsStatsFactory struct {
	dialPro dbfactory.GRPCDialProvider
}

var _ statspro.StatsFactory = NomsStatsFactory{}

func (sf NomsStatsFactory) Init(ctx *sql.Context, sourceDb dsess.SqlDatabase, prov *sqle.DoltDatabaseProvider, fs filesys.Filesys, hdp env.HomeDirProvider) (statspro.Database, error) {
	params := make(map[string]interface{})
	params[dbfactory.GRPCDialProviderParam] = sf.dialPro

	var urlPath string
	u, err := earl.Parse(prov.DbFactoryUrl())
	if u.Scheme == dbfactory.MemScheme {
		urlPath = path.Join(prov.DbFactoryUrl(), dbfactory.DoltDataDir)
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

		dEnv = env.Load(context.Background(), hdp, statsFs, urlPath, "test")
		sess := dsess.DSessFromSess(ctx.Session)
		err = dEnv.InitRepo(ctx, types.Format_Default, sess.Username(), sess.Email(), prov.DefaultBranch())
		if err != nil {
			return nil, err
		}
	} else if !isDir {
		return nil, fmt.Errorf("file exists where the dolt stats directory should be")
	} else {
		dEnv = env.LoadWithoutDB(ctx, hdp, statsFs, "")
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
	return NewNomsStats(sourceDb, statsDb), nil
}

func NewNomsStats(sourceDb, statsDb dsess.SqlDatabase) *NomsStatsDatabase {
	return &NomsStatsDatabase{mu: &sync.Mutex{}, destDb: statsDb, sourceDb: sourceDb}
}

type dbStats map[sql.StatQualifier]*statspro.DoltStats

type NomsStatsDatabase struct {
	mu           *sync.Mutex
	destDb       dsess.SqlDatabase
	sourceDb     dsess.SqlDatabase
	stats        []dbStats
	branches     []string
	tableHashes  []map[string]hash.Hash
	schemaHashes []map[string]hash.Hash
	dirty        []*prolly.MutableMap
}

var _ statspro.Database = (*NomsStatsDatabase)(nil)

func (n *NomsStatsDatabase) Close() error {
	return n.destDb.DbData().Ddb.Close()
}

func (n *NomsStatsDatabase) Branches() []string {
	return n.branches
}

func (n *NomsStatsDatabase) LoadBranchStats(ctx *sql.Context, branch string) error {
	if ok, err := n.SchemaChange(ctx, branch); err != nil {
		return err
	} else if ok {
		ctx.GetLogger().Debugf("statistics load: detected schema change incompatility, purging %s/%s", branch, n.sourceDb.Name())
		if err := n.DeleteBranchStats(ctx, branch, true); err != nil {
			return err
		}
	}

	statsMap, err := n.destDb.DbData().Ddb.GetStatistics(ctx, branch)
	if errors.Is(err, doltdb.ErrNoStatistics) {
		return n.trackBranch(ctx, branch)
	} else if errors.Is(err, datas.ErrNoBranchStats) {
		return n.trackBranch(ctx, branch)
	} else if err != nil {
		return err
	}
	if cnt, err := statsMap.Count(); err != nil {
		return err
	} else if cnt == 0 {
		return n.trackBranch(ctx, branch)
	}

	doltStats, err := loadStats(ctx, n.sourceDb, statsMap)
	if err != nil {
		return err
	}
	n.branches = append(n.branches, branch)
	n.stats = append(n.stats, doltStats)
	n.dirty = append(n.dirty, nil)
	n.tableHashes = append(n.tableHashes, make(map[string]hash.Hash))
	n.schemaHashes = append(n.schemaHashes, make(map[string]hash.Hash))
	return nil
}

func (n *NomsStatsDatabase) SchemaChange(ctx *sql.Context, branch string) (bool, error) {
	root, err := n.sourceDb.GetRoot(ctx)
	if err != nil {
		return false, err
	}
	tables, err := n.sourceDb.GetTableNames(ctx)
	if err != nil {
		return false, err
	}

	var keys []string
	var schHashes []hash.Hash
	for _, tableName := range tables {
		table, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		curHash, err := table.GetSchemaHash(ctx)
		if err != nil {
			return false, err
		}

		keys = append(keys, branch+"/"+tableName)
		schHashes = append(schHashes, curHash)
	}

	ddb := n.destDb.DbData().Ddb
	var schemaChange bool
	for i, key := range keys {
		curHash := schHashes[i]
		if val, ok, err := ddb.GetTuple(ctx, key); err != nil {
			return false, err
		} else if ok {
			oldHash := hash.Parse(string(val))
			if !ok || !oldHash.Equal(curHash) {
				schemaChange = true
				break
			}
		} else if err != nil {
			return false, err
		}
	}
	if schemaChange {
		for _, key := range keys {
			ddb.DeleteTuple(ctx, key)
		}
		return true, nil
	}
	return false, nil
}

func (n *NomsStatsDatabase) getBranchStats(branch string) dbStats {
	for i, b := range n.branches {
		if strings.EqualFold(b, branch) {
			return n.stats[i]
		}
	}
	return nil
}

func (n *NomsStatsDatabase) GetStat(branch string, qual sql.StatQualifier) (*statspro.DoltStats, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	stats := n.getBranchStats(branch)
	ret, ok := stats[qual]
	return ret, ok
}

func (n *NomsStatsDatabase) ListStatQuals(branch string) []sql.StatQualifier {
	n.mu.Lock()
	defer n.mu.Unlock()
	stats := n.getBranchStats(branch)
	var ret []sql.StatQualifier
	for qual, _ := range stats {
		ret = append(ret, qual)
	}
	return ret
}

func (n *NomsStatsDatabase) setStat(ctx context.Context, branch string, qual sql.StatQualifier, stats *statspro.DoltStats) error {
	var statsMap *prolly.MutableMap
	for i, b := range n.branches {
		if strings.EqualFold(branch, b) {
			n.stats[i][qual] = stats
			if n.dirty[i] == nil {
				if err := n.initMutable(ctx, i); err != nil {
					return err
				}
			}
			statsMap = n.dirty[i]
		}
	}
	if statsMap == nil {
		if err := n.trackBranch(ctx, branch); err != nil {
			return err
		}
		statsMap = n.dirty[len(n.branches)-1]
		n.stats[len(n.branches)-1][qual] = stats
	}

	return n.replaceStats(ctx, statsMap, stats)
}
func (n *NomsStatsDatabase) SetStat(ctx context.Context, branch string, qual sql.StatQualifier, stats *statspro.DoltStats) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.setStat(ctx, branch, qual, stats)
}

func (n *NomsStatsDatabase) trackBranch(ctx context.Context, branch string) error {
	n.branches = append(n.branches, branch)
	n.stats = append(n.stats, make(dbStats))
	n.tableHashes = append(n.tableHashes, make(map[string]hash.Hash))
	n.schemaHashes = append(n.schemaHashes, make(map[string]hash.Hash))

	kd, vd := schema.StatsTableDoltSchema.GetMapDescriptors()
	newMap, err := prolly.NewMapFromTuples(ctx, n.destDb.DbData().Ddb.NodeStore(), kd, vd)
	if err != nil {
		return err
	}
	n.dirty = append(n.dirty, newMap.Mutate())
	return n.destDb.DbData().Ddb.SetStatisics(ctx, branch, newMap.HashOf())
}

func (n *NomsStatsDatabase) initMutable(ctx context.Context, i int) error {
	statsMap, err := n.destDb.DbData().Ddb.GetStatistics(ctx, n.branches[i])
	if err != nil {
		return err
	}
	n.dirty[i] = statsMap.Mutate()
	return nil
}

func (n *NomsStatsDatabase) DeleteStats(ctx *sql.Context, branch string, quals ...sql.StatQualifier) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for i, b := range n.branches {
		if strings.EqualFold(b, branch) {
			for _, qual := range quals {
				ctx.GetLogger().Debugf("statistics refresh: deleting index statistics: %s/%s", branch, qual)
				delete(n.stats[i], qual)
			}
		}
	}
}

func (n *NomsStatsDatabase) DeleteBranchStats(ctx *sql.Context, branch string, flush bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	ctx.GetLogger().Debugf("statistics refresh: deleting branch statistics: %s", branch)

	for i, b := range n.branches {
		if strings.EqualFold(b, branch) {
			n.branches = append(n.branches[:i], n.branches[i+1:]...)
			n.dirty = append(n.dirty[:i], n.dirty[i+1:]...)
			n.stats = append(n.stats[:i], n.stats[i+1:]...)
			n.tableHashes = append(n.tableHashes[:i], n.tableHashes[i+1:]...)
			n.schemaHashes = append(n.schemaHashes[:i], n.schemaHashes[i+1:]...)
		}
	}
	if flush {
		return n.destDb.DbData().Ddb.DropStatisics(ctx, branch)
	}
	return nil
}

func (n *NomsStatsDatabase) ReplaceChunks(ctx context.Context, branch string, qual sql.StatQualifier, targetHashes []hash.Hash, dropChunks, newChunks []sql.HistogramBucket) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	var dbStat dbStats
	for i, b := range n.branches {
		if strings.EqualFold(b, branch) {
			// naive merge the new with old
			dbStat = n.stats[i]
		}
	}

	if dbStat == nil {
		if err := n.trackBranch(ctx, branch); err != nil {
			return err
		}
		dbStat = n.stats[len(n.branches)-1]
	}

	if _, ok := dbStat[qual]; ok {
		oldChunks := dbStat[qual].Hist
		targetBuckets, err := statspro.MergeNewChunks(targetHashes, oldChunks, newChunks)
		if err != nil {
			return err
		}
		newStat, err := dbStat[qual].WithHistogram(targetBuckets)
		if err != nil {
			return err
		}
		dbStat[qual] = newStat.(*statspro.DoltStats)
	} else {
		dbStat[qual] = statspro.NewDoltStats()
	}
	dbStat[qual].Chunks = targetHashes
	dbStat[qual].UpdateActive()

	// let |n.SetStats| update memory and disk
	return n.setStat(ctx, branch, qual, dbStat[qual])
}

func (n *NomsStatsDatabase) Flush(ctx context.Context, branch string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for i, b := range n.branches {
		if strings.EqualFold(b, branch) {
			if n.dirty[i] != nil {
				flushedMap, err := n.dirty[i].Map(ctx)
				if err != nil {
					return err
				}
				n.dirty[i] = nil
				if err := n.destDb.DbData().Ddb.SetStatisics(ctx, branch, flushedMap.HashOf()); err != nil {
					return err
				}
				return nil
			}
		}
	}
	return nil
}

func (n *NomsStatsDatabase) GetTableHash(branch, tableName string) hash.Hash {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, b := range n.branches {
		if strings.EqualFold(branch, b) {
			return n.tableHashes[i][tableName]
		}
	}
	return hash.Hash{}
}

func (n *NomsStatsDatabase) SetTableHash(branch, tableName string, h hash.Hash) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, b := range n.branches {
		if strings.EqualFold(branch, b) {
			n.tableHashes[i][tableName] = h
			break
		}
	}
}

func (n *NomsStatsDatabase) GetSchemaHash(ctx context.Context, branch, tableName string) (hash.Hash, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, b := range n.branches {
		if strings.EqualFold(branch, b) {
			return n.schemaHashes[i][tableName], nil
		}
		if val, ok, err := n.destDb.DbData().Ddb.GetTuple(ctx, branch+"/"+tableName); ok {
			if err != nil {
				return hash.Hash{}, err
			}
			h := hash.Parse(string(val))
			n.schemaHashes[i][tableName] = h
			return h, nil
		} else if err != nil {
			return hash.Hash{}, err
		}
		break
	}
	return hash.Hash{}, nil
}

func (n *NomsStatsDatabase) SetSchemaHash(ctx context.Context, branch, tableName string, h hash.Hash) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	branchIdx := -1
	for i, b := range n.branches {
		if strings.EqualFold(branch, b) {
			branchIdx = i
			break
		}
	}
	if branchIdx < 0 {
		branchIdx = len(n.branches)
		if err := n.trackBranch(ctx, branch); err != nil {
			return err
		}
	}

	n.schemaHashes[branchIdx][tableName] = h
	key := branch + "/" + tableName
	if err := n.destDb.DbData().Ddb.DeleteTuple(ctx, key); err != doltdb.ErrTupleNotFound {
		return err
	}

	return n.destDb.DbData().Ddb.SetTuple(ctx, key, []byte(h.String()))
}
