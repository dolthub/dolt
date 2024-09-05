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

	ddb, err := doltdb.LoadDoltDBWithParams(ctx, types.Format_Default, urlPath, statsFs, params)
	if err != nil {
		return nil, err
	}

	dEnv.DoltDB = ddb

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
	mu               *sync.Mutex
	destDb           dsess.SqlDatabase
	sourceDb         dsess.SqlDatabase
	stats            []dbStats
	branches         []string
	latestTableRoots []map[string]hash.Hash
	dirty            []*prolly.MutableMap
}

var _ statspro.Database = (*NomsStatsDatabase)(nil)

func (n *NomsStatsDatabase) Close() error {
	return n.destDb.DbData().Ddb.Close()
}

func (n *NomsStatsDatabase) LoadBranchStats(ctx *sql.Context, branch string) error {
	statsMap, err := n.destDb.DbData().Ddb.GetStatistics(ctx, branch)
	if errors.Is(err, doltdb.ErrNoStatistics) {
		return nil
	} else if err != nil {
		return err
	}
	doltStats, err := loadStats(ctx, n.sourceDb, statsMap)
	if err != nil {
		return err
	}
	n.branches = append(n.branches, branch)
	n.stats = append(n.stats, doltStats)
	n.dirty = append(n.dirty, nil)
	n.latestTableRoots = append(n.latestTableRoots, make(map[string]hash.Hash))
	return nil
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
	stats := n.getBranchStats(branch)
	ret, ok := stats[qual]
	return ret, ok
}

func (n *NomsStatsDatabase) ListStatQuals(branch string) []sql.StatQualifier {
	stats := n.getBranchStats(branch)
	var ret []sql.StatQualifier
	for qual, _ := range stats {
		ret = append(ret, qual)
	}
	return ret
}

func (n *NomsStatsDatabase) SetStat(ctx context.Context, branch string, qual sql.StatQualifier, stats *statspro.DoltStats) error {
	var statsMap *prolly.MutableMap
	for i, b := range n.branches {
		if strings.EqualFold(branch, b) {
			n.stats[i][qual] = stats
			if n.dirty[i] == nil {
				n.initMutable(ctx, i)
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

func (n *NomsStatsDatabase) trackBranch(ctx context.Context, branch string) error {
	n.branches = append(n.branches, branch)
	n.stats = append(n.stats, make(dbStats))
	n.latestTableRoots = append(n.latestTableRoots, make(map[string]hash.Hash))

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

func (n *NomsStatsDatabase) DeleteStats(branch string, quals ...sql.StatQualifier) {
	for i, b := range n.branches {
		if strings.EqualFold(b, branch) {
			for _, qual := range quals {
				delete(n.stats[i], qual)
			}
		}
	}
}

func (n *NomsStatsDatabase) DeleteBranchStats(ctx context.Context, branch string, flush bool) error {
	for i, b := range n.branches {
		if strings.EqualFold(b, branch) {
			n.branches = append(n.branches[:i], n.branches[i+1:]...)
			n.dirty = append(n.dirty[:i], n.dirty[i+1:]...)
			n.stats = append(n.stats[:i], n.stats[i+1:]...)
			n.latestTableRoots = append(n.latestTableRoots[:i], n.latestTableRoots[i+1:]...)
		}
	}
	if flush {
		return n.destDb.DbData().Ddb.DropStatisics(ctx, branch)
	}
	return nil
}

func (n *NomsStatsDatabase) ReplaceChunks(ctx context.Context, branch string, qual sql.StatQualifier, targetHashes []hash.Hash, dropChunks, newChunks []sql.HistogramBucket) error {
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
		dbStat[qual].Hist = targetBuckets
	} else {
		dbStat[qual] = statspro.NewDoltStats()
	}
	dbStat[qual].Chunks = targetHashes
	dbStat[qual].UpdateActive()

	// let |n.SetStats| update memory and disk
	return n.SetStat(ctx, branch, qual, dbStat[qual])
}

func (n *NomsStatsDatabase) Flush(ctx context.Context, branch string) error {
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

func (n *NomsStatsDatabase) GetLatestHash(branch, tableName string) hash.Hash {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, b := range n.branches {
		if strings.EqualFold(branch, b) {
			return n.latestTableRoots[i][tableName]
		}
	}
	return hash.Hash{}
}

func (n *NomsStatsDatabase) SetLatestHash(branch, tableName string, h hash.Hash) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, b := range n.branches {
		if strings.EqualFold(branch, b) {
			n.latestTableRoots[i][tableName] = h
			break
		}
	}
}
