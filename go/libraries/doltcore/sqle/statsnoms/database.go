package statsnoms

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type NomsStatsFactory struct{}

var _ statspro.StatsFactory = NomsStatsFactory{}

func (sf NomsStatsFactory) Init(ctx context.Context, fs filesys.Filesys, hdp env.HomeDirProvider) (statspro.Database, error) {
	absPath, err := fs.Abs(dbfactory.DoltStatsDir)
	if err != nil {
		return nil, err
	}

	exists, isDir := fs.Exists(dbfactory.DoltStatsDir)
	if !exists {
		urlStr := earl.FileUrlFromPath(filepath.ToSlash(absPath), os.PathSeparator)
		_, _, _, err := dbfactory.CreateDB(ctx, types.Format_Default, urlStr, nil)
		if err != nil {
			return nil, err
		}
	} else if !isDir {
		return nil, fmt.Errorf("file exists where the dolt stats directory should be")
	}

	dEnv := env.LoadWithoutDB(ctx, hdp, fs, "")
	ddb, err := doltdb.LoadDoltDB(ctx, types.Format_Default, dbfactory.DoltStatsDir, fs)
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
	sqlDb, err := sqle.NewDatabase(ctx, "stats", dEnv.DbData(), opts)
	if err != nil {
		return nil, err
	}
	return NewNomsStats(sqlDb), nil
}

func NewNomsStats(db dsess.SqlDatabase) *NomsStatsDatabase {
	return &NomsStatsDatabase{mu: &sync.Mutex{}, db: db}
}

type dbStats map[sql.StatQualifier]*statspro.DoltStats

type NomsStatsDatabase struct {
	mu               *sync.Mutex
	db               dsess.SqlDatabase
	stats            []dbStats
	branches         []string
	latestTableRoots []map[string]hash.Hash
	dirty            []*prolly.MutableMap
}

var _ statspro.Database = (*NomsStatsDatabase)(nil)

func (n *NomsStatsDatabase) Load(ctx *sql.Context, branch string) error {
	statsMap, err := n.db.DbData().Ddb.GetStatistics(ctx, branch)
	if err != nil {
		return err
	}
	doltStats, err := loadStats(ctx, n.db, statsMap)
	if err != nil {
		return err
	}
	n.branches = append(n.branches, branch)
	n.stats = append(n.stats, doltStats)
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

	return replaceStats(ctx, statsMap, stats)
}

func (n *NomsStatsDatabase) initMutable(ctx context.Context, i int) error {
	statsMap, err := n.db.DbData().Ddb.GetStatistics(ctx, n.branches[i])
	if err != nil {
		return err
	}
	n.dirty[i] = statsMap.Mutate()
	return nil
}

func (n *NomsStatsDatabase) DeleteStats(branch string, quals ...sql.StatQualifier) {
	//TODO implement me
	panic("implement me")
}

func (n *NomsStatsDatabase) DeleteBranchStats(ctx context.Context, branch string) error {
	return n.db.DbData().Ddb.DropStatisics(ctx, branch)
}

func (n *NomsStatsDatabase) ReplaceChunks(ctx context.Context, branch string, qual sql.StatQualifier, targetHashes []hash.Hash, _, newChunks []statspro.DoltBucket) {
	for i, b := range n.branches {
		if strings.EqualFold(b, branch) {
			// naive merge the new with old
			dbStat := n.stats[i][qual]
			oldChunks := dbStat.Histogram
			targetBuckets := statspro.MergeNewChunks(targetHashes, oldChunks, newChunks)
			dbStat.Chunks = targetHashes
			dbStat.Histogram = targetBuckets
			dbStat.UpdateActive()
			// let |n.SetStats| update memory and disk
			n.SetStat(ctx, branch, qual, dbStat)
		}
	}
}

func (n *NomsStatsDatabase) Flush(ctx context.Context, branch string) error {
	for i, b := range n.branches {
		if strings.EqualFold(b, branch) {
			flushedMap, err := n.dirty[i].Map(ctx)
			if err != nil {
				return err
			}
			n.dirty[i] = nil
			n.db.DbData().Ddb.SetStatisics(ctx, branch, flushedMap.HashOf())
			return nil
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
