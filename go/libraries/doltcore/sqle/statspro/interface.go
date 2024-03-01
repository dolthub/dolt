package statspro

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/go-mysql-server/sql"
)

// Database is a backing store for a collection of DoltStats.
// Each stats database tracks a user database, with multiple
// branches potentially each having their own statistics.
type Database interface {
	ListStatQuals(branch string) []sql.StatQualifier
	// Load calls a startup routine for tracking a specific branch's statistics.
	Load(ctx *sql.Context, branch string) error
	GetStat(branch string, qual sql.StatQualifier) (*DoltStats, bool)
	//SetStat bulk replaces the statistic, deleting any previous version
	SetStat(ctx context.Context, branch string, qual sql.StatQualifier, stats *DoltStats) error
	DeleteStats(branch string, quals ...sql.StatQualifier)
	DeleteBranchStats(ctx context.Context, branch string) error
	// ReplaceChunks is an update interface that lets a stats implementation
	// decide how to edit stats for a stats refresh.
	ReplaceChunks(ctx context.Context, branch string, qual sql.StatQualifier, targetHashes []hash.Hash, dropChunks, newChunks []DoltBucket)
	// Flush instructs the database to sync any partial state to disk
	Flush(ctx context.Context, branch string) error
	SetLatestHash(branch, tableName string, h hash.Hash)
	GetLatestHash(branch, tableName string) hash.Hash
}

// StatsFactory instances construct statistic databases.
type StatsFactory interface {
	// Init gets a reference to the stats database for a dolt database
	// rooted at the given filesystem. It will create the database if
	// it does not exist.
	Init(ctx context.Context, fs filesys.Filesys, hdp env.HomeDirProvider) (Database, error)
}
