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

package statspro

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

// Database is a backing store for a collection of DoltStats.
// Each stats database tracks a user database, with multiple
// branches potentially each having their own statistics.
type Database interface {
	// ListStatQuals returns the list of index statistics for a branch.
	ListStatQuals(branch string) []sql.StatQualifier
	// LoadBranchStats starts tracking a specific branch's statistics.
	LoadBranchStats(ctx *sql.Context, branch string) error
	// DeleteBranchStats removes references to in memory index statistics.
	// If |flush| is true delete the data from storage.
	DeleteBranchStats(ctx *sql.Context, branch string, flush bool) error
	// GetStat returns a branch's index statistics.
	GetStat(branch string, qual sql.StatQualifier) (*DoltStats, bool)
	//SetStat bulk replaces the statistic, deleting any previous version
	SetStat(ctx context.Context, branch string, qual sql.StatQualifier, stats *DoltStats) error
	//DeleteStats deletes a list of index statistics.
	DeleteStats(ctx *sql.Context, branch string, quals ...sql.StatQualifier)
	// ReplaceChunks is an update interface that lets a stats implementation
	// decide how to edit stats for a stats refresh.
	ReplaceChunks(ctx context.Context, branch string, qual sql.StatQualifier, targetHashes []hash.Hash, dropChunks, newChunks []sql.HistogramBucket) error
	// Flush instructs the database to sync any partial state to disk
	Flush(ctx context.Context, branch string) error
	// Close finalizes any file references.
	Close() error
	SetLatestHash(branch, tableName string, h hash.Hash)
	GetLatestHash(branch, tableName string) hash.Hash
	Branches() []string
}

// StatsFactory instances construct statistic databases.
type StatsFactory interface {
	// Init gets a reference to the stats database for a dolt database
	// rooted at the given filesystem. It will create the database if
	// it does not exist.
	Init(ctx *sql.Context, sourceDb dsess.SqlDatabase, prov *sqle.DoltDatabaseProvider, fs filesys.Filesys, hdp env.HomeDirProvider) (Database, error)
}
