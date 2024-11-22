// Copyright 2019 Dolthub, Inc.
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

package dtables

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

// StatisticsTable is a sql.Table implementation that implements a system table which shows the dolt commit log
type StatisticsTable struct {
	dbName     string
	branch     string
	tableNames []string
}

var _ sql.Table = (*StatisticsTable)(nil)
var _ sql.StatisticsTable = (*StatisticsTable)(nil)

// NewStatisticsTable creates a StatisticsTable
func NewStatisticsTable(_ *sql.Context, dbName, branch string, tableNames []string) sql.Table {
	return &StatisticsTable{dbName: dbName, branch: branch, tableNames: tableNames}
}

// DataLength implements sql.StatisticsTable
func (st *StatisticsTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(schema.StatsTableSqlSchema(st.dbName).Schema)
	numRows, _, err := st.RowCount(ctx)
	if err != nil {
		return 0, err
	}

	// maxSize is the upper bound for how much space a table takes up on disk. It will typically
	// greatly overestimate the actual size of the table on disk because it does not take into
	// account that the data on disk is compressed and it assumes that every variable length
	// field is fully used. Because of this, maxSize can easily be several orders of magnitude
	// larger than the actual space used by the table on disk.
	maxSize := numBytesPerRow * numRows

	// To return a more realistic estimate of the size of the table on disk, we multiply maxSize by
	// compressionFactor. This will still not give an accurate size of the table on disk, but it
	// will generally be much closer than maxSize. This value comes from quickly testing some dbs
	// with only columns that have a fixed length (e.g. int) and some with only columns that have
	// a variable length (e.g. TEXT). 0.002 was between the two sets of values. Ultimately, having
	// accurate table statistics is a better long term solution for this.
	// https://github.com/dolthub/dolt/issues/6624
	const compressionFactor = 0.002
	estimatedSize := float64(maxSize) * compressionFactor
	return uint64(estimatedSize), nil
}

type BranchStatsProvider interface {
	GetTableDoltStats(ctx *sql.Context, branch, db, table string) ([]sql.Statistic, error)
}

// RowCount implements sql.StatisticsTable
func (st *StatisticsTable) RowCount(ctx *sql.Context) (uint64, bool, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	var cnt int
	for _, table := range st.tableNames {
		// only Dolt-specific provider has branch support
		dbStats, err := dSess.StatsProvider().(BranchStatsProvider).GetTableDoltStats(ctx, st.branch, st.dbName, table)
		if err != nil {

		}
		for _, dbStat := range dbStats {
			cnt += len(dbStat.Histogram())
		}
	}

	return uint64(cnt), true, nil
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// StatisticsTableName
func (st *StatisticsTable) Name() string {
	return doltdb.StatisticsTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// StatisticsTableName
func (st *StatisticsTable) String() string {
	return doltdb.StatisticsTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (st *StatisticsTable) Schema() sql.Schema {
	return schema.StatsTableSqlSchema(st.dbName).Schema
}

// Collation implements the sql.Table interface.
func (st *StatisticsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (st *StatisticsTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (st *StatisticsTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider().(BranchStatsProvider)
	var dStats []sql.Statistic
	for _, table := range st.tableNames {
		dbStats, err := statsPro.GetTableDoltStats(ctx, st.branch, st.dbName, table)
		if err != nil {
			return nil, err
		}
		dStats = append(dStats, dbStats...)
	}
	return stats.NewStatsIter(ctx, dStats...)
}

// PreciseMatch implements sql.IndexAddressable
func (st *StatisticsTable) PreciseMatch() bool {
	return true
}
