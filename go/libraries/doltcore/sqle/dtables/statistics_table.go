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
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
)

// StatisticsTable is a sql.Table implementation that implements a system table which shows the dolt commit log
type StatisticsTable struct {
	dbName string
	branch string
	ddb    *doltdb.DoltDB
}

var _ sql.Table = (*StatisticsTable)(nil)
var _ sql.StatisticsTable = (*StatisticsTable)(nil)

// NewStatisticsTable creates a StatisticsTable
func NewStatisticsTable(_ *sql.Context, dbName string, ddb *doltdb.DoltDB, asOf interface{}) sql.Table {
	ret := &StatisticsTable{dbName: dbName, ddb: ddb}
	if branch, ok := asOf.(string); ok {
		ret.branch = branch
	}
	return ret
}

// DataLength implements sql.StatisticsTable
func (st *StatisticsTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(schema.StatsTableSqlSchema(st.dbName).Schema)
	numRows, _, err := st.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

type BranchStatsProvider interface {
	GetTableDoltStats(ctx *sql.Context, branch, db, table string) ([]sql.Statistic, error)
}

// RowCount implements sql.StatisticsTable
func (st *StatisticsTable) RowCount(ctx *sql.Context) (uint64, bool, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	prov := dSess.Provider()

	sqlDb, err := prov.Database(ctx, st.dbName)
	if err != nil {
		return 0, false, err
	}

	tables, err := sqlDb.GetTableNames(ctx)
	if err != nil {
		return 0, false, err
	}

	var cnt int
	for _, table := range tables {
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
	prov := dSess.Provider()

	var sqlDb sql.Database
	var err error
	if st.branch != "" {
		sqlDb, err = prov.Database(ctx, fmt.Sprintf("%s/%s", st.dbName, st.branch))
	} else {
		sqlDb, err = prov.Database(ctx, st.dbName)
	}
	if err != nil {
		return nil, err
	}

	tables, err := sqlDb.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	statsPro := dSess.StatsProvider().(BranchStatsProvider)
	var dStats []sql.Statistic
	for _, table := range tables {
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
