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

package dtables

/*
import (
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/go-mysql-server/sql"
)

// StatisticsInfoTable is a sql.Table implementation that implements a system table which shows the dolt commit log
type StatisticsInfoTable struct {
	dbName     string
	schemaName string
}

type StatsInfoProvider interface {
	GetStatsProviderInfo(ctx *sql.Context) ([]sql.Row, error)
}

var _ sql.Table = (*StatisticsInfoTable)(nil)
var _ sql.StatisticsTable = (*StatisticsInfoTable)(nil)

// NewStatisticsInfoTable creates a StatisticsInfoTable
func NewStatisticsInfoTable(_ *sql.Context, dbName, schemaName, branch string, tableNames []string) sql.Table {
	return &StatisticsInfoTable{dbName: dbName, schemaName: schemaName}
}

// DataLength implements sql.StatisticsInfoTable
func (st *StatisticsInfoTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(schema.StatsInfoSchema.Schema)
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

// RowCount implements sql.StatisticsInfoTable
func (st *StatisticsInfoTable) RowCount(ctx *sql.Context) (uint64, bool, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	prov := dSess.StatsProvider().(StatsInfoProvider)
	info, err := prov.GetStatsProviderInfo(ctx)
	if err != nil {
		return 0, false, err
	}
	return uint64(len(info)), true, nil
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// StatisticsInfoTableName
func (st *StatisticsInfoTable) Name() string {
	return doltdb.StatisticsInfoTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// StatisticsInfoTableName
func (st *StatisticsInfoTable) String() string {
	return doltdb.StatisticsInfoTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (st *StatisticsInfoTable) Schema() sql.Schema {
	return schema.StatsInfoSchema.Schema
}

// Collation implements the sql.Table interface.
func (st *StatisticsInfoTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (st *StatisticsInfoTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (st *StatisticsInfoTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	prov := dSess.StatsProvider().(StatsInfoProvider)
	infoRows, err := prov.GetStatsProviderInfo(ctx)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(infoRows...), nil
}

// PreciseMatch implements sql.IndexAddressable
func (st *StatisticsInfoTable) PreciseMatch() bool {
	return true
}
 
*/
