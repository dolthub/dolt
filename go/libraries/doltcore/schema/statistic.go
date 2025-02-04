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

package schema

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	stypes "github.com/dolthub/dolt/go/store/types"
)

const StatsVersion int64 = 1

const (
	StatsQualifierColName     = "qualifier"
	StatsDbColName            = "database_name"
	StatsTableColName         = "table_name"
	StatsIndexColName         = "index_name"
	StatsPositionColName      = "position"
	StatsCommitHashColName    = "commit_hash"
	StatsRowCountColName      = "row_count"
	StatsDistinctCountColName = "distinct_count"
	StatsNullCountColName     = "null_count"
	StatsColumnsColName       = "columns"
	StatsTypesColName         = "types"
	StatsUpperBoundColName    = "upper_bound"
	StatsUpperBoundCntColName = "upper_bound_cnt"
	StatsCreatedAtColName     = "created_at"
	StatsMcv1ColName          = "mcv1"
	StatsMcv2ColName          = "mcv2"
	StatsMcv3ColName          = "mcv3"
	StatsMcv4ColName          = "mcv4"
	StatsMcvCountsColName     = "mcv_counts"
	StatsVersionColName       = "version"
)

const (
	StatsDbTag uint64 = iota
	StatsTableTag
	StatsIndexTag
	StatsPositionTag
	StatsVersionTag
	StatsCommitHashTag
	StatsRowCountTag
	StatsDistinctCountTag
	StatsNullCountTag
	StatsColumnsTag
	StatsTypesTag
	StatsUpperBoundTag
	StatsUpperBoundCntTag
	StatsCreatedAtTag
	StatsMcv1Tag
	StatsMcv2Tag
	StatsMcv3Tag
	StatsMcv4Tag
	StatsMcvCountsTag
)

func StatsTableSqlSchema(dbName string) sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema: sql.Schema{
			&sql.Column{Name: StatsDbColName, Type: types.Text, DatabaseSource: dbName},
			&sql.Column{Name: StatsTableColName, Type: types.Text, DatabaseSource: dbName},
			&sql.Column{Name: StatsIndexColName, Type: types.Text, DatabaseSource: dbName},
			&sql.Column{Name: StatsRowCountColName, Type: types.Int64, DatabaseSource: dbName},
			&sql.Column{Name: StatsDistinctCountColName, Type: types.Int64, DatabaseSource: dbName},
			&sql.Column{Name: StatsNullCountColName, Type: types.Int64, DatabaseSource: dbName},
			&sql.Column{Name: StatsColumnsColName, Type: types.Int64, DatabaseSource: dbName},
			&sql.Column{Name: StatsTypesColName, Type: types.Int64, DatabaseSource: dbName},
			&sql.Column{Name: StatsUpperBoundColName, Type: types.Text, DatabaseSource: dbName},
			&sql.Column{Name: StatsUpperBoundCntColName, Type: types.Int64, DatabaseSource: dbName},
			&sql.Column{Name: StatsCreatedAtColName, Type: types.Int64, DatabaseSource: dbName},
			&sql.Column{Name: StatsMcv1ColName, Type: types.Text, DatabaseSource: dbName},
			&sql.Column{Name: StatsMcv2ColName, Type: types.Text, DatabaseSource: dbName},
			&sql.Column{Name: StatsMcv3ColName, Type: types.Text, DatabaseSource: dbName},
			&sql.Column{Name: StatsMcv4ColName, Type: types.Text, DatabaseSource: dbName},
			&sql.Column{Name: StatsMcvCountsColName, Type: types.Text, DatabaseSource: dbName},
		},
		PkOrdinals: []int{0},
	}
}

var StatsTableDoltSchema = StatsTableDoltSchemaGen()

func StatsTableDoltSchemaGen() Schema {
	colColl := NewColCollection(
		NewColumn(StatsCommitHashColName, StatsCommitHashTag, stypes.StringKind, true, NotNullConstraint{}),
		NewColumn(StatsVersionColName, StatsVersionTag, stypes.IntKind, false, NotNullConstraint{}),
		NewColumn(StatsRowCountColName, StatsRowCountTag, stypes.IntKind, false, NotNullConstraint{}),
		NewColumn(StatsDistinctCountColName, StatsDistinctCountTag, stypes.IntKind, false, NotNullConstraint{}),
		NewColumn(StatsNullCountColName, StatsNullCountTag, stypes.IntKind, false, NotNullConstraint{}),
		NewColumn(StatsUpperBoundColName, StatsUpperBoundTag, stypes.StringKind, false, NotNullConstraint{}),
		NewColumn(StatsUpperBoundCntColName, StatsUpperBoundCntTag, stypes.IntKind, false, NotNullConstraint{}),
		NewColumn(StatsMcv1ColName, StatsMcv1Tag, stypes.StringKind, false),
		NewColumn(StatsMcv2ColName, StatsMcv2Tag, stypes.StringKind, false),
		NewColumn(StatsMcv3ColName, StatsMcv3Tag, stypes.StringKind, false),
		NewColumn(StatsMcv4ColName, StatsMcv4Tag, stypes.StringKind, false),
		NewColumn(StatsMcvCountsColName, StatsMcvCountsTag, stypes.StringKind, false, NotNullConstraint{}),
	)
	return MustSchemaFromCols(colColl)
}
