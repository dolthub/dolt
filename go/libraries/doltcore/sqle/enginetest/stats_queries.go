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

package enginetest

import (
	"fmt"
	"strings"
	"testing"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
)

// fillerVarchar pushes the tree into level 3
var fillerVarchar = strings.Repeat("x", 500)

var DoltHistogramTests = []queries.ScriptTest{
	{
		Name: "mcv checking",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy values (0,0,'a'), (1,0,'a'), (2,0,'a'), (3,0,'a'), (4,1,'a'), (5,2,'a')",
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: " SELECT mcv_cnt from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(mcv_cnt JSON path '$.mcv_counts')) as dt  where table_name = 'xy' and column_name = 'y,z'",
				Expected: []sql.Row{
					{types.JSONDocument{Val: []interface{}{
						float64(4),
					}}},
				},
			},
			{
				Query: " SELECT mcv from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(mcv JSON path '$.mcvs[*]')) as dt  where table_name = 'xy' and column_name = 'y,z'",
				Expected: []sql.Row{
					{types.JSONDocument{Val: []interface{}{
						[]interface{}{float64(0), "a"},
					}}},
				},
			},
			{
				Query: " SELECT x,z from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(x bigint path '$.upper_bound[0]', z text path '$.upper_bound[1]')) as dt  where table_name = 'xy' and column_name = 'y,z'",
				Expected: []sql.Row{
					{2, "a"},
				},
			},
		},
	},
	{
		Name: "int pk",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y varchar(500));",
			fmt.Sprintf("insert into xy select x, '%s' from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 10000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select x, '%s'  from (with recursive inputs(x) as (select 10001 union select x+1 from inputs where x < 20000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select x, '%s'  from (with recursive inputs(x) as (select 20001 union select x+1 from inputs where x < 30000) select * from inputs) dt", fillerVarchar),
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT json_length(json_extract(histogram, \"$.statistic.buckets\")) from information_schema.column_statistics where column_name = 'x'",
				Expected: []sql.Row{{32}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.row_count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.null_count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(0)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.distinct_count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(bound_cnt int path '$.bound_count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{int64(1)}},
			},
		},
	},
	{
		Name: "nulls distinct across chunk boundary",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y varchar(500), z bigint, key(z));",
			fmt.Sprintf("insert into xy select x, '%s', x  from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 200) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select x, '%s', NULL  from (with recursive inputs(x) as (select 201 union select x+1 from inputs where x < 400) select * from inputs) dt", fillerVarchar),
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT json_length(json_extract(histogram, \"$.statistic.buckets\")) from information_schema.column_statistics where column_name = 'z'",
				Expected: []sql.Row{{2}},
			},
			{
				// bucket boundary duplication
				Query:    "SELECT json_value(histogram, \"$.statistic.distinct_count\", 'signed') from information_schema.column_statistics where column_name = 'z'",
				Expected: []sql.Row{{202}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.row_count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(400)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.null_count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(200)}},
			},
			{
				// chunk border double count
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.distinct_count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(202)}},
			},
			{
				// max bound count is an all nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(bound_cnt int path '$.bound_count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{int64(183)}},
			},
		},
	},
	{
		Name: "int index",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y varchar(500), z bigint, key(z));",
			fmt.Sprintf("insert into xy select x, '%s', x from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 10000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select x, '%s', x  from (with recursive inputs(x) as (select 10001 union select x+1 from inputs where x < 20000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select x, '%s', NULL  from (with recursive inputs(x) as (select 20001 union select x+1 from inputs where x < 30000) select * from inputs) dt", fillerVarchar),
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT json_length(json_extract(histogram, \"$.statistic.buckets\")) from information_schema.column_statistics where column_name = 'z'",
				Expected: []sql.Row{{152}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.row_count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.null_count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(10000)}},
			},
			{
				// border NULL double count
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.distinct_count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(20036)}},
			},
			{
				// max bound count is nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(bound_cnt int path '$.bound_count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{int64(440)}},
			},
		},
	},
	{
		Name: "multiint index",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y varchar(500), z bigint, key(x, z));",
			fmt.Sprintf("insert into xy select x, '%s', x+1  from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 10000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select x, '%s', x+1  from (with recursive inputs(x) as (select 10001 union select x+1 from inputs where x < 20000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select x, '%s', NULL from (with recursive inputs(x) as (select 20001 union select x+1 from inputs where x < 30000) select * from inputs) dt", fillerVarchar),
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT json_length(json_extract(histogram, \"$.statistic.buckets\")) from information_schema.column_statistics where column_name = 'x,z'",
				Expected: []sql.Row{{155}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.row_count')) as dt  where table_name = 'xy' and column_name = 'x,z'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.null_count')) as dt  where table_name = 'xy' and column_name = 'x,z'",
				Expected: []sql.Row{{float64(10000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.distinct_count')) as dt  where table_name = 'xy' and column_name = 'x,z'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				// max bound count is nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(bound_cnt int path '$.bound_count')) as dt  where table_name = 'xy' and column_name = 'x,z'",
				Expected: []sql.Row{{int64(1)}},
			},
		},
	},
	{
		Name: "several int index",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y varchar(500), z bigint, key(z), key (x,z));",
			fmt.Sprintf("insert into xy select x, '%s', x+1  from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 10000) select * from inputs) dt", fillerVarchar),
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    " SELECT column_name from information_schema.column_statistics",
				Expected: []sql.Row{},
			},
			{
				Query: "analyze table xy",
			},
			{
				Query:    " SELECT column_name from information_schema.column_statistics",
				Expected: []sql.Row{{"x"}, {"z"}, {"x,z"}},
			},
		},
	},
	{
		Name: "varchar pk",
		SetUpScript: []string{
			"CREATE table xy (x varchar(16) primary key, y varchar(500));",
			fmt.Sprintf("insert into xy select cast (x as char), '%s'  from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 10000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select cast (x as char), '%s'  from (with recursive inputs(x) as (select 10001 union select x+1 from inputs where x < 20000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select cast (x as char), '%s' from (with recursive inputs(x) as (select 20001 union select x+1 from inputs where x < 30000) select * from inputs) dt", fillerVarchar),
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT json_length(json_extract(histogram, \"$.statistic.buckets\")) from information_schema.column_statistics where column_name = 'x'",
				Expected: []sql.Row{{26}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.row_count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.null_count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(0)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.distinct_count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				// max bound count is nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(bound_cnt int path '$.bound_count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{int64(1)}},
			},
		},
	},
	{
		Name: "int-varchar inverse ordinal pk",
		SetUpScript: []string{
			"CREATE table xy (x varchar(16), y varchar(500), z bigint, primary key(z,x));",
			fmt.Sprintf("insert into xy select cast (x as char), '%s', x  from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 10000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select cast (x as char), '%s', x  from (with recursive inputs(x) as (select 10001 union select x+1 from inputs where x < 20000) select * from inputs) dt", fillerVarchar),
			fmt.Sprintf("insert into xy select cast (x as char), '%s', x from (with recursive inputs(x) as (select 20001 union select x+1 from inputs where x < 30000) select * from inputs) dt", fillerVarchar),
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    " SELECT column_name from information_schema.column_statistics",
				Expected: []sql.Row{{"z,x"}},
			},
			{
				Query:    "SELECT json_length(json_extract(histogram, \"$.statistic.buckets\")) from information_schema.column_statistics where column_name = 'z,x'",
				Expected: []sql.Row{{42}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.row_count')) as dt  where table_name = 'xy' and column_name = 'z,x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.null_count')) as dt  where table_name = 'xy' and column_name = 'z,x'",
				Expected: []sql.Row{{float64(0)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(cnt int path '$.distinct_count')) as dt  where table_name = 'xy' and column_name = 'z,x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				// max bound count is nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.statistic.buckets[*]' COLUMNS(bound_cnt int path '$.bound_count')) as dt  where table_name = 'xy' and column_name = 'z,x'",
				Expected: []sql.Row{{int64(1)}},
			},
		},
	},
}

var DoltStatsIOTests = []queries.ScriptTest{
	{
		Name: "single-table",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy values (0,0,'a'), (1,0,'a'), (2,0,'a'), (3,0,'a'), (4,1,'a'), (5,2,'a')",
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select database_name, table_name, index_name, columns, types from dolt_statistics",
				Expected: []sql.Row{
					{"mydb", "xy", "primary", "x", "bigint"},
					{"mydb", "xy", "y", "y,z", "int,varchar(500)"},
				},
			},
			{
				Query:    fmt.Sprintf("select %s, %s, %s from dolt_statistics", schema.StatsRowCountColName, schema.StatsDistinctCountColName, schema.StatsNullCountColName),
				Expected: []sql.Row{{uint64(6), uint64(6), uint64(0)}, {uint64(6), uint64(3), uint64(0)}},
			},
			{
				Query: fmt.Sprintf("select %s, %s from dolt_statistics", schema.StatsUpperBoundColName, schema.StatsUpperBoundCntColName),
				Expected: []sql.Row{
					{"5", uint64(1)},
					{"2,a", uint64(1)},
				},
			},
			{
				Query: fmt.Sprintf("select %s, %s, %s, %s, %s from dolt_statistics", schema.StatsMcv1ColName, schema.StatsMcv2ColName, schema.StatsMcv3ColName, schema.StatsMcv4ColName, schema.StatsMcvCountsColName),
				Expected: []sql.Row{
					{"", "", "", "", ""},
					{"0,a", "", "", "", "4"},
				},
			},
		},
	},
	{
		Name: "boundary nils don't panic when trying to convert to the zero type",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y varchar(10), key(y,x));",
			"insert into xy values (0,null),(1,null)",
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select database_name, table_name, index_name, columns, types from dolt_statistics",
				Expected: []sql.Row{
					{"mydb", "xy", "primary", "x", "bigint"},
					{"mydb", "xy", "y", "y,x", "varchar(10),bigint"},
				},
			},
		},
	},
	{
		Name: "multi-table",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy values (0,0,'a'), (1,0,'a'), (2,0,'a'), (3,0,'a'), (4,1,'a'), (5,2,'a')",
			"CREATE table ab (a bigint primary key, b int, c int, key(b,c));",
			"insert into ab values (0,0,1), (1,0,1), (2,0,1), (3,0,1), (4,1,1), (5,2,1)",
			"analyze table xy",
			"analyze table ab",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "select database_name, table_name, index_name, columns, types  from dolt_statistics where table_name = 'xy'",
				Expected: []sql.Row{
					{"mydb", "xy", "primary", "x", "bigint"},
					{"mydb", "xy", "y", "y,z", "int,varchar(500)"},
				},
			},
			{
				Query:    fmt.Sprintf("select %s, %s, %s from dolt_statistics where table_name = 'xy'", schema.StatsRowCountColName, schema.StatsDistinctCountColName, schema.StatsNullCountColName),
				Expected: []sql.Row{{uint64(6), uint64(6), uint64(0)}, {uint64(6), uint64(3), uint64(0)}},
			},
			{
				Query: "select `table_name`, `index_name` from dolt_statistics",
				Expected: []sql.Row{
					{"ab", "primary"},
					{"ab", "b"},
					{"xy", "primary"},
					{"xy", "y"},
				},
			},
			{
				Query: "select database_name, table_name, index_name, columns, types  from dolt_statistics where table_name = 'ab'",
				Expected: []sql.Row{
					{"mydb", "ab", "primary", "a", "bigint"},
					{"mydb", "ab", "b", "b,c", "int,int"},
				},
			},
			{
				Query:    fmt.Sprintf("select %s, %s, %s from dolt_statistics where table_name = 'ab'", schema.StatsRowCountColName, schema.StatsDistinctCountColName, schema.StatsNullCountColName),
				Expected: []sql.Row{{uint64(6), uint64(6), uint64(0)}, {uint64(6), uint64(3), uint64(0)}},
			},
		},
	},
	{
		// only edited chunks are scanned and re-written
		Name: "incremental stats updates",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy values (0,0,'a'), (2,0,'a'), (4,1,'a'), (6,2,'a')",
			"analyze table xy",
			"insert into xy values (1,0,'a'), (3,0,'a'), (5,2,'a'),  (7,1,'a')",
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: fmt.Sprintf("select %s, %s, %s from dolt_statistics where table_name = 'xy'", schema.StatsRowCountColName, schema.StatsDistinctCountColName, schema.StatsNullCountColName),
				Expected: []sql.Row{
					{uint64(8), uint64(8), uint64(0)},
					{uint64(8), uint64(3), uint64(0)},
				},
			},
		},
	},
	{
		Name: "incremental stats deletes manual analyze",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy select x, 1, 1 from (with recursive inputs(x) as (select 4 union select x+1 from inputs where x < 1000) select * from inputs) dt;",
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) as cnt from dolt_statistics group by table_name, index_name order by cnt",
				Expected: []sql.Row{{6}, {7}},
			},
			{
				Query: "delete from xy where x > 500",
			},
			{
				Query: "analyze table xy",
			},
			{
				Query:    "select count(*) from dolt_statistics group by table_name, index_name",
				Expected: []sql.Row{{4}, {4}},
			},
		},
	},
	{
		Name: "incremental stats deletes auto",
		SetUpScript: []string{
			"set @@PERSIST.dolt_stats_auto_refresh_interval = 0;",
			"set @@PERSIST.dolt_stats_auto_refresh_threshold = 0;",
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy select x, 1, 1 from (with recursive inputs(x) as (select 4 union select x+1 from inputs where x < 1000) select * from inputs) dt;",
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) as cnt from dolt_statistics group by table_name, index_name order by cnt",
				Expected: []sql.Row{{6}, {7}},
			},
			{
				Query: "delete from xy where x > 500",
			},
			{
				Query: "call dolt_stats_restart()",
			},
			{
				Query: "select sleep(.1)",
			},
			{
				Query:    "select count(*) from dolt_statistics group by table_name, index_name",
				Expected: []sql.Row{{4}, {4}},
			},
		},
	},
}

var StatBranchTests = []queries.ScriptTest{
	{
		Name: "multi branch stats",
		SetUpScript: []string{
			"set @@PERSIST.dolt_stats_auto_refresh_interval = 0;",
			"set @@PERSIST.dolt_stats_auto_refresh_threshold = 0;",
			"set @@PERSIST.dolt_stats_branches = 'main,feat';",
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy values (0,0,'a'), (1,0,'a'), (2,0,'a'), (3,0,'a'), (4,1,'a'), (5,2,'a')",
			"call dolt_commit('-Am', 'xy')",
			"call dolt_checkout('-b','feat')",
			"CREATE table ab (a bigint primary key, b int, c int, key(b,c));",
			"insert into ab values (0,0,1), (1,0,1), (2,0,1), (3,0,1), (4,1,1), (5,2,1)",
			"call dolt_commit('-Am', 'ab')",
			"call dolt_checkout('main')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "call dolt_stats_restart()",
			},
			{
				Query: "select sleep(.1)",
			},
			{
				Query: "select table_name, index_name, row_count from dolt_statistics",
				Expected: []sql.Row{
					{"xy", "primary", uint64(6)},
					{"xy", "y", uint64(6)},
				},
			},
			{
				Query: "select table_name, index_name, row_count from dolt_statistics as of 'feat'",
				Expected: []sql.Row{
					{"ab", "primary", uint64(6)},
					{"ab", "b", uint64(6)},
					{"xy", "primary", uint64(6)},
					{"xy", "y", uint64(6)},
				},
			},
			{
				Query: "select table_name, index_name, row_count from dolt_statistics as of 'main'",
				Expected: []sql.Row{
					{"xy", "primary", uint64(6)},
					{"xy", "y", uint64(6)},
				},
			},
			{
				Query: "call dolt_checkout('feat')",
			},
			{
				Query: "insert into xy values ('6',3,'a')",
			},
			{
				Query: "call dolt_commit('-am', 'cm')",
			},
			{
				Query: "select sleep(.1)",
			},
			{
				Query: "select table_name, index_name, row_count from dolt_statistics as of 'feat'",
				Expected: []sql.Row{
					{"ab", "primary", uint64(6)},
					{"ab", "b", uint64(6)},
					{"xy", "primary", uint64(7)},
					{"xy", "y", uint64(7)},
				},
			},
			{
				Query: "select table_name, index_name, row_count from dolt_statistics as of 'main'",
				Expected: []sql.Row{
					{"xy", "primary", uint64(6)},
					{"xy", "y", uint64(6)},
				},
			},
			{
				Query: "call dolt_checkout('feat')",
			},
			{
				Query: "call dolt_stats_stop()",
			},
			{
				Query: "call dolt_stats_drop()",
			},
			{
				Query:    "select table_name, index_name, row_count from dolt_statistics as of 'feat'",
				Expected: []sql.Row{},
			},
			{
				// we dropped 'feat', not 'main'
				Query: "select table_name, index_name, row_count from dolt_statistics as of 'main'",
				Expected: []sql.Row{
					{"xy", "primary", uint64(6)},
					{"xy", "y", uint64(6)},
				},
			},
		},
	},
	{
		Name: "issue #7710: branch connection string errors",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy values (0,0,'a'), (1,0,'a'), (2,0,'a'), (3,0,'a'), (4,1,'a'), (5,2,'a')",
			"use `mydb/main`",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "analyze table xy",
				Expected: []sql.Row{
					{"xy", "analyze", "status", "OK"},
				},
			},
		},
	},
}

var StatProcTests = []queries.ScriptTest{
	{
		Name: "deleting stats removes information_schema access point",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy values (0,0,0)",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "analyze table xy",
			},
			{
				Query:    "select count(*) from information_schema.column_statistics",
				Expected: []sql.Row{{2}},
			},
			{
				Query: "call dolt_stats_drop()",
			},
			{
				Query:    "select count(*) from information_schema.column_statistics",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "restart empty stats panic",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "analyze table xy",
			},
			{
				Query:    "select count(*) from dolt_statistics",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "set @@GLOBAL.dolt_stats_auto_refresh_threshold = 0",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "set @@GLOBAL.dolt_stats_auto_refresh_interval = 0",
				Expected: []sql.Row{{}},
			},
			{
				// don't panic
				Query: "call dolt_stats_restart()",
			},
			{
				Query: "select sleep(.1)",
			},
			{
				Query: "insert into xy values (0,0,0)",
			},
			{
				Query: "select sleep(.1)",
			},
			{
				Query:    "select count(*) from dolt_statistics",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "basic start, status, stop loop",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy values (0,0,'a'), (2,0,'a'), (4,1,'a'), (6,2,'a')",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from dolt_statistics",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "call dolt_stats_status()",
				Expected: []sql.Row{{"no active stats thread"}},
			},
			// set refresh interval arbitrarily high to avoid updating when we restart
			{
				Query:    "set @@PERSIST.dolt_stats_auto_refresh_interval = 100000;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "set @@PERSIST.dolt_stats_auto_refresh_threshold = 0",
				Expected: []sql.Row{{}},
			},
			{
				Query: "call dolt_stats_restart()",
			},
			{
				Query:    "call dolt_stats_status()",
				Expected: []sql.Row{{"restarted thread: mydb"}},
			},
			{
				Query:    "set @@PERSIST.dolt_stats_auto_refresh_interval = 0;",
				Expected: []sql.Row{{}},
			},
			// new restart picks up 0-interval, will start refreshing immediately
			{
				Query: "call dolt_stats_restart()",
			},
			{
				Query: "select sleep(.1)",
			},
			{
				Query:    "call dolt_stats_status()",
				Expected: []sql.Row{{"refreshed mydb"}},
			},
			{
				Query:    "select count(*) from dolt_statistics",
				Expected: []sql.Row{{2}},
			},
			// kill refresh thread
			{
				Query: "call dolt_stats_stop()",
			},
			{
				Query:    "call dolt_stats_status()",
				Expected: []sql.Row{{"cancelled thread: mydb"}},
			},
			// insert without refresh thread will not update stats
			{
				Query: "insert into xy values (1,0,'a'), (3,0,'a'), (5,2,'a'),  (7,1,'a')",
			},
			{
				Query: "select sleep(.1)",
			},
			{
				Query:    "call dolt_stats_status()",
				Expected: []sql.Row{{"cancelled thread: mydb"}},
			},
			// manual analyze will update stats
			{
				Query:    "analyze table xy",
				Expected: []sql.Row{{"xy", "analyze", "status", "OK"}},
			},
			{
				Query:    "call dolt_stats_status()",
				Expected: []sql.Row{{"refreshed mydb"}},
			},
			{
				Query:    "select count(*) from dolt_statistics",
				Expected: []sql.Row{{2}},
			},
			// kill refresh thread and delete stats ref
			{
				Query: "call dolt_stats_drop()",
			},
			{
				Query:    "call dolt_stats_status()",
				Expected: []sql.Row{{"dropped"}},
			},
			{
				Query:    "select count(*) from dolt_statistics",
				Expected: []sql.Row{{0}},
			},
		},
	},
}

// TestProviderReloadScriptWithEngine runs the test script given with the engine provided.
func TestProviderReloadScriptWithEngine(t *testing.T, e enginetest.QueryEngine, harness enginetest.Harness, script queries.ScriptTest) {
	ctx := enginetest.NewContext(harness)
	err := enginetest.CreateNewConnectionForServerEngine(ctx, e)
	require.NoError(t, err, nil)

	t.Run(script.Name, func(t *testing.T) {
		for _, statement := range script.SetUpScript {
			if sh, ok := harness.(enginetest.SkippingHarness); ok {
				if sh.SkipQueryTest(statement) {
					t.Skip()
				}
			}
			ctx = ctx.WithQuery(statement)
			enginetest.RunQueryWithContext(t, e, harness, ctx, statement)
		}

		assertions := script.Assertions
		if len(assertions) == 0 {
			assertions = []queries.ScriptTestAssertion{
				{
					Query:           script.Query,
					Expected:        script.Expected,
					ExpectedErr:     script.ExpectedErr,
					ExpectedIndexes: script.ExpectedIndexes,
				},
			}
		}

		{
			// reload provider, get disk stats
			eng, ok := e.(*gms.Engine)
			if !ok {
				t.Errorf("expected *gms.Engine but found: %T", e)
			}

			err := eng.Analyzer.Catalog.StatsProvider.DropDbStats(ctx, "mydb", false)
			require.NoError(t, err)

			err = eng.Analyzer.Catalog.StatsProvider.(*statspro.Provider).LoadStats(ctx, "mydb", "main")
			require.NoError(t, err)
		}

		for _, assertion := range assertions {
			t.Run(assertion.Query, func(t *testing.T) {
				if assertion.NewSession {
					th, ok := harness.(enginetest.TransactionHarness)
					require.True(t, ok, "ScriptTestAssertion requested a NewSession, "+
						"but harness doesn't implement TransactionHarness")
					ctx = th.NewSession()
				}

				if sh, ok := harness.(enginetest.SkippingHarness); ok && sh.SkipQueryTest(assertion.Query) {
					t.Skip()
				}
				if assertion.Skip {
					t.Skip()
				}

				if assertion.ExpectedErr != nil {
					enginetest.AssertErr(t, e, harness, assertion.Query, nil, assertion.ExpectedErr)
				} else if assertion.ExpectedErrStr != "" {
					enginetest.AssertErrWithCtx(t, e, harness, ctx, assertion.Query, nil, nil, assertion.ExpectedErrStr)
				} else if assertion.ExpectedWarning != 0 {
					enginetest.AssertWarningAndTestQuery(t, e, nil, harness, assertion.Query,
						assertion.Expected, nil, assertion.ExpectedWarning, assertion.ExpectedWarningsCount,
						assertion.ExpectedWarningMessageSubstring, assertion.SkipResultsCheck)
				} else if assertion.SkipResultsCheck {
					enginetest.RunQueryWithContext(t, e, harness, nil, assertion.Query)
				} else if assertion.CheckIndexedAccess {
					enginetest.TestQueryWithIndexCheck(t, ctx, e, harness, assertion.Query, assertion.Expected, assertion.ExpectedColumns, assertion.Bindings)
				} else {
					var expected = assertion.Expected
					if enginetest.IsServerEngine(e) && assertion.SkipResultCheckOnServerEngine {
						// TODO: remove this check in the future
						expected = nil
					}
					enginetest.TestQueryWithContext(t, ctx, e, harness, assertion.Query, expected, assertion.ExpectedColumns, assertion.Bindings, nil)
				}
			})
		}
	})
}

func mustNewStatQual(s string) sql.StatQualifier {
	qual, _ := sql.NewQualifierFromString(s)
	return qual
}
