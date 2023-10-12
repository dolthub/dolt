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

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// fillerVarchar pushes the tree into level 3
var fillerVarchar = strings.Repeat("x", 500)

var DoltStatsTests = []queries.ScriptTest{
	{
		Name: "mcv checking",
		SetUpScript: []string{
			"CREATE table xy (x bigint primary key, y int, z varchar(500), key(y,z));",
			"insert into xy values (0,0,'a'), (1,0,'a'), (2,0,'a'), (3,0,'a'), (4,1,'a'), (5,2,'a')",
			"analyze table xy",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: " SELECT mcv_cnt from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(mcv_cnt JSON path '$.McvCount')) as dt  where table_name = 'xy' and column_name = 'y,z'",
				Expected: []sql.Row{
					{types.JSONDocument{Val: []interface{}{
						float64(1),
						float64(4),
						float64(1),
					}}},
				},
			},
			{
				Query: " SELECT mcv from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(mcv JSON path '$.Mcv')) as dt  where table_name = 'xy' and column_name = 'y,z'",
				Expected: []sql.Row{
					{types.JSONDocument{Val: []interface{}{
						[]interface{}{float64(1), "a"},
						[]interface{}{float64(0), "a"},
						[]interface{}{float64(2), "a"},
					}}},
				},
			},
			{
				Query: " SELECT bound from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(bound JSON path '$.UpperBound')) as dt  where table_name = 'xy' and column_name = 'y,z'",
				Expected: []sql.Row{
					{types.JSONDocument{Val: []interface{}{float64(2), "a"}}},
				},
			},
		},
	},
	// test MCV
	// test bound count
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
				Query:    "SELECT json_length(json_extract(histogram, \"$.buckets\")) from information_schema.column_statistics where column_name = 'x'",
				Expected: []sql.Row{{32}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Null')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(0)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Distinct')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(bound_cnt int path '$.BoundCount')) as dt  where table_name = 'xy' and column_name = 'x'",
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
				Query:    "SELECT json_length(json_extract(histogram, \"$.buckets\")) from information_schema.column_statistics where column_name = 'z'",
				Expected: []sql.Row{{2}},
			},
			{
				// bucket boundary duplication
				Query:    "SELECT json_value(histogram, \"$.distinct\", 'signed') from information_schema.column_statistics where column_name = 'z'",
				Expected: []sql.Row{{202}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(400)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Null')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(200)}},
			},
			{
				// chunk border double count
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Distinct')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(202)}},
			},
			{
				// max bound count is an all nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(bound_cnt int path '$.BoundCount')) as dt  where table_name = 'xy' and column_name = 'z'",
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
				Query:    "SELECT json_length(json_extract(histogram, \"$.buckets\")) from information_schema.column_statistics where column_name = 'z'",
				Expected: []sql.Row{{152}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Count')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Null')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(10000)}},
			},
			{
				// border NULL double count
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Distinct')) as dt  where table_name = 'xy' and column_name = 'z'",
				Expected: []sql.Row{{float64(20036)}},
			},
			{
				// max bound count is nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(bound_cnt int path '$.BoundCount')) as dt  where table_name = 'xy' and column_name = 'z'",
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
				Query:    "SELECT json_length(json_extract(histogram, \"$.buckets\")) from information_schema.column_statistics where column_name = 'x,z'",
				Expected: []sql.Row{{155}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Count')) as dt  where table_name = 'xy' and column_name = 'x,z'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Null')) as dt  where table_name = 'xy' and column_name = 'x,z'",
				Expected: []sql.Row{{float64(10000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Distinct')) as dt  where table_name = 'xy' and column_name = 'x,z'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				// max bound count is nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(bound_cnt int path '$.BoundCount')) as dt  where table_name = 'xy' and column_name = 'x,z'",
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
				Query:    "SELECT json_length(json_extract(histogram, \"$.buckets\")) from information_schema.column_statistics where column_name = 'x'",
				Expected: []sql.Row{{26}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Count')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Null')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(0)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Distinct')) as dt  where table_name = 'xy' and column_name = 'x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				// max bound count is nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(bound_cnt int path '$.BoundCount')) as dt  where table_name = 'xy' and column_name = 'x'",
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
				Expected: []sql.Row{{"z"}, {"z,x"}},
			},
			{
				Query:    "SELECT json_length(json_extract(histogram, \"$.buckets\")) from information_schema.column_statistics where column_name = 'z,x'",
				Expected: []sql.Row{{42}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Count')) as dt  where table_name = 'xy' and column_name = 'z,x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Null')) as dt  where table_name = 'xy' and column_name = 'z,x'",
				Expected: []sql.Row{{float64(0)}},
			},
			{
				Query:    " SELECT sum(cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(cnt int path '$.Distinct')) as dt  where table_name = 'xy' and column_name = 'z,x'",
				Expected: []sql.Row{{float64(30000)}},
			},
			{
				// max bound count is nulls chunk
				Query:    " SELECT max(bound_cnt) from information_schema.column_statistics join json_table(histogram, '$.buckets[*]' COLUMNS(bound_cnt int path '$.BoundCount')) as dt  where table_name = 'xy' and column_name = 'z,x'",
				Expected: []sql.Row{{int64(1)}},
			},
		},
	},
}
