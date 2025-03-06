// Copyright 2025 Dolthub, Inc.
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
	"encoding/json"
	"log"
	"strconv"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
)

type scriptTest struct {
	name       string
	setup      []string
	assertions []assertion
}

type assertion struct {
	query string
	res   []sql.Row
	err   string
}

func TestStatScripts(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()

	scripts := []scriptTest{
		{
			name: "track updates",
			setup: []string{
				"create table xy (x int primary key, y varchar(16), key (y,x))",
				"insert into xy values (0,'zero'), (1, 'one')",
			},
			assertions: []assertion{
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res:   []sql.Row{{"mydb", "xy", "primary"}, {"mydb", "xy", "y"}},
				},
				{
					query: "insert into xy select x, 1 from (with recursive inputs(x) as (select 4 union select x+1 from inputs where x < 1000) select * from inputs) dt;",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(9)}},
				},
				{
					query: "update xy set y = 2 where x between 100 and 800",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(9)}},
				},
			},
		},
		{
			name: "track deletes",
			setup: []string{
				"create table xy (x int primary key, y varchar(16), key (y,x))",
				"insert into xy values (0,'zero'), (1, 'one')",
			},
			assertions: []assertion{
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res:   []sql.Row{{"mydb", "xy", "primary"}, {"mydb", "xy", "y"}},
				},
				{
					query: "insert into xy select x, 1 from (with recursive inputs(x) as (select 4 union select x+1 from inputs where x < 1000) select * from inputs) dt;",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(9)}},
				},
				{
					query: "delete from xy where x > 600",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(5)}},
				},
			},
		},
		{
			name: "ddl table",
			setup: []string{
				"create table xy (x int primary key, y varchar(16), key (y,x))",
				"insert into xy values (0,'0'), (1,'0'), (2,'0')",
			},
			assertions: []assertion{
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res:   []sql.Row{{"mydb", "xy", "primary"}, {"mydb", "xy", "y"}},
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(2)}},
				},
				{
					query: "truncate table xy",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(0)}},
				},
				{
					query: "insert into xy values (0,'0'), (1,'0'), (2,'0')",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(2)}},
				},
				{
					query: "drop table xy",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(0)}},
				},
			},
		},
		{
			name: "ddl index",
			setup: []string{
				"create table xy (x int primary key, y varchar(16), key (y,x))",
				"insert into xy values (0,'0'), (1,'0'), (2,'0')",
			},
			assertions: []assertion{
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res:   []sql.Row{{"mydb", "xy", "primary"}, {"mydb", "xy", "y"}},
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(2)}},
				},
				{
					query: "alter table xy drop index y",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(1)}},
				},
				{
					query: "alter table xy add index yx (y,x)",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(2)}},
				},
				{
					query: "select types, upper_bound from dolt_statistics where index_name = 'yx'",
					res:   []sql.Row{{"varchar(16),int", "0,2"}},
				},
				{
					query: "alter table xy modify column y int",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select types, upper_bound from dolt_statistics where index_name = 'yx'",
					res:   []sql.Row{{"int,int", "0,2"}},
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(2)}},
				},
			},
		},
		{
			name: "mcv counts",
			setup: []string{
				"create table xy (x int primary key, y int, key (y,x))",
				"alter table xy add index y2 (y)",
				"alter table xy add index x2 (x,y)",
				"insert into xy values (0,0), (1,0), (2,0), (3,0), (4,0), (5,0), (6,1), (7,1), (8,1), (9,1),(10,3),(11,4),(12,5),(13,6),(14,7),(15,8),(16,9),(17,10),(18,11)",
			},
			assertions: []assertion{
				{
					query: "select mcv1, mcv2, mcv_counts from dolt_statistics where index_name = 'y2'",
					res:   []sql.Row{{"1", "0", "4,6"}},
				},
				{
					query: "select mcv_counts from dolt_statistics where index_name = 'y'",
					res:   []sql.Row{{""}},
				},
				{
					query: "select mcv_counts from dolt_statistics where index_name = 'x2'",
					res:   []sql.Row{{""}},
				},
			},
		},
		{
			name: "vector index",
			setup: []string{
				"create table xy (x int primary key, y json, vector key(y))",
				"insert into xy values (0, '0'), (1, '1'), (2, '2'), (3, NULL), (4, NULL)",
			},
			assertions: []assertion{
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res:   []sql.Row{{"mydb", "xy", "primary"}},
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             1,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  1,
							CachedBucketCnt:   1,
							CachedBoundCnt:    1,
							CachedTemplateCnt: 1,
							StatCnt:           1,
						}},
					},
				},
			},
		},
		{
			name: "generated index",
			setup: []string{
				"create table t (pk int primary key, c0 int)",
				"insert into t values (0,0), (1,1), (2,2), (3,NULL), (4,NULL)",
				"alter table t add column c1 int generated always as (c0);",
				"alter table t add index idx(c1);",
			},
			assertions: []assertion{
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res:   []sql.Row{{"mydb", "t", "idx"}, {"mydb", "t", "primary"}},
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             1,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           1,
						}},
					},
				},
			},
		},
		{
			name: "keyless index",
			setup: []string{
				"create table t (c1 int, c2 int, index (c2))",
				"insert into t values (0,0), (1,1), (2,2), (3,NULL), (4,NULL)",
			},
			assertions: []assertion{
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res:   []sql.Row{{"mydb", "t", "c2"}},
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             1,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  1,
							CachedBucketCnt:   1,
							CachedBoundCnt:    1,
							CachedTemplateCnt: 1,
							StatCnt:           1,
						}},
					},
				},
			},
		},
		{
			name: "caps testing",
			setup: []string{
				"create table XY (x int primary key, Y int, key Yx (Y,x))",
				"alter table xy add index y2 (y)",
				"insert into xy values (0,0), (1,0), (2,0)",
			},
			assertions: []assertion{
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res:   []sql.Row{{"mydb", "xy", "primary"}, {"mydb", "xy", "y2"}, {"mydb", "xy", "yx"}},
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(3)}},
				},
				{
					query: "insert into xy select x, 1 from (with recursive inputs(x) as (select 4 union select x+1 from inputs where x < 1000) select * from inputs) dt;",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(12)}},
				},
				{
					query: "delete from xy where x > 500",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(6)}},
				},
			},
		},
		{
			name: "database ddl",
			setup: []string{
				"create table mydb.xy (x int primary key, y int, key (y,x))",
				"insert into xy values (0,0), (1,0), (2,0)",
				"create database repo2",
				"create table repo2.xy (x int primary key, y int, key (y,x))",
				"insert into repo2.xy values (0,0), (1,0), (2,0)",
				"create table repo2.ab (a int primary key, b int, key (b,a))",
				"insert into repo2.ab values (0,0), (1,0), (2,0)",
			},
			assertions: []assertion{
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res: []sql.Row{
						{"mydb", "xy", "primary"}, {"mydb", "xy", "y"},
					},
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(2)}},
				},
				{
					query: "select database_name, table_name, index_name  from repo2.dolt_statistics order by index_name",
					res: []sql.Row{
						{"repo2", "ab", "b"}, {"repo2", "ab", "primary"},
						{"repo2", "xy", "primary"}, {"repo2", "xy", "y"},
					},
				},
				{
					query: "use repo2",
				},
				{
					query: "select database_name, table_name, index_name  from dolt_statistics order by index_name",
					res: []sql.Row{
						{"repo2", "ab", "b"}, {"repo2", "ab", "primary"},
						{"repo2", "xy", "primary"}, {"repo2", "xy", "y"},
					},
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(4)}},
				},
				{
					query: "insert into repo2.xy select x, 1 from (with recursive inputs(x) as (select 4 union select x+1 from inputs where x < 1000) select * from inputs) dt;",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(10)}},
				},
				{
					query: "drop database repo2",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "use mydb",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(2)}},
				},
			},
		},
		{
			name: "recreate table without index",
			setup: []string{
				"create table xy (x int primary key, y int, key (y,x))",
				"insert into xy values (0,0), (1,0), (2,0)",
			},
			assertions: []assertion{
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(2)}},
				},
				{
					query: "drop table xy",
				},
				{
					query: "create table xy (x int primary key, y int)",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(0)}},
				},
			},
		},
		{
			name: "stats info",
			setup: []string{
				"create table xy (x int primary key, y int, key (y,x))",
				"insert into xy values (0,0), (1,0), (2,0)",
				"call dolt_add('-A')",
				"call dolt_commit('-m', 'create xy')",
				"call dolt_checkout('-b', 'feat')",
				"call dolt_checkout('main')",
			},
			assertions: []assertion{
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             2,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           2,
						}},
					},
				},
				{
					query: "call dolt_checkout('feat')",
				},
				{
					query: "drop table xy",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "call dolt_stats_gc()",
				},
				{
					query: "call dolt_stats_gc()",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             2,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           1,
						},
						}},
				},
				{
					query: "call dolt_checkout('main')",
				},
				{
					query: "call dolt_branch('-D', 'feat')",
				},
				{
					query: "call dolt_stats_gc()",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             1,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           1,
						},
						}},
				},
			},
		},
		{
			name: "test gc",
			setup: []string{
				"create table xy (x int primary key, y int, key (y,x))",
				"insert into xy values (0,0), (1,0), (2,0)",
				"call dolt_add('-A')",
				"call dolt_commit('-m', 'create xy')",
				"call dolt_checkout('-b', 'feat')",
				"call dolt_checkout('main')",
			},
			assertions: []assertion{
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             2,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           2,
						},
						}},
				},
				{
					query: "call dolt_stats_gc()",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             2,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           2,
						},
						}},
				},
			},
		},
		{
			name: "stats stop/start",
			setup: []string{
				"create table xy (x int primary key, y int, key (y,x))",
				"insert into xy values (0,0), (1,0), (2,0)",
				"call dolt_add('-A')",
				"call dolt_commit('-m', 'create xy')",
				"call dolt_checkout('-b', 'feat')",
				"call dolt_checkout('main')",
			},
			assertions: []assertion{
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             2,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           2,
						},
						}},
				},
				{
					query: "call dolt_stats_stop()",
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             2,
							Backing:           "mydb",
							Active:            false,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           2,
						},
						}},
				},
				{
					query: "call dolt_stats_restart()",
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             2,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           2,
						},
						}},
				},
			},
		},
		{
			name: "stats purge",
			setup: []string{
				"create table xy (x int primary key, y int, key (y,x))",
				"insert into xy values (0,0), (1,0), (2,0)",
				"call dolt_add('-A')",
				"call dolt_commit('-m', 'create xy')",
				"call dolt_checkout('-b', 'feat')",
				"call dolt_checkout('main')",
			},
			assertions: []assertion{
				{
					query: "insert into xy values (3,0)",
				},
				{
					query: "call dolt_checkout('feat')",
				},
				{
					query: "insert into xy values (3,0)",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             2,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  4,
							CachedBucketCnt:   4,
							CachedBoundCnt:    4,
							CachedTemplateCnt: 2,
							StatCnt:           2,
						},
						}},
				},
				{
					query: "call dolt_stats_purge()",
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             0,
							Backing:           "mydb",
							Active:            false,
							StorageBucketCnt:  0,
							CachedBucketCnt:   0,
							CachedBoundCnt:    0,
							CachedTemplateCnt: 0,
							StatCnt:           0,
						},
						}},
				},
				{
					query: "call dolt_stats_restart()",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{
						{dprocedures.StatsInfo{
							DbCnt:             2,
							Backing:           "mydb",
							Active:            true,
							StorageBucketCnt:  2,
							CachedBucketCnt:   2,
							CachedBoundCnt:    2,
							CachedTemplateCnt: 2,
							StatCnt:           2,
						},
						}},
				},
			},
		},
		{
			name: "null bounds",
			setup: []string{
				"create table xy (x int primary key, y int, key (y))",
				"insert into xy values (0,NULL), (1,0), (2,0)",
				"CREATE table xyz (x bigint primary key, y varchar(500), z bigint, key(x, z));",
				"insert into xyz values (0,0,NULL), (1,1,0), (2,2,0)",
			},
			assertions: []assertion{
				{
					query: "call dolt_stats_info('--short')",
					res: []sql.Row{{dprocedures.StatsInfo{
						DbCnt:             1,
						Active:            true,
						StorageBucketCnt:  4,
						CachedBucketCnt:   4,
						CachedBoundCnt:    4,
						CachedTemplateCnt: 4,
						StatCnt:           2,
						Backing:           "mydb",
					}}},
				},
				{
					query: "select index_name, null_count from dolt_statistics",
					res:   []sql.Row{{"primary", uint64(0)}, {"y", uint64(1)}, {"primary", uint64(0)}, {"x", uint64(1)}},
				},
			},
		},
	}

	for _, tt := range scripts {
		t.Run(tt.name, func(t *testing.T) {
			bthreads := sql.NewBackgroundThreads()
			ctx, sqlEng, sc := emptySetup(t, bthreads, false)
			sc.SetEnableGc(false)
			defer sqlEng.Close()

			require.NoError(t, sc.Restart())

			//sc.Debug = true

			for _, s := range tt.setup {
				require.NoError(t, executeQuery(ctx, sqlEng, s))
			}

			require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
			require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_gc()"))
			require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_flush()"))

			for i, a := range tt.assertions {
				if sc.Debug {
					log.Println(a.query)
				}
				rows, err := executeQueryResults(ctx, sqlEng, a.query)
				if a.err != "" {
					require.Equal(t, a.err, err.Error())
				} else {
					require.NoError(t, err)
				}
				if a.res != nil {
					cmp, exp := normalize(rows, a.res)
					require.Equal(t, exp, cmp, strconv.Itoa(i)+": "+a.query)
				}
			}
		})
	}
}

func normalize(cmp, exp []sql.Row) ([]sql.Row, []sql.Row) {
	for i, r := range exp {
		for j, v := range r {
			if _, ok := v.(dprocedures.StatsInfo); ok {
				if strSi, ok := cmp[i][j].(string); ok {
					si := dprocedures.StatsInfo{}
					if err := json.Unmarshal([]byte(strSi), &si); err != nil {
						log.Fatal(err)
					}
					si.GenCnt = 0
					cmp[i][j] = si
				}
			}
		}
	}
	return cmp, exp
}
