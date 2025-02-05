package statspro

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

type scriptTest struct {
	name       string
	setup      []string
	assertions []assertion
}

type assertion struct {
	query string
	res   []sql.Row
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
					res:   []sql.Row{{int64(8)}},
				},
				{
					query: "delete from xy where x > 600",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "select count(*) from dolt_statistics",
					res:   []sql.Row{{int64(4)}},
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
					res:   []sql.Row{{0}},
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
				"insert into xy values (0,0), (1,0), (2,0), (3,0), (4,0), (5,0), (6,1), (7,1), (8,1), (9,1),(10,3),(11,4),(12,5),(13,6),(14,7),(15,8),(16,9),(17,10),(18,11)",
			},
			assertions: []assertion{
				{
					query: "select mcv1, mcv2, mcv_counts from dolt_statistics where index_name = 'y2'",
					res:   []sql.Row{{"1", "0", "4,6"}},
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
					res:   []sql.Row{{int64(1)}},
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
					query: "call dolt_stats_info()",
					res:   []sql.Row{{`{"dbCnt":2,"readCnt":0,"active":true,"dbSeedCnt":2,"estBucketCnt":4,"cachedBucketCnt":2,"statCnt":2,"gcCounter":1,"branchCounter":1}`}},
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
					query: "call dolt_stats_wait()",
				},
				{
					query: "call dolt_stats_gc()",
				},
				{
					query: "call dolt_stats_sync()",
				},
				{
					query: "call dolt_stats_info()",
					res:   []sql.Row{{`{"dbCnt":2,"readCnt":0,"active":true,"dbSeedCnt":2,"estBucketCnt":2,"cachedBucketCnt":2,"statCnt":1,"gcCounter":3,"branchCounter":2}`}},
				},
				{
					query: "call dolt_checkout('main')",
				},
				{
					query: "call dolt_branch('-D', 'feat')",
				},
				{
					query: "call dolt_stats_sync()",
				},
				{
					query: "call dolt_stats_gc()",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "call dolt_stats_info()",
					res:   []sql.Row{{`{"dbCnt":1,"readCnt":0,"active":true,"dbSeedCnt":1,"estBucketCnt":2,"cachedBucketCnt":2,"statCnt":1,"gcCounter":4,"branchCounter":3}`}},
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
					query: "call dolt_stats_info()",
					res:   []sql.Row{{`{"dbCnt":2,"readCnt":0,"active":true,"dbSeedCnt":2,"estBucketCnt":4,"cachedBucketCnt":2,"statCnt":2,"gcCounter":1,"branchCounter":1}`}},
				},
				{
					query: "call dolt_stats_stop()",
				},
				{
					query: "call dolt_stats_info()",
					res:   []sql.Row{{`{"dbCnt":2,"readCnt":0,"active":false,"dbSeedCnt":0,"estBucketCnt":4,"cachedBucketCnt":2,"statCnt":2,"gcCounter":1,"branchCounter":1}`}},
				},
				{
					query: "call dolt_stats_restart()",
				},
				{
					query: "call dolt_stats_info()",
					res:   []sql.Row{{`{"dbCnt":2,"readCnt":0,"active":true,"dbSeedCnt":2,"estBucketCnt":4,"cachedBucketCnt":2,"statCnt":2,"gcCounter":1,"branchCounter":1}`}},
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
					query: "call dolt_stats_info()",
					res:   []sql.Row{{`{"dbCnt":2,"readCnt":0,"active":true,"dbSeedCnt":2,"estBucketCnt":4,"cachedBucketCnt":2,"statCnt":2,"gcCounter":1,"branchCounter":1}`}},
				},
				{
					query: "call dolt_stats_purge()",
				},
				{
					query: "call dolt_stats_info()",
					res:   []sql.Row{{`{"dbCnt":2,"readCnt":0,"active":false,"dbSeedCnt":2,"estBucketCnt":4,"cachedBucketCnt":2,"statCnt":2,"gcCounter":1,"branchCounter":1}`}},
				},
				{
					query: "call dolt_stats_restart()",
				},
				{
					query: "call dolt_stats_wait()",
				},
				{
					query: "call dolt_stats_info()",
					res:   []sql.Row{{`{"dbCnt":2,"readCnt":0,"active":true,"dbSeedCnt":2,"estBucketCnt":4,"cachedBucketCnt":2,"statCnt":2,"gcCounter":1,"branchCounter":1}`}},
				},
			},
		},
	}

	for _, tt := range scripts {
		t.Run(tt.name, func(t *testing.T) {
			ctx, sqlEng, sc, _ := emptySetup(t, threads, false)
			sc.SetEnableGc(true)

			require.NoError(t, sc.Restart(ctx))

			for _, s := range tt.setup {
				require.NoError(t, executeQuery(ctx, sqlEng, s))
			}

			require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_sync()"))
			require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
			require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_gc()"))

			for _, a := range tt.assertions {
				rows, err := executeQueryResults(ctx, sqlEng, a.query)
				require.NoError(t, err)
				if a.res != nil {
					require.Equal(t, a.res, rows)
				}
			}
		})
	}
}
