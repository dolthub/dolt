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
	}

	for _, tt := range scripts {
		t.Run(tt.name, func(t *testing.T) {
			ctx, sqlEng, sc, _ := emptySetup(t, threads, false)
			sc.SetEnableGc(true)

			for _, s := range tt.setup {
				require.NoError(t, executeQuery(ctx, sqlEng, s))
			}

			require.NoError(t, sc.Restart(ctx))

			require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_branch_sync()"))
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
