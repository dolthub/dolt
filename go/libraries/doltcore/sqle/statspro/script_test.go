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
			setup: []string{
				"create table xy (x int primary key, y varchar(16), key (y,x))",
				"insert into xy values (0,'zero'), (1, 'one')",
			},
			assertions: []assertion{
				{
					query: "",
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
			require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))

			require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))

			require.NoError(t, sc.Restart(ctx))
		})
	}
}
