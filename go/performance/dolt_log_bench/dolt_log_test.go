// Copyright 2022 Dolthub, Inc.
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

package dolt_log_bench

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

var dEnv *env.DoltEnv

func init() {
	dEnv = dtestutils.CreateTestEnv()
	populateCommitGraph(dEnv)
}

func setupBenchmark(t *testing.B, dEnv *env.DoltEnv) (*sql.Context, *engine.SqlEngine) {
	ctx := context.Background()
	config := &engine.SqlEngineConfig{
		ServerUser: "root",
		Autocommit: true,
	}

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	require.NoError(t, err)

	eng, err := engine.NewSqlEngine(ctx, mrEnv, config)
	require.NoError(t, err)

	sqlCtx, err := eng.NewLocalContext(ctx)
	require.NoError(t, err)

	sqlCtx.SetCurrentDatabase("dolt")
	return sqlCtx, eng
}

func populateCommitGraph(dEnv *env.DoltEnv) {
	ctx := context.Background()
	cliCtx, err := commands.NewArgFreeCliContext(ctx, dEnv, dEnv.FS)
	if err != nil {
		panic(err)
	}
	defer cliCtx.Close()
	execSql := func(dEnv *env.DoltEnv, q string) int {
		args := []string{"-r", "null", "-q", q}
		return commands.SqlCmd{}.Exec(ctx, "sql", args, dEnv, cliCtx)
	}
	for i := 0; i < 500; i++ {
		execSql(dEnv, fmt.Sprintf("call dolt_commit('--allow-empty', '-m', '%d') ", i))
	}
}

func BenchmarkDoltLog(b *testing.B) {
	ctx, eng := setupBenchmark(b, dEnv)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, iter, _, err := eng.Query(ctx, "select * from dolt_log()")
		require.NoError(b, err)
		for {
			_, err := iter.Next(ctx)
			if err != nil {
				break
			}
		}
		require.Error(b, io.EOF)
		err = iter.Close(ctx)
		require.NoError(b, err)
	}
	_ = eng.Close()
	b.ReportAllocs()
}
