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

package microsysbench

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

const (
	tableSize   = 10_000
	dataFile    = "testdata.sql"
	createTable = "CREATE TABLE `sbtest1` (" +
		" `id` int NOT NULL AUTO_INCREMENT," +
		" `k` int NOT NULL DEFAULT '0'," +
		" `c` char(120) NOT NULL DEFAULT ''," +
		" `pad` char(60) NOT NULL DEFAULT ''," +
		" PRIMARY KEY (`id`)," +
		" KEY `k_1` (`k`)" +
		");"
)

var dEnv *env.DoltEnv

func init() {
	dEnv = dtestutils.CreateTestEnv()
	populateRepo(dEnv, readTestData(dataFile))
}

func BenchmarkOltpPointSelect(b *testing.B) {
	benchmarkSysbenchQuery(b, func(int) string {
		q := "SELECT c FROM sbtest1 WHERE id=%d"
		return fmt.Sprintf(q, rand.Intn(tableSize))
	})
}

func BenchmarkOltpJoinScan(b *testing.B) {
	benchmarkSysbenchQuery(b, func(int) string {
		return `select a.id, a.k 
				from sbtest1 a, sbtest1 b 
				where a.id = b.id limit 500`
	})
}

func BenchmarkProjectionAggregation(b *testing.B) {
	benchmarkSysbenchQuery(b, func(int) string {
		q := "SELECT c, count(id) FROM sbtest1 WHERE k > %d GROUP BY c ORDER BY c"
		return fmt.Sprintf(q, rand.Intn(tableSize))
	})
}

func BenchmarkSelectRandomPoints(b *testing.B) {
	benchmarkSysbenchQuery(b, func(int) string {
		var sb strings.Builder
		sb.Grow(120)
		sb.WriteString("SELECT id, k, c, pad FROM sbtest1 WHERE k IN (")
		sb.WriteString(strconv.Itoa(rand.Intn(tableSize)))
		for i := 1; i < 10; i++ {
			sb.WriteString(", ")
			sb.WriteString(strconv.Itoa(rand.Intn(tableSize)))
		}
		sb.WriteString(");")
		return sb.String()
	})
}

func benchmarkSysbenchQuery(b *testing.B, getQuery func(int) string) {
	ctx, eng := setupBenchmark(b, dEnv)
	for i := 0; i < b.N; i++ {
		_, iter, err := eng.Query(ctx, getQuery(i))
		require.NoError(b, err)
		for {
			if _, err = iter.Next(ctx); err != nil {
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

func setupBenchmark(t *testing.B, dEnv *env.DoltEnv) (*sql.Context, *engine.SqlEngine) {
	ctx := context.Background()
	config := &engine.SqlEngineConfig{
		InitialDb:    "dolt",
		IsReadOnly:   false,
		PrivFilePath: "",
		ServerUser:   "root",
		ServerPass:   "",
		Autocommit:   true,
	}

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
	require.NoError(t, err)

	eng, err := engine.NewSqlEngine(ctx, mrEnv, engine.FormatNull, config)
	require.NoError(t, err)

	sqlCtx, err := eng.NewContext(ctx)
	require.NoError(t, err)
	sqlCtx.Session.SetClient(sql.Client{User: "root", Address: "%", Capabilities: 0})

	return sqlCtx, eng
}

func readTestData(file string) string {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func populateRepo(dEnv *env.DoltEnv, insertData string) {
	execSql := func(dEnv *env.DoltEnv, q string) int {
		ctx := context.Background()
		args := []string{"-r", "null", "-q", q}
		return commands.SqlCmd{}.Exec(ctx, "sql", args, dEnv)
	}
	execSql(dEnv, createTable)
	execSql(dEnv, insertData)
}
