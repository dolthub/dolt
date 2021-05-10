// Copyright 2021 Dolthub, Inc.
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

package serverbench

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/gocraft/dbr/v2"
	"golang.org/x/sync/errgroup"

	srv "github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

type query string

type serverTest struct {
	name  string
	setup []query
	test  []query
}

func BenchmarkServerSmoke(b *testing.B) {

	setup := make([]query, 101)
	setup[0] = "CREATE TABLE test (pk int PRIMARY KEY AUTO_INCREMENT, c0 int);"

	q := strings.Builder{}
	q.WriteString("INSERT INTO test (c0) VALUES (0)")
	i := 1
	for i < 1000 {
		q.WriteString(fmt.Sprintf(",(%d)", i))
		i++
	}
	qs := q.String()

	for i := range setup {
		if i == 0 {
			continue
		}
		setup[i] = query(qs)
	}

	benchmarkServer(b, serverTest{
		name:  "smoke test",
		setup: setup,
		test: []query{
			"SELECT count(*) FROM test;",
		},
	})
}

func benchmarkServer(b *testing.B, test serverTest) {
	ctx := context.Background()
	dEnv, cfg := getEnvAndConfig(ctx, b)
	executeServerQueries(ctx, b, dEnv, cfg, test.setup)
	b.Run(test.name, func(b *testing.B) {
		b.ResetTimer()
		executeServerQueries(ctx, b, dEnv, cfg, test.test)
	})
}

const (
	database = "dolt_bench"
	port     = 1234

	name  = "name"
	email = "name@fake.horse"
)

func getEnvAndConfig(ctx context.Context, b *testing.B) (dEnv *env.DoltEnv, cfg srv.ServerConfig) {
	tmp := b.TempDir()
	//b.Logf("db directory: %s", tmp)
	dbDir := path.Join(tmp, database)
	err := os.Mkdir(dbDir, os.ModePerm)
	if err != nil {
		b.Fatal(err)
	}

	err = os.Chdir(dbDir)
	if err != nil {
		b.Fatal(err)
	}

	fs, err := filesys.LocalFilesysWithWorkingDir(".")
	if err != nil {
		b.Fatal(err)
	}

	dEnv = env.Load(ctx, os.UserHomeDir, fs, doltdb.LocalDirDoltDB, "test")
	err = dEnv.InitRepo(ctx, types.Format_7_18, name, email)
	if err != nil {
		b.Fatal(err)
	}

	yaml := []byte(fmt.Sprintf(`
log_level: warning

behavior:
 read_only: false

user:
 name: "root"
 password: ""

databases:
 - name: "%s"
   path: "%s"

listener:
 host: localhost
 port: %d
 max_connections: 128
 read_timeout_millis: 28800000
 write_timeout_millis: 28800000
`, database, dbDir, port))

	cfg, err = srv.NewYamlConfig(yaml)
	if err != nil {
		b.Fatal(err)
	}

	return dEnv, cfg
}

func executeServerQueries(ctx context.Context, b *testing.B, dEnv *env.DoltEnv, cfg srv.ServerConfig, queries []query) {
	serverController := srv.CreateServerController()

	eg, ctx := errgroup.WithContext(ctx)

	//b.Logf("Starting server with Config %v\n", srv.ConfigInfo(cfg))
	eg.Go(func() (err error) {
		startErr, closeErr := srv.Serve(ctx, "", cfg, serverController, dEnv)
		if startErr != nil {
			return startErr
		}
		if closeErr != nil {
			return closeErr
		}
		return nil
	})
	if err := serverController.WaitForStart(); err != nil {
		b.Fatal(err)
	}

	for _, q := range queries {
		if err := executeQuery(cfg, q); err != nil {
			b.Fatal(err)
		}
	}

	serverController.StopServer()
	if err := serverController.WaitForClose(); err != nil {
		b.Fatal(err)
	}
	if err := eg.Wait(); err != nil {
		b.Fatal(err)
	}
}

func executeQuery(cfg srv.ServerConfig, q query) error {
	cs := srv.ConnectionString(cfg) + database
	conn, err := dbr.Open("mysql", cs, nil)
	if err != nil {
		return err
	}

	rows, err := conn.Query(string(q))
	if err != nil {
		return err
	}

	for {
		if err = rows.Err(); err != nil {
			return err
		}
		if ok := rows.Next(); !ok {
			break
		}
	}

	return rows.Err()
}
