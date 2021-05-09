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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
	"os"
	"path"
	"testing"

	"github.com/gocraft/dbr/v2"
	"golang.org/x/sync/errgroup"

	srv "github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

const (
	database = "dolt_bench"
	port     = 1234
)

type query string

type serverTest struct {
	name  string
	setup []query
	test  []query
}

func BenchmarkSqlServer(b *testing.B) {
	tests := []serverTest{
		{
			name: "smoke test",
			setup: []query{
				`CREATE TABLE test (pk int PRIMARY KEY, c0 int);`,
				`INSERT INTO test VALUES 
					(0,0), (1,1), (2,2), (3,3), (4,4),
					(5,5), (6,6), (7,7), (8,8), (9,9);`,
			},
			test: []query{
				`SELECT * FROM test;`,
			},
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		cfg, fs := GetConfigAndFS(b.TempDir())
		dEnv := GetFSDoltEnv(ctx, fs)

		executeBenchQueries(ctx, b, dEnv, cfg, test.setup)
		b.Run(test.name, func(b *testing.B) {
			executeBenchQueries(ctx, b, dEnv, cfg, test.test)
		})
	}
}


func GetConfigAndFS(tmpDir string) (cfg srv.ServerConfig, fs filesys.Filesys) {
	dbDir := path.Join(tmpDir, database)
	err := os.Mkdir(dbDir, os.ModePerm)
	if err != nil {
		panic(err)
	}

	err = os.Chdir(dbDir)
	if err != nil {
		panic(err)
	}

	fs, err = filesys.LocalFilesysWithWorkingDir(".")
	if err != nil {
		panic(err)
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
		panic(err)
	}

	return cfg, fs
}

func GetFSDoltEnv(ctx context.Context, fs filesys.Filesys) *env.DoltEnv {
	const name = "test mcgibbins"
	const email = "bigfakeytester@fake.horse"
	dEnv := env.Load(ctx, os.UserHomeDir, fs, doltdb.LocalDirDoltDB, "test")
	err := dEnv.InitRepo(ctx, types.Format_7_18, name, email)
	if err != nil {
		panic(err)
	}
	return dEnv
}

func executeBenchQueries(ctx context.Context, b *testing.B, dEnv *env.DoltEnv, cfg srv.ServerConfig, queries []query) {
	serverController := srv.CreateServerController()

	eg, ctx := errgroup.WithContext(ctx)

	b.Logf("Starting server with Config %v\n", srv.ConfigInfo(cfg))
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
