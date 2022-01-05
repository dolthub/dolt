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
	"runtime/pprof"
	"strings"
	"testing"

	srv "github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/testcommands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// usage: `go test -bench .`
func BenchmarkAsyncPushOnWrite(b *testing.B) {
	setup := make([]query, 1)
	setup[0] = "CREATE TABLE bench (a int, b int, c int);"

	q := strings.Builder{}
	q.WriteString("INSERT INTO bench (a, b, c) VALUES (0, 0, 0)")
	i := 1
	for i < 1000 {
		q.WriteString(fmt.Sprintf(",(%d, %d, %d)", i, i, i))
		i++
	}
	qs := q.String()

	bench := make([]query, 100)
	commit := query("select dolt_commit('-am', 'cm')")
	i = 0
	for i < len(bench) {
		bench[i] = query(qs)
		bench[i+1] = commit
		i += 2
	}

	benchmarkAsyncPush(b, serverTest{
		name:  "smoke bench",
		setup: setup,
		bench: bench,
	})
}

func benchmarkAsyncPush(b *testing.B, test serverTest) {
	var dEnv *env.DoltEnv
	var cfg srv.ServerConfig
	ctx := context.Background()

	// setup
	dEnv, cfg = getAsyncEnvAndConfig(ctx, b)
	dsess.InitPersistedSystemVars(dEnv)
	executeServerQueries(ctx, b, dEnv, cfg, test.setup)

	// bench
	f := getProfFile(b)
	err := pprof.StartCPUProfile(f)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		pprof.StopCPUProfile()
		if err = f.Close(); err != nil {
			b.Fatal(err)
		}
		fmt.Printf("\twriting CPU profile for %s: %s\n", b.Name(), f.Name())
	}()

	b.Run(test.name, func(b *testing.B) {
		executeServerQueries(ctx, b, dEnv, cfg, test.bench)
	})
}

func getAsyncEnvAndConfig(ctx context.Context, b *testing.B) (dEnv *env.DoltEnv, cfg srv.ServerConfig) {
	multiSetup := testcommands.NewMultiRepoTestSetup(b.Fatal)

	multiSetup.NewDB("dolt_bench")
	multiSetup.NewRemote("remote1")

	writerName := multiSetup.DbNames[0]

	localCfg, ok := multiSetup.MrEnv.GetEnv(writerName).Config.GetConfig(env.LocalConfig)
	if !ok {
		b.Fatal("local config does not exist")
	}
	localCfg.SetStrings(map[string]string{fmt.Sprintf("%s.%s", env.SqlServerGlobalsPrefix, sqle.ReplicateToRemoteKey): "remote1", fmt.Sprintf("%s.%s", env.SqlServerGlobalsPrefix, sqle.AsyncReplicationKey): "1"})

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
`, writerName, multiSetup.DbPaths[writerName], port))

	cfg, err := srv.NewYamlConfig(yaml)
	if err != nil {
		b.Fatal(err)
	}

	return multiSetup.MrEnv.GetEnv(writerName), cfg
}
