// Copyright 2026 Dolthub, Inc.
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
	"testing"

	"github.com/gocraft/dbr/v2"
	"golang.org/x/sync/errgroup"

	srv "github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
)

// BenchmarkDoltTagsIndexLookup measures a dolt_tags point lookup via the
// tag_name index over 1 000 tags.
// usage: go test -bench BenchmarkDoltTagsIndexLookup
func BenchmarkDoltTagsIndexLookup(b *testing.B) {
	ctx := context.Background()
	dEnv, cfg := getEnvAndConfig(ctx, b)

	setup := make([]query, 0, 1002)
	setup = append(setup, "CREATE TABLE t (pk int PRIMARY KEY);")
	setup = append(setup, "CALL DOLT_COMMIT('-Am', 'init');")
	for i := 0; i < 1000; i++ {
		setup = append(setup, query(fmt.Sprintf("CALL DOLT_TAG('tag-%04d', '-m', 'tag %d');", i, i)))
	}
	executeServerQueries(ctx, b, dEnv, cfg, setup)

	runTagLookupBench(b, ctx, dEnv, cfg, "SELECT tag_name, message FROM dolt_tags WHERE tag_name = 'tag-0500';")
}

func runTagLookupBench(b *testing.B, ctx context.Context, dEnv *env.DoltEnv, cfg servercfg.ServerConfig, q string) {
	sc := svcs.NewController()
	eg, bctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		startErr, closeErr := srv.Serve(bctx, &srv.Config{
			Version:      "",
			ServerConfig: cfg,
			Controller:   sc,
			DoltEnv:      dEnv,
		})
		if startErr != nil {
			return startErr
		}
		return closeErr
	})
	if err := sc.WaitForStart(); err != nil {
		b.Fatal(err)
	}
	defer func() {
		sc.Stop()
		_ = eg.Wait()
	}()

	conn, err := dbr.Open("mysql", servercfg.ConnectionString(cfg, database), nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := conn.Query(q)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		if err = rows.Err(); err != nil {
			b.Fatal(err)
		}
		_ = rows.Close()
	}
}
