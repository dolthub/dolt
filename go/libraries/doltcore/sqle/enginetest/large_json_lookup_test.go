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

package enginetest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/valctx"
)

// TestLargeJSONLookupValctx is the regression test for a defect in go-mysql-server's
// types.LookupJSONValue, which shadowed its `ctx` parameter with a fresh
// context.Background() right before invoking searchableJson.Lookup. With context
// validation enabled, that bare context made NomsBlockStore.Get panic at the
// type assertion `ctx.Value(validationKey).(*Validation)` — but only on JSON values
// large enough to be backed by an IndexedJsonDocument and read with a cold node-store
// cache, which is why the bug went unnoticed in single-process tests until our
// integration tests turned valctx on.
//
// This test exercises the production-equivalent flow:
//   - engine.NewSqlEngine builds the engine, provider, session factory and context
//     factory just like the running sql-server does, so the context that reaches
//     LookupJSONValue is the one produced by sqlContextFactory (i.e. it has the
//     valctx validation key attached).
//   - sql.SessionCommandBegin / SessionCommandEnd wrap each query, mirroring the
//     server's handler.go flow that installs the dolt session validate hook.
//   - We toggle valctx on for the duration of this test only via SetEnabledForTest,
//     so we don't leak the enabled flag to other tests in the package which haven't
//     been adapted to the same lifecycle.
//
// If LookupJSONValue or any function it calls ever drops the caller's context for
// a bare one again, this test will panic the same way the integration tests did.
func TestLargeJSONLookupValctx(t *testing.T) {
	const payloadSize = 200_000

	dEnv := dtestutils.CreateTestEnv()
	defer func() { _ = dEnv.Close() }()

	bgCtx := context.Background()
	mrEnv, err := env.MultiEnvForDirectory(bgCtx, dEnv.FS, dEnv)
	require.NoError(t, err)

	sqlEng, err := engine.NewSqlEngine(bgCtx, mrEnv, &engine.SqlEngineConfig{
		Autocommit:             true,
		BranchActivityTracking: false,
	})
	require.NoError(t, err)
	defer sqlEng.Close()

	// Turn on context validation only after env init and engine construction have
	// finished — those paths use bare contexts that are not (and should not be)
	// validated. Restore on exit so other tests in the same process aren't affected.
	prevValctx := valctx.SetEnabledForTest(true)
	defer valctx.SetEnabledForTest(prevValctx)

	sqlEng.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.AddRootAccount()

	runQuery := func(query string) []sql.Row {
		t.Helper()
		ctx, err := sqlEng.NewDefaultContext(bgCtx)
		require.NoError(t, err)
		// AddRootAccount registers root@localhost; match it for privilege checks to pass.
		ctx.Session.SetClient(sql.Client{User: "root", Address: "localhost", Capabilities: 0})

		require.NoError(t, sql.SessionCommandBegin(ctx.Session))
		defer sql.SessionCommandEnd(ctx.Session)

		ctx.SetCurrentDatabase("dolt")
		_, iter, _, err := sqlEng.Query(ctx, query)
		require.NoError(t, err)
		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err)
		return rows
	}

	exec := func(q string) { runQuery(q) }

	// Set up a row whose JSON column carries a 50 KB string field — large enough that
	// the document is stored across multiple chunks and reading it back must hit the
	// chunk store.
	exec("CREATE TABLE jt (id BIGINT PRIMARY KEY, doc JSON)")
	// Build a JSON document large enough to span multiple chunks AND with several
	// keys so the prolly-tree key index is non-empty (otherwise dolt falls back to
	// LazyJSONDocument and the SearchableJSON code path under test never runs).
	var sb strings.Builder
	sb.WriteString("JSON_OBJECT(")
	for i := 0; i < 16; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "'k%02d', '%s'", i, strings.Repeat("x", payloadSize/16))
	}
	sb.WriteString(")")
	exec("INSERT INTO jt VALUES (1, " + sb.String() + ")")
	exec("CALL DOLT_COMMIT('-Am', 'add large json row')")

	// `->>` and JSON_EXTRACT both flow through types.LookupJSONValue. With the
	// production fix in place the caller's validated context flows through to
	// IndexedJsonDocument.Lookup and the chunk store accepts it; without the fix
	// the type-assertion on validationKey panics here.
	rows := runQuery("SELECT doc->>'$.k08' FROM jt WHERE id = 1")
	require.Len(t, rows, 1)

	rows = runQuery("SELECT JSON_EXTRACT(doc, '$.k08') FROM jt WHERE id = 1")
	require.Len(t, rows, 1)
}
