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

package enginetest

import (
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

type batchCommitLifecycle struct{ commits int }

func (*batchCommitLifecycle) DoltgresTransactionStarted()        {}
func (l *batchCommitLifecycle) DoltgresTransactionCommitted()    { l.commits++ }
func (*batchCommitLifecycle) DoltgresTransactionRolledBack()     {}
func (*batchCommitLifecycle) DoltgresSavepointCreated(string)    {}
func (*batchCommitLifecycle) DoltgresSavepointRolledBack(string) {}
func (*batchCommitLifecycle) DoltgresSavepointReleased(string)   {}

func TestMultiBranchCommitLifecycle(t *testing.T) {
	for _, test := range []struct {
		name      string
		query     string
		automatic bool
	}{
		{"working sets", "COMMIT", false},
		{"automatic heads", "COMMIT", true},
		{"explicit heads", "CALL dolt_commit_all('-am', 'batch')", false},
		{"implicit heads", "CALL dolt_commit('-am', 'batch')", false},
		{"single working set helper", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newDoltHarness(t)
			defer h.Close()
			engine, err := h.NewEngine(t)
			require.NoError(t, err)
			defer engine.Close()
			ctx := enginetest.NewContext(h)
			for _, query := range []string{
				"CREATE TABLE t (pk INT PRIMARY KEY)", "CALL dolt_commit('-Am', 'setup')",
				"CALL dolt_branch('other')", "SET autocommit=0", "SET dolt_multi_branch_commit=1", "START TRANSACTION",
			} {
				enginetest.RunQueryWithContext(t, engine, h, ctx, query)
			}
			if test.automatic {
				enginetest.RunQueryWithContext(t, engine, h, ctx, "SET dolt_transaction_commit=1")
			}
			sess := dsess.DSessFromSess(ctx.Session)
			lifecycle := &batchCommitLifecycle{}
			sess.DoltgresSessObj = lifecycle
			enginetest.RunQueryWithContext(t, engine, h, ctx, "INSERT INTO t VALUES (1)")
			if test.query == "" {
				require.NoError(t, sess.CommitWorkingSet(ctx, "mydb", ctx.GetTransaction()))
			} else {
				enginetest.RunQueryWithContext(t, engine, h, ctx, "INSERT INTO `mydb/other`.t VALUES (2)")
				enginetest.RunQueryWithContext(t, engine, h, ctx, test.query)
			}
			require.Equal(t, 1, lifecycle.commits, "a successful batch must emit exactly one semantic commit event")
			require.Nil(t, ctx.GetTransaction())
		})
	}
}
