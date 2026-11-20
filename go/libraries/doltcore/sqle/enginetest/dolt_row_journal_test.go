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
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/rowjournal"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// rowJournalClient is a session in a row journal test.
type rowJournalClient struct {
	t       *testing.T
	harness *DoltHarness
	engine  enginetest.QueryEngine
	ctx     *sql.Context
}

func newRowJournalClient(t *testing.T, harness *DoltHarness, engine enginetest.QueryEngine) *rowJournalClient {
	return &rowJournalClient{
		t:       t,
		harness: harness,
		engine:  engine,
		ctx:     harness.NewContextWithClient(sql.Client{Address: "localhost", User: "root"}),
	}
}

func (c *rowJournalClient) run(query string) {
	c.t.Helper()
	enginetest.RunQueryWithContext(c.t, c.engine, c.harness, c.ctx, query)
}

// txnID returns the ID of the client's current transaction, or zero if it has none.
func (c *rowJournalClient) txnID() rowjournal.TxnID {
	tx, ok := c.ctx.GetTransaction().(*dsess.DoltTransaction)
	if !ok {
		return 0
	}

	return tx.ID()
}

func activeTxnIDs(journal *rowjournal.Journal) []rowjournal.TxnID {
	var ids []rowjournal.TxnID
	for _, txn := range journal.ActiveTxns() {
		ids = append(ids, txn.ID)
	}

	return ids
}

func TestRowJournalTransactionLifecycle(t *testing.T) {
	harness := newDoltHarness(t)
	defer harness.Close()
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()

	journal := harness.provider.RowJournal()
	require.NotNil(t, journal)

	a := newRowJournalClient(t, harness, engine)
	b := newRowJournalClient(t, harness, engine)
	a.run("create table t (pk int primary key, c int)")

	t.Run("autocommit statements leave no transaction registered", func(t *testing.T) {
		a.run("insert into t values (1, 1)")
		a.run("select * from t")
		assert.Empty(t, activeTxnIDs(journal))
	})

	t.Run("transaction IDs ascend across sessions", func(t *testing.T) {
		a.run("begin")
		first := a.txnID()
		require.NotZero(t, first)

		b.run("begin")
		second := b.txnID()
		require.Greater(t, second, first)
		assert.Equal(t, []rowjournal.TxnID{first, second}, activeTxnIDs(journal))

		// The transaction keeps its ID for as long as it runs.
		a.run("insert into t values (2, 2)")
		assert.Equal(t, first, a.txnID())

		a.run("commit")
		assert.Equal(t, []rowjournal.TxnID{second}, activeTxnIDs(journal))
		b.run("rollback")
		assert.Empty(t, activeTxnIDs(journal))

		a.run("begin")
		assert.Greater(t, a.txnID(), second)
		a.run("rollback")
	})

	t.Run("committing with changes ends the registration", func(t *testing.T) {
		a.run("begin")
		a.run("update t set c = c + 1")
		require.Len(t, journal.ActiveTxns(), 1)
		a.run("commit")
		assert.Empty(t, activeTxnIDs(journal))
	})

	t.Run("dolt_commit ends the registration", func(t *testing.T) {
		a.run("begin")
		id := a.txnID()
		a.run("update t set c = c + 1")
		a.run("call dolt_commit('-Am', 'commit')")
		assert.Empty(t, activeTxnIDs(journal))

		// The statement after it runs in a new transaction.
		a.run("begin")
		assert.Greater(t, a.txnID(), id)
		a.run("rollback")
		assert.Empty(t, activeTxnIDs(journal))
	})

	t.Run("savepoints are levels of the registered transaction", func(t *testing.T) {
		level := func() uint32 {
			active := journal.ActiveTxns()
			require.Len(t, active, 1)
			return active[0].Level
		}

		a.run("begin")
		assert.Equal(t, uint32(0), level())
		a.run("savepoint s1")
		assert.Equal(t, uint32(1), level())
		a.run("savepoint s2")
		assert.Equal(t, uint32(2), level())
		a.run("rollback to savepoint s1")
		assert.Equal(t, uint32(1), level())
		a.run("savepoint s2")
		assert.Equal(t, uint32(2), level())
		a.run("release savepoint s2")
		assert.Equal(t, uint32(1), level())
		a.run("release savepoint s1")
		assert.Equal(t, uint32(0), level())

		// Without nested savepoints, releasing one that has a later savepoint leaves the later one usable.
		a.run("savepoint s3")
		a.run("savepoint s4")
		a.run("release savepoint s3")
		assert.Equal(t, uint32(2), level())
		a.run("rollback to savepoint s4")
		assert.Equal(t, uint32(2), level())
		a.run("rollback")
		assert.Empty(t, activeTxnIDs(journal))
	})

	t.Run("a session that ends abandons its transaction", func(t *testing.T) {
		c := newRowJournalClient(t, harness, engine)
		c.run("begin")
		require.Len(t, journal.ActiveTxns(), 1)
		dsess.DSessFromSess(c.ctx.Session).SessionEnd()
		assert.Empty(t, activeTxnIDs(journal))
	})

	t.Run("a transaction replaced without being ended is abandoned", func(t *testing.T) {
		a.run("begin")
		abandoned := a.txnID()
		a.run("begin")
		require.Greater(t, a.txnID(), abandoned)
		assert.Equal(t, []rowjournal.TxnID{a.txnID()}, activeTxnIDs(journal))
		a.run("rollback")
		assert.Empty(t, activeTxnIDs(journal))
	})
}

// journalRecords collects the records appended to a row journal.
type journalRecords struct {
	records []rowjournal.Record
}

func (r *journalRecords) Append(rec rowjournal.Record) {
	r.records = append(r.records, rec)
}

// TestRowJournalVersionControlRecords tests that DDL statements and version control operations are recorded in the
// row journal.
func TestRowJournalVersionControlRecords(t *testing.T) {
	harness := newDoltHarness(t)
	defer harness.Close()
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()

	journal := harness.provider.RowJournal()
	require.NotNil(t, journal)
	sink := &journalRecords{}
	journal.SetRecordSink(sink)
	defer journal.SetRecordSink(nil)

	// described returns what the statement given appended to the journal about refs and schemas.
	client := newRowJournalClient(t, harness, engine)
	described := func(query string) []string {
		sink.records = nil
		client.run(query)

		var descriptions []string
		for _, rec := range sink.records {
			switch rec := rec.(type) {
			case rowjournal.SchemaChangeRecord:
				kind := map[rowjournal.SchemaChangeKind]string{
					rowjournal.SchemaCreateTable: "create",
					rowjournal.SchemaAlterTable:  "alter",
					rowjournal.SchemaDropTable:   "drop",
				}[rec.Kind]
				require.NotZero(t, rec.Txn)
				descriptions = append(descriptions, kind+" "+rec.Relation.Branch+" "+rec.Relation.Table.Name)
			case rowjournal.RefUpdateRecord:
				kind := map[rowjournal.RefUpdateKind]string{
					rowjournal.RefCreate:  "create",
					rowjournal.RefDelete:  "delete",
					rowjournal.RefRename:  "rename",
					rowjournal.RefAdvance: "advance",
				}[rec.Kind]
				description := kind + " " + rec.Ref
				if rec.NewRef != "" {
					description += " " + rec.NewRef
				}

				if rec.Kind == rowjournal.RefAdvance {
					require.False(t, rec.NewHash.IsEmpty())
				}

				descriptions = append(descriptions, description)
			}
		}

		return descriptions
	}

	assert.Equal(t, []string{"create heads/main vc"}, described("create table vc (pk int primary key, c int)"))
	assert.Empty(t, described("insert into vc values (1, 1)"))
	assert.Equal(t, []string{"alter heads/main vc"}, described("alter table vc add column d int"))
	assert.Equal(t, []string{"advance refs/heads/main"}, described("call dolt_commit('-Am', 'vc')"))
	assert.Equal(t, []string{"create refs/heads/b1"}, described("call dolt_branch('b1')"))
	assert.Equal(t, []string{"rename refs/heads/b1 refs/heads/b2"}, described("call dolt_branch('-m', 'b1', 'b2')"))
	assert.Equal(t, []string{"delete refs/heads/b2"}, described("call dolt_branch('-D', 'b2')"))
	assert.Equal(t, []string{"drop heads/main vc"}, described("drop table vc"))
}
