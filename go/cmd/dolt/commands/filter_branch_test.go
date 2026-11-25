// Copyright 2020 Dolthub, Inc.
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

package commands

import (
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestFilterQueryDoesNotPublishWorkingSet(t *testing.T) {
	for _, query := range []string{
		"create table t (pk int primary key); insert into t values (1); alter table t add column x int; update t set x=2; create table u (pk int primary key); insert into u values (3)",
		"create table t (pk int primary key); insert into t values (1); create table u (pk int primary key); insert into u values (3); select * from missing",
		"create table t (pk int primary key);\nDELIMITER //\nCREATE PROCEDURE p() BEGIN INSERT INTO t VALUES (1); CREATE TABLE u (pk int primary key); INSERT INTO u VALUES (3); END//\nDELIMITER ;\nCALL p(); DROP PROCEDURE p;",
		"create table t (pk int primary key); start transaction; insert into t values (1); create table u (pk int primary key); insert into u values (3); commit",
		"create table t (pk int primary key); create table u (pk int primary key); start transaction; insert into t values (2); rollback; insert into t values (1); insert into u values (3)",
		"create table t (pk int primary key); create table u (pk int primary key); insert into t values (1); savepoint s; insert into t values (2); rollback to s; release savepoint s; insert into u values (3)",
	} {
		t.Run(query, func(t *testing.T) {
			ctx := t.Context()
			dEnv := dtestutils.CreateTestEnv()
			t.Cleanup(func() { dEnv.Close() })
			head, err := dEnv.HeadCommit(ctx)
			require.NoError(t, err)
			original, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			before := workingSetHash(t, dEnv)

			result, err := processFilterQuery(ctx, dEnv, head, original, "test", query, false, true)
			require.NoError(t, err)
			for _, name := range []string{"t", "u"} {
				table, ok, err := result.GetTable(ctx, doltdb.TableName{Name: name})
				require.NoError(t, err)
				require.True(t, ok)
				rows, err := table.GetRowData(ctx)
				require.NoError(t, err)
				count, err := rows.Count()
				require.NoError(t, err)
				require.Equal(t, uint64(1), count)
			}

			require.Equal(t, before, workingSetHash(t, dEnv), "DDL and DML must only transform the supplied root")
			requireNoScratchBranch(t, dEnv)
		})
	}
}

// TestFilterQueryDeletesScratchBranchOnError checks that a failed query leaves neither the scratch branch nor
// changes to the current branch behind.
func TestFilterQueryDeletesScratchBranchOnError(t *testing.T) {
	ctx := t.Context()
	dEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() { dEnv.Close() })
	head, err := dEnv.HeadCommit(ctx)
	require.NoError(t, err)
	original, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	before := workingSetHash(t, dEnv)

	_, err = processFilterQuery(ctx, dEnv, head, original, "test", "create table t (pk int primary key); select * from missing", false, false)
	require.Error(t, err)
	require.Equal(t, before, workingSetHash(t, dEnv))
	requireNoScratchBranch(t, dEnv)
}

// workingSetHash returns the hash of the current branch's working set.
func workingSetHash(t *testing.T, dEnv *env.DoltEnv) hash.Hash {
	ws, err := dEnv.WorkingSet(t.Context())
	require.NoError(t, err)
	h, err := ws.HashOf()
	require.NoError(t, err)
	return h
}

func requireNoScratchBranch(t *testing.T, dEnv *env.DoltEnv) {
	_, exists, err := dEnv.DoltDB(t.Context()).HasBranch(t.Context(), filterScratchBranch)
	require.NoError(t, err)
	require.False(t, exists)
}

type closeCountingDatabase struct {
	dsess.SqlDatabase
	closes *int
}

func (db closeCountingDatabase) Close() { *db.closes++ }

// TestFilterEngineCloseClosesDatabase checks that the engine closes the database it owns, which stops its sequence
// tracker's background store reads.
func TestFilterEngineCloseClosesDatabase(t *testing.T) {
	closes := 0
	eng := &filterEngine{
		Engine: sqle.NewDefault(memory.NewDBProvider()),
		db:     closeCountingDatabase{closes: &closes},
	}
	require.NoError(t, eng.Close())
	require.Equal(t, 1, closes)
}
