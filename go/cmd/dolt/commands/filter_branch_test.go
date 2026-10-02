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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
)

func TestFilterQueryDoesNotPublishWorkingSet(t *testing.T) {
	for _, query := range []string{
		"create table t (pk int primary key); insert into t values (1); alter table t add column x int; update t set x=2; create table u (pk int primary key); insert into u values (3)",
		"create table t (pk int primary key); insert into t values (1); create table u (pk int primary key); insert into u values (3); select * from missing",
		"create table t (pk int primary key);\nDELIMITER //\nCREATE PROCEDURE p() BEGIN INSERT INTO t VALUES (1); CREATE TABLE u (pk int primary key); INSERT INTO u VALUES (3); END//\nDELIMITER ;\nCALL p(); DROP PROCEDURE p;",
	} {
		t.Run(query, func(t *testing.T) {
			ctx := t.Context()
			dEnv := dtestutils.CreateTestEnv()
			t.Cleanup(func() { dEnv.Close() })
			original, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			before, err := dEnv.DoltDB(ctx).NomsRoot(ctx)
			require.NoError(t, err)
			result, err := processFilterQuery(ctx, dEnv, original, "test", query, false, true)
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
			after, err := dEnv.DoltDB(ctx).NomsRoot(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after, "DDL and DML must only transform the supplied root")
		})
	}
}

func TestFilterQueryRejectsTransactionControl(t *testing.T) {
	for _, statement := range []string{
		"start transaction", "commit", "rollback", "savepoint s", "rollback to s", "release savepoint s",
		"\nDELIMITER //\nCREATE PROCEDURE p() BEGIN COMMIT; END//\nDELIMITER ;\nCALL p();",
	} {
		t.Run(statement, func(t *testing.T) {
			ctx := t.Context()
			dEnv := dtestutils.CreateTestEnv()
			t.Cleanup(func() { dEnv.Close() })
			original, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			before, err := dEnv.DoltDB(ctx).NomsRoot(ctx)
			require.NoError(t, err)
			_, err = processFilterQuery(ctx, dEnv, original, "test", fmt.Sprintf("create table t (pk int primary key); %s", statement), false, false)
			require.ErrorContains(t, err, "transaction control statements are not supported by filter-branch")
			after, err := dEnv.DoltDB(ctx).NomsRoot(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
